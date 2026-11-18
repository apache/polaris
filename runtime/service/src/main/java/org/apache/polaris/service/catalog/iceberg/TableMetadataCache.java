/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.catalog.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.util.JsonUtil;

/**
 * Process-wide cache of table metadata JSON documents keyed by realm, table entity id and metadata
 * file location. Each commit writes its metadata to a new file, so a cached document matches the
 * file at its location. A metadata file rewritten in place outside Polaris stays stale on every
 * node that cached it until it is evicted or goes unused for an hour, which a frequently read
 * document may never do. Expiry after an hour without access also frees the documents of superseded
 * metadata versions. Raw JSON is cached rather than parsed {@link TableMetadata} because objects
 * reachable from it are not safe to share across threads (apache/iceberg#17585) and the document
 * length bounds the cache by size.
 */
@ApplicationScoped
public class TableMetadataCache {

  /** Scopes a metadata document to its realm, its table entity and its location. */
  private record Key(String realmId, long tableEntityId, String metadataLocation) {}

  private static final Duration EXPIRE_AFTER_ACCESS = Duration.ofHours(1);

  /** Largest share of the total budget a single document may use. */
  private static final double MAX_ENTRY_FRACTION_OF_BUDGET = 0.1;

  /**
   * Estimated heap cost of a cache entry beyond its strings and array: map nodes and key record.
   */
  private static final int ENTRY_OVERHEAD_BYTES = 128;

  /** Estimated heap size of an empty {@link String}: object headers, fields and byte[] header. */
  private static final int STRING_INSTANCE_SIZE = 48;

  /** Estimated heap size of a byte[] header. */
  private static final int ARRAY_HEADER_BYTES = 16;

  private final boolean enabled;
  private final long maxBytesPerEntry;
  private final Cache<Key, byte[]> metadataJsonCache;

  @Inject
  public TableMetadataCache(TableMetadataCacheConfiguration configuration) {
    this(configuration, Ticker.systemTicker());
  }

  @VisibleForTesting
  TableMetadataCache(TableMetadataCacheConfiguration configuration, Ticker ticker) {
    long maxBytes = maxBytes(configuration, Runtime.getRuntime().maxMemory());
    this.enabled = maxBytes > 0;
    this.maxBytesPerEntry = maxBytesPerEntry(configuration, maxBytes);
    this.metadataJsonCache =
        Caffeine.newBuilder()
            .maximumWeight(maxBytes)
            .weigher(TableMetadataCache::estimatedEntryHeapBytes)
            // Runs maintenance on the calling thread, trading request latency for evicting
            // documents over the budget without waiting for an executor.
            .executor(Runnable::run)
            .expireAfterAccess(EXPIRE_AFTER_ACCESS)
            .ticker(ticker)
            .build();
  }

  @VisibleForTesting
  static long maxBytes(TableMetadataCacheConfiguration configuration, long maxHeapBytes) {
    return configuration
        .maxBytes()
        .orElseGet(() -> (long) (configuration.fractionOfMaxHeapSize() * maxHeapBytes));
  }

  @VisibleForTesting
  static long maxBytesPerEntry(TableMetadataCacheConfiguration configuration, long maxBytes) {
    return Math.min(
        configuration.maxBytesPerEntry(), (long) (maxBytes * MAX_ENTRY_FRACTION_OF_BUDGET));
  }

  private static int estimatedEntryHeapBytes(Key key, byte[] metadataJson) {
    long bytes =
        ENTRY_OVERHEAD_BYTES
            + estimatedSizeOf(key.realmId())
            + estimatedSizeOf(key.metadataLocation())
            + ARRAY_HEADER_BYTES
            + metadataJson.length;
    return (int) Math.min(bytes, Integer.MAX_VALUE);
  }

  /** Estimated heap size of a string, assuming Latin-1 content stored at one byte per character. */
  private static long estimatedSizeOf(String value) {
    return STRING_INSTANCE_SIZE + (long) value.length();
  }

  @VisibleForTesting
  boolean isEnabled() {
    return enabled;
  }

  /**
   * Returns the table metadata at the metadata location, reading it through a {@link FileIO} from
   * the given supplier on a cache miss. A document that cannot be read or parsed surfaces as {@link
   * RuntimeIOException}.
   */
  public TableMetadata getOrLoadMetadata(
      String realmId,
      long tableEntityId,
      String metadataLocation,
      Supplier<FileIO> fileIOSupplier) {
    byte[] metadataJson = getOrLoad(realmId, tableEntityId, metadataLocation, fileIOSupplier);
    try {
      return TableMetadataParser.fromJson(
          metadataLocation, JsonUtil.mapper().readValue(metadataJson, JsonNode.class));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to parse metadata file %s", metadataLocation);
    }
  }

  /**
   * Returns the metadata JSON document, reading it through a {@link FileIO} from the given supplier
   * on a cache miss. The read runs outside a cache compute, which would lock a hash bin shared with
   * other keys for the duration of the read, so concurrent misses for one key may each read it.
   */
  @VisibleForTesting
  byte[] getOrLoad(
      String realmId,
      long tableEntityId,
      String metadataLocation,
      Supplier<FileIO> fileIOSupplier) {
    if (!enabled) {
      return read(fileIOSupplier.get(), metadataLocation);
    }
    Key key = new Key(realmId, tableEntityId, metadataLocation);
    byte[] cached = metadataJsonCache.getIfPresent(key);
    if (cached != null) {
      return cached;
    }
    byte[] metadataJson = read(fileIOSupplier.get(), metadataLocation);
    admit(key, metadataJson);
    return metadataJson;
  }

  /**
   * Caches the uncompressed metadata JSON document written to the metadata location. The cache
   * keeps the array, so the caller must not modify it afterwards.
   */
  public void putMetadata(
      String realmId, long tableEntityId, String metadataLocation, byte[] metadataJson) {
    if (enabled) {
      admit(new Key(realmId, tableEntityId, metadataLocation), metadataJson);
    }
  }

  /** Drops the document cached for the metadata location. */
  public void invalidateMetadata(String realmId, long tableEntityId, String metadataLocation) {
    if (enabled) {
      metadataJsonCache.invalidate(new Key(realmId, tableEntityId, metadataLocation));
    }
  }

  @VisibleForTesting
  void invalidateAll() {
    metadataJsonCache.invalidateAll();
  }

  /** Caches the document unless its estimated entry size exceeds the per-entry cap. */
  private void admit(Key key, byte[] metadataJson) {
    if (estimatedEntryHeapBytes(key, metadataJson) <= maxBytesPerEntry) {
      metadataJsonCache.put(key, metadataJson);
    }
  }

  private static byte[] read(FileIO fileIO, String metadataLocation) {
    InputFile inputFile = fileIO.newInputFile(metadataLocation);
    TableMetadataParser.Codec codec = TableMetadataParser.Codec.fromFileName(metadataLocation);
    try (InputStream stream =
        codec == TableMetadataParser.Codec.GZIP
            ? new GZIPInputStream(inputFile.newStream())
            : inputFile.newStream()) {
      return stream.readAllBytes();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read metadata file %s", metadataLocation);
    }
  }
}
