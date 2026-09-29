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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.polaris.core.entity.PolarisEntityCore;

/**
 * Process-wide cache of table metadata JSON documents keyed by realm, the id and version of every
 * entity on the table's resolved path (catalog, namespaces and table), and metadata file location.
 * Every input to reading a metadata file lives on one of those entities, so any change bumps an
 * entity version and an entry never goes stale. Raw JSON is cached rather than parsed {@link
 * TableMetadata} because objects reachable from it are not safe to share across threads
 * (apache/iceberg#17585) and the document length bounds the cache by size.
 */
@ApplicationScoped
public class TableMetadataCache {

  /** Id and version of one entity on the resolved path. */
  private record EntityVersion(long id, int version) {
    private static EntityVersion of(PolarisEntityCore entity) {
      return new EntityVersion(entity.getId(), entity.getEntityVersion());
    }
  }

  /**
   * Scopes a metadata document to its realm, to the versions of the entities on the table's
   * resolved path, and to its location.
   */
  private record Key(String realmId, List<EntityVersion> resolvedPath, String metadataLocation) {
    private static Key of(
        String realmId, List<? extends PolarisEntityCore> resolvedPath, String metadataLocation) {
      return new Key(
          realmId, resolvedPath.stream().map(EntityVersion::of).toList(), metadataLocation);
    }
  }

  private static final Duration EXPIRE_AFTER_ACCESS = Duration.ofHours(1);

  /** Estimated heap cost of a cache entry beyond its strings and path: map node and key record. */
  private static final int ENTRY_OVERHEAD_BYTES = 64;

  /** Estimated heap cost of one resolved path element: list slot plus id and version record. */
  private static final int PATH_ELEMENT_BYTES = 32;

  /** Estimated heap size of an empty {@link String}: object headers, fields and byte[] header. */
  private static final int STRING_INSTANCE_SIZE = 48;

  private final boolean enabled;
  private final long maxBytesPerEntry;
  private final Cache<Key, String> metadataJsonCache;

  @Inject
  public TableMetadataCache(TableMetadataCacheConfiguration configuration) {
    long maxBytes = maxBytes(configuration, Runtime.getRuntime().maxMemory());
    this.enabled = maxBytes > 0;
    this.maxBytesPerEntry = configuration.maxBytesPerEntry();
    this.metadataJsonCache =
        Caffeine.newBuilder()
            .maximumWeight(maxBytes)
            .weigher(TableMetadataCache::estimatedEntryHeapBytes)
            // Run maintenance on the writing threads so eviction keeps up with inserts instead of
            // waiting for an executor, keeping the overshoot past the budget small.
            .executor(Runnable::run)
            .expireAfterAccess(EXPIRE_AFTER_ACCESS)
            .build();
  }

  @VisibleForTesting
  static long maxBytes(TableMetadataCacheConfiguration configuration, long maxHeapBytes) {
    return configuration
        .maxBytes()
        .orElseGet(() -> (long) (configuration.fractionOfMaxHeapSize() * maxHeapBytes));
  }

  private static int estimatedEntryHeapBytes(Key key, String metadataJson) {
    long bytes =
        ENTRY_OVERHEAD_BYTES
            + (long) key.resolvedPath().size() * PATH_ELEMENT_BYTES
            + estimatedSizeOf(key.realmId())
            + estimatedSizeOf(key.metadataLocation())
            + estimatedSizeOf(metadataJson);
    return (int) Math.min(bytes, Integer.MAX_VALUE);
  }

  /** Estimated heap size of a string: instance overhead plus two bytes per UTF-16 code unit. */
  private static long estimatedSizeOf(String value) {
    return STRING_INSTANCE_SIZE + (long) value.length() * Character.BYTES;
  }

  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Returns the table metadata at the metadata location, reading it through a {@link FileIO} from
   * the given supplier on a cache miss. A document that cannot be read or parsed surfaces as {@link
   * RuntimeIOException}.
   */
  public TableMetadata getOrLoadMetadata(
      String realmId,
      List<? extends PolarisEntityCore> resolvedPath,
      String metadataLocation,
      Supplier<FileIO> fileIOSupplier) {
    String metadataJson = getOrLoad(realmId, resolvedPath, metadataLocation, fileIOSupplier);
    try {
      return TableMetadataParser.fromJson(metadataLocation, metadataJson);
    } catch (UncheckedIOException e) {
      throw new RuntimeIOException(
          e.getCause(), "Failed to parse metadata file %s", metadataLocation);
    }
  }

  /**
   * Returns the metadata JSON document, reading it through a {@link FileIO} from the given supplier
   * on a cache miss. The read happens outside the cache's compute so a slow object-storage read
   * never blocks other keys; concurrent misses for the same key may read the document more than
   * once.
   */
  @VisibleForTesting
  String getOrLoad(
      String realmId,
      List<? extends PolarisEntityCore> resolvedPath,
      String metadataLocation,
      Supplier<FileIO> fileIOSupplier) {
    if (!enabled) {
      return read(fileIOSupplier.get(), metadataLocation);
    }
    Key key = Key.of(realmId, resolvedPath, metadataLocation);
    String cached = metadataJsonCache.getIfPresent(key);
    if (cached != null) {
      return cached;
    }
    String metadataJson = read(fileIOSupplier.get(), metadataLocation);
    admit(key, metadataJson);
    return metadataJson;
  }

  /** Caches the metadata under its own metadata file location. */
  public void put(
      String realmId, List<? extends PolarisEntityCore> resolvedPath, TableMetadata metadata) {
    if (enabled) {
      admit(
          Key.of(realmId, resolvedPath, metadata.metadataFileLocation()),
          TableMetadataParser.toJson(metadata));
    }
  }

  /** Caches the document unless its estimated entry size exceeds the per-entry cap. */
  private void admit(Key key, String metadataJson) {
    if (estimatedEntryHeapBytes(key, metadataJson) <= maxBytesPerEntry) {
      metadataJsonCache.put(key, metadataJson);
    }
  }

  private static String read(FileIO fileIO, String metadataLocation) {
    InputFile inputFile = fileIO.newInputFile(metadataLocation);
    TableMetadataParser.Codec codec = TableMetadataParser.Codec.fromFileName(metadataLocation);
    try (InputStream stream =
        codec == TableMetadataParser.Codec.GZIP
            ? new GZIPInputStream(inputFile.newStream())
            : inputFile.newStream()) {
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read metadata file %s", metadataLocation);
    }
  }
}
