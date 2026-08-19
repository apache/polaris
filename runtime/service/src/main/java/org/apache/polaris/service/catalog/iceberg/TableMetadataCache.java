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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.zip.GZIPInputStream;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

/**
 * Process-wide cache of table metadata JSON documents keyed by realm, catalog, catalog version and
 * metadata file location. Metadata files are immutable once written, so an entry never becomes
 * stale and the location acts as a content address. Callers resolve and validate storage access for
 * the location before consulting the cache, so a hit only skips the object-storage read, never any
 * authorization. The raw JSON is cached rather than parsed {@link org.apache.iceberg.TableMetadata}
 * objects because {@code TableMetadata} lazily materializes some fields and is not safe to share
 * across request threads, and raw documents allow bounding the cache by size. Entry weights
 * estimate heap usage (UTF-16 characters plus per-entry map and object overhead); the configured
 * budget is an approximate bound because eviction may briefly lag writes.
 */
@ApplicationScoped
public class TableMetadataCache {

  /**
   * Scopes a metadata document to the realm and catalog that is allowed to read it, and to the
   * catalog entity version that produced it. The version changes whenever the catalog is updated,
   * so a storage-binding change (new endpoint, region or role) yields a fresh key and never serves
   * a document loaded through the previous binding.
   */
  public record Key(String realmId, long catalogId, long catalogVersion, String metadataLocation) {}

  private static final Duration EXPIRE_AFTER_ACCESS = Duration.ofHours(1);

  /** Estimated heap cost of a cache entry beyond its strings: map node and key record. */
  private static final int ENTRY_OVERHEAD_BYTES = 64;

  /** Estimated heap size of an empty {@link String}: object headers, fields and byte[] header. */
  private static final int STRING_INSTANCE_SIZE = 48;

  private final boolean enabled;
  private final Cache<Key, String> metadataJsonByLocation;

  @Inject
  public TableMetadataCache(TableMetadataCacheConfiguration configuration) {
    this.enabled = configuration.maxBytes() > 0;
    this.metadataJsonByLocation =
        Caffeine.newBuilder()
            .maximumWeight(configuration.maxBytes())
            .weigher(TableMetadataCache::estimatedEntryHeapBytes)
            // Run maintenance on the writing threads so eviction keeps up with inserts instead of
            // waiting for an executor, keeping the overshoot past the budget small.
            .executor(Runnable::run)
            .expireAfterAccess(EXPIRE_AFTER_ACCESS)
            .build();
  }

  private static int estimatedEntryHeapBytes(Key key, String metadataJson) {
    long bytes =
        ENTRY_OVERHEAD_BYTES
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
   * Returns the metadata JSON document at the key's immutable location, reading it through the
   * given {@link FileIO} on a cache miss. The read happens outside the cache's compute so a slow
   * object-storage read never blocks access to other keys; concurrent misses for the same key may
   * read the same immutable document more than once.
   */
  public String getOrLoad(Key key, FileIO fileIO) {
    if (!enabled) {
      return read(fileIO, key.metadataLocation());
    }
    String cached = metadataJsonByLocation.getIfPresent(key);
    if (cached != null) {
      return cached;
    }
    String metadataJson = read(fileIO, key.metadataLocation());
    metadataJsonByLocation.put(key, metadataJson);
    return metadataJson;
  }

  public void put(Key key, String metadataJson) {
    if (enabled) {
      metadataJsonByLocation.put(key, metadataJson);
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
