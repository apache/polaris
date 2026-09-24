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
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.polaris.core.storage.ImmutableStorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.jspecify.annotations.Nullable;

/**
 * Process-wide cache of table metadata JSON documents keyed by realm, catalog, the non-credential
 * {@link FileIO} inputs and metadata file location. Metadata files are immutable once written, so
 * an entry never becomes stale and the location acts as a content address. The {@link FileIO} is
 * built only on a miss, since a hit reads nothing from object storage. The raw JSON is cached
 * rather than parsed {@link org.apache.iceberg.TableMetadata} objects because {@code TableMetadata}
 * lazily materializes some fields and is not safe to share across request threads, and raw
 * documents allow bounding the cache by size. Entry weights estimate heap usage (UTF-16 characters
 * plus per-entry map and object overhead); the configured budget is an approximate bound because
 * eviction may briefly lag writes.
 */
@ApplicationScoped
public class TableMetadataCache {

  /**
   * Scopes a metadata document to the realm and catalog that is allowed to read it, and to the
   * inputs of the {@link FileIO} that reads it. Credentials and their expiry change on every vend,
   * so the key keeps only the remaining storage access properties.
   */
  public record Key(
      String realmId,
      long catalogId,
      @Nullable String ioImplClassName,
      Map<String, String> tableProperties,
      StorageAccessConfig storageAccessConfig,
      String metadataLocation) {
    public Key {
      storageAccessConfig =
          ImmutableStorageAccessConfig.copyOf(storageAccessConfig)
              .withCredentials(Map.of())
              .withExpiresAt(Optional.empty());
    }
  }

  private static final Duration EXPIRE_AFTER_ACCESS = Duration.ofHours(1);

  /** Percentage of the maximum heap size the cache uses when no budget is configured. */
  private static final long DEFAULT_MAX_HEAP_PERCENTAGE = 5;

  /** Estimated heap cost of a cache entry beyond its strings: map node and key record. */
  private static final int ENTRY_OVERHEAD_BYTES = 64;

  /** Estimated heap size of an empty {@link String}: object headers, fields and byte[] header. */
  private static final int STRING_INSTANCE_SIZE = 48;

  private final boolean enabled;
  private final long maxContentLength;
  private final Cache<Key, String> metadataJsonByLocation;

  @Inject
  public TableMetadataCache(TableMetadataCacheConfiguration configuration) {
    long maxBytes =
        configuration.maxBytes().orElseGet(() -> defaultMaxBytes(Runtime.getRuntime().maxMemory()));
    this.enabled = maxBytes > 0;
    this.maxContentLength = configuration.maxContentLength();
    this.metadataJsonByLocation =
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
  static long defaultMaxBytes(long maxHeapBytes) {
    return maxHeapBytes / 100 * DEFAULT_MAX_HEAP_PERCENTAGE;
  }

  private static int estimatedEntryHeapBytes(Key key, String metadataJson) {
    long bytes =
        ENTRY_OVERHEAD_BYTES
            + estimatedSizeOf(key.realmId())
            + estimatedSizeOf(key.tableProperties())
            + estimatedSizeOf(key.storageAccessConfig().extraProperties())
            + estimatedSizeOf(key.storageAccessConfig().internalProperties())
            + estimatedSizeOf(key.metadataLocation())
            + estimatedSizeOf(metadataJson);
    if (key.ioImplClassName() != null) {
      bytes += estimatedSizeOf(key.ioImplClassName());
    }
    return (int) Math.min(bytes, Integer.MAX_VALUE);
  }

  /** Estimated heap size of a string: instance overhead plus two bytes per UTF-16 code unit. */
  private static long estimatedSizeOf(String value) {
    return STRING_INSTANCE_SIZE + (long) value.length() * Character.BYTES;
  }

  /** Estimated heap size of a map's keys and values. */
  private static long estimatedSizeOf(Map<String, String> properties) {
    long bytes = 0;
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      bytes += estimatedSizeOf(entry.getKey()) + estimatedSizeOf(entry.getValue());
    }
    return bytes;
  }

  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Returns the table metadata at the key's immutable location, reading it through a {@link FileIO}
   * from the given supplier on a cache miss. A document that cannot be read or parsed surfaces as
   * {@link RuntimeIOException}.
   */
  public TableMetadata getOrLoadMetadata(Key key, Supplier<FileIO> fileIOSupplier) {
    String metadataJson = getOrLoad(key, fileIOSupplier);
    try {
      return TableMetadataParser.fromJson(key.metadataLocation(), metadataJson);
    } catch (UncheckedIOException e) {
      throw new RuntimeIOException(
          e.getCause(), "Failed to parse metadata file %s", key.metadataLocation());
    }
  }

  /**
   * Returns the metadata JSON document at the key's immutable location, reading it through a {@link
   * FileIO} from the given supplier on a cache miss. The read happens outside the cache's compute
   * so a slow object-storage read never blocks access to other keys; concurrent misses for the same
   * key may read the same immutable document more than once.
   */
  @VisibleForTesting
  String getOrLoad(Key key, Supplier<FileIO> fileIOSupplier) {
    if (!enabled) {
      return read(fileIOSupplier.get(), key.metadataLocation());
    }
    String cached = metadataJsonByLocation.getIfPresent(key);
    if (cached != null) {
      return cached;
    }
    String metadataJson = read(fileIOSupplier.get(), key.metadataLocation());
    admit(key, metadataJson);
    return metadataJson;
  }

  public void put(Key key, String metadataJson) {
    if (enabled) {
      admit(key, metadataJson);
    }
  }

  /**
   * Caches the document unless it exceeds the content length cap. Size is measured in characters,
   * which equals the byte size of ASCII JSON.
   */
  private void admit(Key key, String metadataJson) {
    if (metadataJson.length() <= maxContentLength) {
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
