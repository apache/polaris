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
package org.apache.polaris.core.storage.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Storage subscoped credential cache. */
public class StorageCredentialCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageCredentialCache.class);

  private final Cache<StorageCredentialCacheKey, StorageCredentialCacheEntry> cache;

  /** Initialize the creds cache */
  public StorageCredentialCache(StorageCredentialCacheConfig cacheConfig) {
    cache =
        Caffeine.newBuilder()
            .maximumSize(cacheConfig.maxEntries())
            .expireAfter(
                Expiry.creating(
                    (StorageCredentialCacheKey key, StorageCredentialCacheEntry entry) -> {
                      long expireAfterMillis =
                          Math.max(
                              0,
                              Math.min(
                                  (entry.getExpirationTime() - System.currentTimeMillis()) / 2,
                                  entry.getMaxCacheDurationMs()));
                      return Duration.ofMillis(expireAfterMillis);
                    }))
            .build();
  }

  /** How long credentials should remain in the cache. */
  private long maxCacheDurationMs(RealmConfig realmConfig) {
    var cacheDurationSeconds =
        realmConfig.getConfig(FeatureConfiguration.STORAGE_CREDENTIAL_CACHE_DURATION_SECONDS);
    var credentialDurationSeconds =
        realmConfig.getConfig(FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS);
    if (cacheDurationSeconds >= credentialDurationSeconds) {
      throw new IllegalArgumentException(
          String.format(
              "%s should be less than %s",
              FeatureConfiguration.STORAGE_CREDENTIAL_CACHE_DURATION_SECONDS.key(),
              FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS.key()));
    } else {
      return cacheDurationSeconds * 1000L;
    }
  }

  /**
   * Get cached credentials or load new ones using the provided supplier.
   *
   * @param key the cache key
   * @param realmConfig realm configuration for cache duration settings
   * @param loader supplier that produces scoped credentials on cache miss; may throw on error
   * @return the storage access config with scoped credentials
   */
  public StorageAccessConfig getOrLoad(
      StorageCredentialCacheKey key,
      RealmConfig realmConfig,
      Supplier<StorageAccessConfig> loader) {
    long maxCacheDurationMs = maxCacheDurationMs(realmConfig);
    return cache
        .get(
            key,
            k -> {
              LOGGER.atDebug().log("StorageCredentialCache::load");
              StorageAccessConfig accessConfig = loader.get();
              return new StorageCredentialCacheEntry(accessConfig, maxCacheDurationMs);
            })
        .toAccessConfig();
  }

  @VisibleForTesting
  @Nullable
  Map<String, String> getIfPresent(StorageCredentialCacheKey key) {
    return getAccessConfig(key).map(StorageAccessConfig::credentials).orElse(null);
  }

  @VisibleForTesting
  Optional<StorageAccessConfig> getAccessConfig(StorageCredentialCacheKey key) {
    return Optional.ofNullable(cache.getIfPresent(key))
        .map(StorageCredentialCacheEntry::toAccessConfig);
  }

  @VisibleForTesting
  public long getEstimatedSize() {
    return this.cache.estimatedSize();
  }

  @VisibleForTesting
  public void invalidateAll() {
    this.cache.invalidateAll();
  }
}
