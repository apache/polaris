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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storage subscoped credential cache. The cache loader is key-driven: on miss, {@link
 * StorageCredentialCacheKey#load()} is invoked to mint a fresh {@link StorageAccessConfig} from the
 * key's own data fields and the auxiliary deps it carries. This guarantees the cached value is a
 * function of the key alone, so two equal keys are logically equivalent.
 */
public class StorageCredentialCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageCredentialCache.class);

  private final LoadingCache<StorageCredentialCacheKey, StorageCredentialCacheEntry> cache;

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
                                  entry.maxCacheDurationMs()));
                      return Duration.ofMillis(expireAfterMillis);
                    }))
            .build(
                key -> {
                  LOGGER.atDebug().log("StorageCredentialCache::load");
                  StorageAccessConfig accessConfig = key.load();
                  return new StorageCredentialCacheEntry(
                      accessConfig, maxCacheDurationMs(key.realmConfig()));
                });
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
   * Return the cached {@link StorageAccessConfig} for {@code key}, loading it via {@link
   * StorageCredentialCacheKey#load()} on miss.
   */
  public StorageAccessConfig getOrLoad(StorageCredentialCacheKey key) {
    return cache.get(key).toAccessConfig();
  }

  @VisibleForTesting
  @Nullable Map<String, String> getIfPresent(StorageCredentialCacheKey key) {
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
