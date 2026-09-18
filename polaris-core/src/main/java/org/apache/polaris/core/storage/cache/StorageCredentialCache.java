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
                      long remainingMs = entry.getExpirationTime() - System.currentTimeMillis();
                      long bufferMs = entry.refreshBufferMs();
                      long effectiveTtl = bufferMs > 0 ? remainingMs - bufferMs : remainingMs / 2;
                      long expireAfterMillis =
                          Math.max(0, Math.min(effectiveTtl, entry.maxCacheDurationMs()));
                      return Duration.ofMillis(expireAfterMillis);
                    }))
            .build(
                key -> {
                  LOGGER.atDebug().log("StorageCredentialCache::load");
                  StorageAccessConfig accessConfig = key.load();
                  return new StorageCredentialCacheEntry(
                      accessConfig,
                      maxCacheDurationMs(key.realmConfig()),
                      refreshBufferMs(key.realmConfig()));
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

  /** Minimum buffer to keep between credential expiry and cache eviction. */
  private long refreshBufferMs(RealmConfig realmConfig) {
    var refreshBufferSeconds =
        realmConfig.getConfig(FeatureConfiguration.STORAGE_CREDENTIAL_REFRESH_BUFFER_SECONDS);
    var credentialDurationSeconds =
        realmConfig.getConfig(FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS);
    if (refreshBufferSeconds < 0 || refreshBufferSeconds >= credentialDurationSeconds) {
      throw new IllegalArgumentException(
          String.format(
              "%s must be >= 0 and less than %s",
              FeatureConfiguration.STORAGE_CREDENTIAL_REFRESH_BUFFER_SECONDS.key(),
              FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS.key()));
    }
    return refreshBufferSeconds * 1000L;
  }

  /**
   * Return the cached {@link StorageAccessConfig} for {@code key}, loading it via {@link
   * StorageCredentialCacheKey#load()} on miss.
   *
   * <p>The refresh buffer (when non-zero) is enforced on every call, not just on cache hits: a
   * freshly loaded credential can itself already be within the buffer of expiring (e.g. a
   * credential provider issuing a short-lived token), in which case Caffeine's {@code expireAfter}
   * would give it a zero TTL but still hand it back on this same call. To honor the "clients always
   * receive credentials with at least this much validity left" contract, such an entry is discarded
   * and reloaded once, bypassing the stale copy. If the reload is still under the buffer, it's
   * returned anyway (with a warning) rather than retried indefinitely -- a provider that can't
   * clear its own configured buffer is a configuration problem, not something to hot-loop against.
   */
  public StorageAccessConfig getOrLoad(StorageCredentialCacheKey key) {
    StorageCredentialCacheEntry entry = cache.get(key);
    if (isUnderRefreshBuffer(entry)) {
      cache.invalidate(key);
      entry = cache.get(key);
      if (isUnderRefreshBuffer(entry)) {
        LOGGER
            .atWarn()
            .log(
                "Reloaded storage credential is still within its configured refresh buffer "
                    + "({} ms remaining, buffer {} ms) -- returning it anyway. The underlying "
                    + "credential provider may be issuing credentials shorter than the "
                    + "configured buffer.",
                entry.getExpirationTime() - System.currentTimeMillis(),
                entry.refreshBufferMs());
      }
    }
    return entry.toAccessConfig();
  }

  private static boolean isUnderRefreshBuffer(StorageCredentialCacheEntry entry) {
    if (entry.refreshBufferMs() <= 0) {
      return false;
    }
    long remainingMs = entry.getExpirationTime() - System.currentTimeMillis();
    return remainingMs < entry.refreshBufferMs();
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
