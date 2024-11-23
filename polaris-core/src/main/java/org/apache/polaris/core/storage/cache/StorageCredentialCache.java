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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.storage.PolarisCredentialVendor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Storage subscoped credential cache. */
public class StorageCredentialCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageCredentialCache.class);

  private static final long CACHE_MAX_DURATION_MS = 30 * 60 * 1000L; // 30 minutes
  private static final long CACHE_MAX_NUMBER_OF_ENTRIES = 10_000L;
  private final LoadingCache<StorageCredentialCacheKey, StorageCredentialCacheEntry> cache;

  /** Initialize the creds cache, max cache duration is half an hr. */
  public StorageCredentialCache() {
    cache =
        Caffeine.newBuilder()
            .maximumSize(CACHE_MAX_NUMBER_OF_ENTRIES)
            .expireAfter(
                new Expiry<StorageCredentialCacheKey, StorageCredentialCacheEntry>() {
                  @Override
                  public long expireAfterCreate(
                      StorageCredentialCacheKey key,
                      StorageCredentialCacheEntry entry,
                      long currentTime) {
                    long expireAfterMillis =
                        Math.max(
                            0,
                            Math.min(
                                (entry.getExpirationTime() - System.currentTimeMillis()) / 2,
                                CACHE_MAX_DURATION_MS));
                    return TimeUnit.MILLISECONDS.toNanos(expireAfterMillis);
                  }

                  @Override
                  public long expireAfterUpdate(
                      StorageCredentialCacheKey key,
                      StorageCredentialCacheEntry entry,
                      long currentTime,
                      long currentDuration) {
                    return currentDuration;
                  }

                  @Override
                  public long expireAfterRead(
                      StorageCredentialCacheKey key,
                      StorageCredentialCacheEntry entry,
                      long currentTime,
                      long currentDuration) {
                    return currentDuration;
                  }
                })
            .build(
                key -> {
                  // the load happen at getOrGenerateSubScopeCreds()
                  return null;
                });
  }

  /**
   * Either get from the cache or generate a new entry for a scoped creds
   *
   * @param credentialVendor the credential vendor used to generate a new scoped creds if needed
   * @param callCtx the call context
   * @param polarisEntity the polaris entity that is going to scoped creds
   * @param allowListOperation whether allow list action on the provided read and write locations
   * @param allowedReadLocations a set of allowed to read locations
   * @param allowedWriteLocations a set of allowed to write locations.
   * @return the a map of string containing the scoped creds information
   */
  public Map<String, String> getOrGenerateSubScopeCreds(
      @NotNull PolarisCredentialVendor credentialVendor,
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisEntity polarisEntity,
      boolean allowListOperation,
      @NotNull Set<String> allowedReadLocations,
      @NotNull Set<String> allowedWriteLocations) {
    if (!isTypeSupported(polarisEntity.getType())) {
      callCtx
          .getDiagServices()
          .fail("entity_type_not_suppported_to_scope_creds", "type={}", polarisEntity.getType());
    }
    StorageCredentialCacheKey key =
        new StorageCredentialCacheKey(
            polarisEntity,
            allowListOperation,
            allowedReadLocations,
            allowedWriteLocations,
            callCtx);
    LOGGER.atDebug().addKeyValue("key", key).log("subscopedCredsCache");
    Function<StorageCredentialCacheKey, StorageCredentialCacheEntry> loader =
        k -> {
          LOGGER.atDebug().log("StorageCredentialCache::load");
          PolarisCredentialVendor.ScopedCredentialsResult scopedCredentialsResult =
              credentialVendor.getSubscopedCredsForEntity(
                  k.getCallContext(),
                  k.getCatalogId(),
                  k.getEntityId(),
                  k.isAllowedListAction(),
                  k.getAllowedReadLocations(),
                  k.getAllowedWriteLocations());
          if (scopedCredentialsResult.isSuccess()) {
            return new StorageCredentialCacheEntry(scopedCredentialsResult);
          }
          LOGGER
              .atDebug()
              .addKeyValue("errorMessage", scopedCredentialsResult.getExtraInformation())
              .log("Failed to get subscoped credentials");
          throw new UnprocessableEntityException(
              "Failed to get subscoped credentials: %s",
              scopedCredentialsResult.getExtraInformation());
        };
    return cache.get(key, loader).convertToMapOfString();
  }

  public Map<String, String> getIfPresent(StorageCredentialCacheKey key) {
    return Optional.ofNullable(cache.getIfPresent(key))
        .map(StorageCredentialCacheEntry::convertToMapOfString)
        .orElse(null);
  }

  private boolean isTypeSupported(PolarisEntityType type) {
    return type == PolarisEntityType.CATALOG
        || type == PolarisEntityType.NAMESPACE
        || type == PolarisEntityType.TABLE_LIKE
        || type == PolarisEntityType.TASK;
  }

  @VisibleForTesting
  public long getEstimatedSize() {
    return this.cache.estimatedSize();
  }
}
