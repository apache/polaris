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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.dao.entity.ScopedCredentialsResult;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageCredentialsVendor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Storage subscoped credential cache. */
public class StorageCredentialCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageCredentialCache.class);

  private final PolarisDiagnostics diagnostics;
  private final LoadingCache<StorageCredentialCacheKey, StorageCredentialCacheEntry> cache;

  /** Initialize the creds cache */
  public StorageCredentialCache(
      PolarisDiagnostics diagnostics, StorageCredentialCacheConfig cacheConfig) {
    this.diagnostics = diagnostics;
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
                  // the load happen at getOrGenerateSubScopeCreds()
                  return null;
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
   * Either get from the cache or generate a new entry for a scoped creds
   *
   * @param storageCredentialsVendor the credential vendor used to generate a new scoped creds if
   *     needed
   * @param polarisEntity the polaris entity that is going to scoped creds
   * @param allowListOperation whether allow list action on the provided read and write locations
   * @param allowedReadLocations a set of allowed to read locations
   * @param allowedWriteLocations a set of allowed to write locations.
   * @param polarisPrincipal the principal requesting credentials
   * @param refreshCredentialsEndpoint optional endpoint for credential refresh
   * @param credentialVendingContext context containing metadata for session tags (catalog,
   *     namespace, table, roles) for audit/correlation purposes
   * @return the a map of string containing the scoped creds information
   */
  public StorageAccessConfig getOrGenerateSubScopeCreds(
      @Nonnull StorageCredentialsVendor storageCredentialsVendor,
      @Nonnull PolarisEntity polarisEntity,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      @Nonnull PolarisPrincipal polarisPrincipal,
      Optional<String> refreshCredentialsEndpoint,
      @Nonnull CredentialVendingContext credentialVendingContext) {
    RealmContext realmContext = storageCredentialsVendor.getRealmContext();
    RealmConfig realmConfig = storageCredentialsVendor.getRealmConfig();
    if (!isTypeSupported(polarisEntity.getType())) {
      diagnostics.fail(
          "entity_type_not_suppported_to_scope_creds", "type={}", polarisEntity.getType());
    }

    boolean includePrincipalNameInSubscopedCredential =
        realmConfig.getConfig(FeatureConfiguration.INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL);
    boolean includeSessionTags =
        realmConfig.getConfig(FeatureConfiguration.INCLUDE_SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL);

    // When session tags are enabled, the cache key needs to include:
    // 1. The credential vending context to avoid returning cached credentials with different
    //    session tags (catalog/namespace/table/roles/traceId)
    // 2. The principal, because the polaris:principal session tag is included in AWS credentials
    //    and we must not serve credentials tagged for principal A to principal B
    // When session tags are disabled, we only include principal if explicitly configured.
    //
    // Note: The trace ID is controlled at the source (StorageAccessConfigProvider). When
    // INCLUDE_TRACE_ID_IN_SESSION_TAGS is disabled, the context's traceId is left empty,
    // which allows efficient caching across requests with different trace IDs.
    boolean includePrincipalInCacheKey =
        includePrincipalNameInSubscopedCredential || includeSessionTags;
    // When session tags are disabled, use empty context to ensure consistent cache key behavior
    CredentialVendingContext contextForCacheKey =
        includeSessionTags ? credentialVendingContext : CredentialVendingContext.empty();
    StorageCredentialCacheKey key =
        StorageCredentialCacheKey.of(
            realmContext.getRealmIdentifier(),
            polarisEntity,
            allowListOperation,
            allowedReadLocations,
            allowedWriteLocations,
            refreshCredentialsEndpoint,
            includePrincipalInCacheKey ? Optional.of(polarisPrincipal) : Optional.empty(),
            contextForCacheKey);
    Function<StorageCredentialCacheKey, StorageCredentialCacheEntry> loader =
        k -> {
          LOGGER.atDebug().log("StorageCredentialCache::load");
          // Use credentialVendingContext from the cache key for correctness.
          // This ensures we use the same context that was used for cache key comparison.
          ScopedCredentialsResult scopedCredentialsResult =
              storageCredentialsVendor.getSubscopedCredsForEntity(
                  polarisEntity,
                  allowListOperation,
                  allowedReadLocations,
                  allowedWriteLocations,
                  polarisPrincipal,
                  refreshCredentialsEndpoint,
                  k.credentialVendingContext());
          if (scopedCredentialsResult.isSuccess()) {
            long maxCacheDurationMs = maxCacheDurationMs(realmConfig);
            return new StorageCredentialCacheEntry(
                scopedCredentialsResult.getStorageAccessConfig(), maxCacheDurationMs);
          }
          LOGGER
              .atDebug()
              .addKeyValue("errorMessage", scopedCredentialsResult.getExtraInformation())
              .log("Failed to get subscoped credentials");
          throw new UnprocessableEntityException(
              "Failed to get subscoped credentials: %s",
              scopedCredentialsResult.getExtraInformation());
        };
    return cache.get(key, loader).toAccessConfig();
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

  @VisibleForTesting
  public void invalidateAll() {
    this.cache.invalidateAll();
  }
}
