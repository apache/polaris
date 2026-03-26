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
package org.apache.polaris.core.storage;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.storage.cache.StorageAccessConfigParameters;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;

/**
 * Abstract of Polaris Storage Integration. Each subclass handles credential vending for a specific
 * cloud storage backend (AWS, GCP, Azure).
 *
 * <p>Integrations are expected to be singletons — they do not hold per-entity storage
 * configuration. Instead, configuration is passed as method parameters at call time. Per-request
 * state (realm config, realm ID) is resolved via suppliers provided at construction time.
 *
 * @param <T> the concrete type of {@link PolarisStorageConfigurationInfo} this integration supports
 */
public abstract class PolarisStorageIntegration<T extends PolarisStorageConfigurationInfo> {

  private final String integrationIdentifierOrId;
  @Nullable private final StorageCredentialCache cache;
  private final Supplier<RealmConfig> realmConfigSupplier;

  /** Test-only constructor without cache or request-scoped suppliers. */
  public PolarisStorageIntegration(String identifierOrId) {
    this(identifierOrId, null, () -> null);
  }

  public PolarisStorageIntegration(
      String identifierOrId,
      @Nullable StorageCredentialCache cache,
      Supplier<RealmConfig> realmConfigSupplier) {
    this.integrationIdentifierOrId = identifierOrId;
    this.cache = cache;
    this.realmConfigSupplier = realmConfigSupplier;
  }

  public String getStorageIdentifierOrId() {
    return integrationIdentifierOrId;
  }

  protected Supplier<RealmConfig> realmConfigSupplier() {
    return realmConfigSupplier;
  }

  /**
   * Get subscoped credentials, using the cache if available. Resolves realm config and realm ID
   * from suppliers. Delegates to {@link #buildCacheKey} for cache key construction and {@link
   * #getSubscopedCreds} for actual credential vending.
   */
  public StorageAccessConfig getOrLoadSubscopedCreds(
      @Nonnull PolarisEntity entity,
      boolean allowList,
      @Nonnull Set<String> readLocations,
      @Nonnull Set<String> writeLocations,
      @Nonnull Optional<String> refreshEndpoint,
      @Nonnull CredentialVendingContext context) {
    RealmConfig realmConfig = realmConfigSupplier.get();
    if (cache != null) {
      StorageAccessConfigParameters key =
          buildCacheKey(
              entity,
              realmConfig,
              allowList,
              readLocations,
              writeLocations,
              refreshEndpoint,
              context);
      return cache.getOrLoad(
          key,
          realmConfig,
          () ->
              getSubscopedCreds(
                  realmConfig,
                  entity,
                  allowList,
                  readLocations,
                  writeLocations,
                  refreshEndpoint,
                  context));
    }
    return getSubscopedCreds(
        realmConfig, entity, allowList, readLocations, writeLocations, refreshEndpoint, context);
  }

  /**
   * Build a backend-specific cache key. Each subclass includes only the fields that affect the
   * vended credentials for that backend. Realm ID is available via {@link #realmIdSupplier()}.
   */
  protected abstract StorageAccessConfigParameters buildCacheKey(
      @Nonnull PolarisEntity entity,
      @Nonnull RealmConfig realmConfig,
      boolean allowList,
      @Nonnull Set<String> readLocations,
      @Nonnull Set<String> writeLocations,
      @Nonnull Optional<String> refreshEndpoint,
      @Nonnull CredentialVendingContext context);

  /**
   * Subscope credentials for the given entity and locations. Subclasses implement the actual
   * credential vending logic (e.g. AWS STS AssumeRole, GCP downscoping, Azure SAS generation).
   */
  public abstract StorageAccessConfig getSubscopedCreds(
      @Nonnull RealmConfig realmConfig,
      @Nonnull PolarisEntity entity,
      boolean allowList,
      @Nonnull Set<String> readLocations,
      @Nonnull Set<String> writeLocations,
      @Nonnull Optional<String> refreshEndpoint,
      @Nonnull CredentialVendingContext context);

  /**
   * Validate access for the provided operation actions and locations.
   *
   * @param actions a set of operation actions to validate, like LIST/READ/DELETE/WRITE/ALL
   * @param locations a set of locations to get access to
   * @return A Map of string, representing the result of validation, the key value is {@code
   *     <location, validate result>}. A validate result looks like this
   *     <pre>
   * {
   *   "status" : "failure",
   *   "actions" : {
   *     "READ" : {
   *       "message" : "The specified file was not found",
   *       "status" : "failure"
   *     },
   *     "DELETE" : {
   *       "message" : "One or more objects could not be deleted (Status Code: 200; Error Code: null)",
   *       "status" : "failure"
   *     },
   *     "LIST" : {
   *       "status" : "success"
   *     },
   *     "WRITE" : {
   *       "message" : "Access Denied (Status Code: 403; Error Code: AccessDenied)",
   *       "status" : "failure"
   *     }
   *   },
   *   "message" : "Some of the integration checks failed. Check the Polaris documentation for more information."
   * }
   * </pre>
   */
  @Nonnull
  public abstract Map<String, Map<PolarisStorageActions, ValidationResult>>
      validateAccessToLocations(
          @Nonnull RealmConfig realmConfig,
          @Nonnull T storageConfig,
          @Nonnull Set<PolarisStorageActions> actions,
          @Nonnull Set<String> locations);

  /**
   * Result of calling {@link #validateAccessToLocations(RealmConfig,
   * PolarisStorageConfigurationInfo, Set, Set)}
   */
  public record ValidationResult(boolean success, String message) {}
}
