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
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;

/**
 * Abstract of Polaris Storage Integration. Each subclass handles credential vending for a specific
 * cloud storage backend (AWS, GCP, Azure).
 *
 * <p>Integration instances are fully bound at construction time to a particular {@link
 * PolarisStorageConfigurationInfo} and {@link RealmConfig}. The credential-vending methods ({@link
 * #getOrLoadSubscopedCreds}, {@link #getSubscopedCreds}) read config and realm state from the
 * instance and therefore do not take them as parameters. Instances for different configs are
 * constructed by {@link PolarisStorageIntegrationProvider}.
 *
 * @param <T> the concrete type of {@link PolarisStorageConfigurationInfo} this integration supports
 */
public abstract class PolarisStorageIntegration<T extends PolarisStorageConfigurationInfo> {

  private final String integrationIdentifierOrId;
  @Nullable private final StorageCredentialCache cache;
  @Nullable private final RealmConfig realmConfig;
  @Nullable private final T storageConfig;

  /**
   * Test-only constructor without cache, realm config, or storage config. Instances built this way
   * must not be used for credential vending — only for {@link #validateAccessToLocations} which
   * takes all of its inputs as method arguments.
   */
  public PolarisStorageIntegration(String identifierOrId) {
    this(identifierOrId, null, null, null);
  }

  public PolarisStorageIntegration(
      String identifierOrId,
      @Nullable StorageCredentialCache cache,
      @Nullable RealmConfig realmConfig,
      @Nullable T storageConfig) {
    this.integrationIdentifierOrId = identifierOrId;
    this.cache = cache;
    this.realmConfig = realmConfig;
    this.storageConfig = storageConfig;
  }

  public String getStorageIdentifierOrId() {
    return integrationIdentifierOrId;
  }

  /** The storage configuration this integration instance is bound to. */
  @Nullable
  public T storageConfig() {
    return storageConfig;
  }

  /** The realm configuration this integration instance is bound to. */
  @Nullable
  protected RealmConfig realmConfig() {
    return realmConfig;
  }

  /**
   * Get subscoped credentials, using the cache if available. Delegates to {@link #buildCacheKey}
   * for cache key construction and {@link #getSubscopedCreds} for actual credential vending.
   */
  public StorageAccessConfig getOrLoadSubscopedCreds(
      boolean allowList,
      @Nonnull Set<String> readLocations,
      @Nonnull Set<String> writeLocations,
      @Nonnull Optional<String> refreshEndpoint,
      @Nonnull CredentialVendingContext context) {
    if (cache != null) {
      StorageCredentialCacheKey key =
          buildCacheKey(allowList, readLocations, writeLocations, refreshEndpoint, context);
      return cache.getOrLoad(
          key,
          realmConfig,
          () ->
              getSubscopedCreds(
                  allowList, readLocations, writeLocations, refreshEndpoint, context));
    }
    return getSubscopedCreds(allowList, readLocations, writeLocations, refreshEndpoint, context);
  }

  /**
   * Build a backend-specific cache key. Each subclass includes only the fields that affect the
   * vended credentials for that backend.
   */
  protected abstract StorageCredentialCacheKey buildCacheKey(
      boolean allowList,
      @Nonnull Set<String> readLocations,
      @Nonnull Set<String> writeLocations,
      @Nonnull Optional<String> refreshEndpoint,
      @Nonnull CredentialVendingContext context);

  /**
   * Subscope credentials for the instance's bound storage configuration. Subclasses implement the
   * actual credential vending logic (e.g. AWS STS AssumeRole, GCP downscoping, Azure SAS
   * generation).
   */
  public abstract StorageAccessConfig getSubscopedCreds(
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
