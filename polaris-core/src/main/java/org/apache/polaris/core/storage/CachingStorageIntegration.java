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

import java.util.List;
import java.util.Optional;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Abstract base for storage integrations that cache vended credentials in an in-memory {@link
 * StorageCredentialCache}. Each subclass handles credential vending for a specific cloud storage
 * backend (AWS, GCP, Azure).
 *
 * <p>Integration instances are fully bound at construction time to a particular {@link
 * PolarisStorageConfigurationInfo} and {@link RealmConfig}. The public {@link
 * #getStorageAccessConfig} method builds a backend-specific {@link StorageCredentialCacheKey} via
 * {@link #buildCacheKey} and either delegates to the shared cache or calls {@link
 * StorageCredentialCacheKey#load()} directly when no cache is present. The actual credential
 * vending lives behind {@code key.load()}, which subclasses wire up by stamping themselves onto the
 * key as an auxiliary field.
 *
 * @param <T> the concrete type of {@link PolarisStorageConfigurationInfo} this integration supports
 */
public abstract class CachingStorageIntegration<T extends PolarisStorageConfigurationInfo>
    implements PolarisStorageIntegration {

  @Nullable private final StorageCredentialCache cache;
  private final RealmConfig realmConfig;
  private final T storageConfig;

  protected CachingStorageIntegration(
      @Nullable StorageCredentialCache cache,
      @NonNull RealmConfig realmConfig,
      @NonNull T storageConfig) {
    this.cache = cache;
    this.realmConfig = realmConfig;
    this.storageConfig = storageConfig;
  }

  /** The storage configuration this integration instance is bound to. */
  public T storageConfig() {
    return storageConfig;
  }

  /** The realm configuration this integration instance is bound to. */
  public RealmConfig realmConfig() {
    return realmConfig;
  }

  @Override
  public final StorageAccessConfig getStorageAccessConfig(
      @NonNull List<LocationGrant> grants,
      @NonNull Optional<String> refreshEndpoint,
      @NonNull CredentialVendingContext context) {
    StorageCredentialCacheKey key = buildCacheKey(grants, refreshEndpoint, context);
    return cache != null ? cache.getOrLoad(key) : key.load();
  }

  /**
   * Build a backend-specific cache key for the given vending request. The key must carry whatever
   * data fields drive identity for cache lookup and an aux reference back to this integration so
   * {@link StorageCredentialCacheKey#load()} can mint credentials on miss.
   */
  protected abstract StorageCredentialCacheKey buildCacheKey(
      @NonNull List<LocationGrant> grants,
      @NonNull Optional<String> refreshEndpoint,
      @NonNull CredentialVendingContext context);
}
