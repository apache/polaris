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
package org.apache.polaris.core.persistence.cache;

import com.google.common.util.concurrent.Striped;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;

/** The entity cache, can be private or shared */
public class EntityCache {
  private static final Comparator<ResolvedPolarisEntity> RESOLVED_POLARIS_ENTITY_COMPARATOR =
      Comparator.nullsLast(
          Comparator.<ResolvedPolarisEntity>comparingInt(rpe -> rpe.getEntity().getEntityVersion())
              .thenComparingInt(rpe -> rpe.getEntity().getGrantRecordsVersion()));
  private static final int STRIPES = 1_024;

  // cache mode
  private EntityCacheMode cacheMode;

  // the meta store manager
  private final PolarisMetaStoreManager polarisMetaStoreManager;

  // Caffeine cache to keep entries
  private final IndexedCache<CacheKey, ResolvedPolarisEntity> cache;

  // Locks to ensure that an entity can only be refreshed by one thread at a time
  private final Striped<Lock> locks;

  /**
   * Constructor. Cache can be private or shared
   *
   * @param polarisMetaStoreManager the meta store manager implementation
   */
  public EntityCache(@Nonnull PolarisMetaStoreManager polarisMetaStoreManager) {
    // remember the meta store manager
    this.polarisMetaStoreManager = polarisMetaStoreManager;

    // enabled by default
    this.cacheMode = EntityCacheMode.ENABLE;

    // Use a Caffeine cache to purge entries when those have not been used for a long time.
    // Assuming 1KB per entry, 100K entries is about 100MB. Note that each entity is stored twice in
    // the cache, once indexed by its identifier and once indexed by its name.
    this.cache =
        new IndexedCache.Builder<CacheKey, ResolvedPolarisEntity>()
            .primaryKey(e -> new IdKey(e.getEntity().getId()))
            .addSecondaryKey(e -> new NameKey(new EntityCacheByNameKey(e.getEntity())))
            .expireAfterAccess(Duration.ofHours(1))
            .maximumSize(100_000)
            .build();
    this.locks = Striped.lock(STRIPES);
  }

  /**
   * Remove the specified cache entry from the cache. This applies both to the cache entry indexed
   * by its id and by its name.
   *
   * @param cacheEntry cache entry to remove
   */
  public void removeCacheEntry(@Nonnull ResolvedPolarisEntity cacheEntry) {
    IdKey key = new IdKey(cacheEntry.getEntity().getId());
    this.cache.invalidate(key);
  }

  /**
   * Get the current cache mode
   *
   * @return the cache mode
   */
  public EntityCacheMode getCacheMode() {
    return cacheMode;
  }

  /**
   * Allows to change the caching mode for testing
   *
   * @param cacheMode the cache mode
   */
  public void setCacheMode(EntityCacheMode cacheMode) {
    this.cacheMode = cacheMode;
  }

  /**
   * Get a cache entity entry given the id of the entity. If the entry is not found, return null.
   *
   * @param entityId entity id
   * @return the cache entry or null if not found
   */
  public @Nullable ResolvedPolarisEntity getEntityById(long entityId) {
    return this.cache.getIfPresent(new IdKey(entityId));
  }

  /**
   * Get a cache entity entry given the name key of the entity. If the entry is not found, return
   * null.
   *
   * @param entityNameKey entity name key
   * @return the cache entry or null if not found
   */
  public @Nullable ResolvedPolarisEntity getEntityByName(
      @Nonnull EntityCacheByNameKey entityNameKey) {
    return this.cache.getIfPresent(new NameKey(entityNameKey));
  }

  /**
   * Get the cached version of the specified entity and refresh it if needed.
   *
   * <p>This method accepts a fully populated entity. It attempts at resolving the entity from the
   * cache using its id mapping. If the entity is not found in the cache, or if the cached entity
   * has a version older than the specified minimum versions, it is removed from the cache, and
   * reloaded from the underlying datastore and added to the cache. Then it is returned.
   *
   * @param callContext the Polaris call context Refresh the cache if needs be with a version of the
   *     entity/grant records matching the minimum specified version.
   * @param entityToValidate copy of the entity held by the caller to validate
   * @param entityMinVersion minimum expected version. Should be reloaded if found in a cache with a
   *     version less than this one
   * @param entityGrantRecordsMinVersion minimum grant records version which is expected, grants
   *     records should be reloaded if needed
   * @return the cache entry for the entity or null if the specified entity does not exist
   */
  public @Nullable ResolvedPolarisEntity getAndRefreshIfNeeded(
      @Nonnull PolarisCallContext callContext,
      @Nonnull PolarisBaseEntity entityToValidate,
      int entityMinVersion,
      int entityGrantRecordsMinVersion) {
    long entityId = entityToValidate.getId();

    // First lookup the cache to find the existing cache entry
    ResolvedPolarisEntity existingCacheEntry = this.getEntityById(entityId);

    // See if we need to load or refresh that entity
    if (existingCacheEntry != null
        && existingCacheEntry.getEntity().getEntityVersion() >= entityMinVersion
        && existingCacheEntry.getEntity().getGrantRecordsVersion()
            >= entityGrantRecordsMinVersion) {
      // Cache hit and cache entry is up to date, nothing to do.
      return existingCacheEntry;
    }

    // Either cache miss, dropped entity or stale entity. In either case, invalidate and reload it.
    // Do the refresh in a critical section based on the entity ID to avoid race conditions.
    Lock lock = this.locks.get(entityId);
    try {
      lock.lock();

      // Lookup the cache again in case another thread has already invalidated it.
      existingCacheEntry = this.getEntityById(entityId);

      // See if the entity has been refreshed concurrently
      if (existingCacheEntry != null
          && existingCacheEntry.getEntity().getEntityVersion() >= entityMinVersion
          && existingCacheEntry.getEntity().getGrantRecordsVersion()
              >= entityGrantRecordsMinVersion) {
        // Cache hit and cache entry is up to date now, exit
        return existingCacheEntry;
      }

      // We are the first to act upon this entity id, invalidate it
      this.cache.invalidate(new IdKey(entityId));

      // Get the entity from the cache or reload it now that it has been invalidated
      EntityCacheLookupResult cacheLookupResult =
          this.getOrLoadEntityById(
              callContext, entityToValidate.getCatalogId(), entityId, entityToValidate.getType());
      if (cacheLookupResult == null) {
        // Entity has been purged
        return null;
      }
      ResolvedPolarisEntity entity = cacheLookupResult.getCacheEntry();

      // assert that entity, grant records and version are all set
      callContext.getDiagServices().checkNotNull(entity, "unexpected_null_entity");
      callContext
          .getDiagServices()
          .checkNotNull(entity.getAllGrantRecords(), "unexpected_null_grant_records");
      callContext
          .getDiagServices()
          .check(
              entity.getEntity().getGrantRecordsVersion() > 0,
              "unexpected_null_grant_records_version");

      return entity;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get the specified entity by id from the cache or load it if it is not in cache.
   *
   * <p>This method attempts at first loading the entity from the cache using its id mapping. If the
   * entity is not found in the cache, it is loaded from the backend and added to the cache using
   * both its id and name mappings. Then it is returned.
   *
   * @param callContext the Polaris call context
   * @param entityCatalogId id of the catalog where this entity resides or NULL_ID if top-level
   * @param entityId id of the entity to lookup
   * @return null if the entity does not exist or was dropped. Else return the entry for that
   *     entity, either as found in the cache or loaded from the backend
   */
  public @Nullable EntityCacheLookupResult getOrLoadEntityById(
      @Nonnull PolarisCallContext callContext,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    final AtomicBoolean cacheHit = new AtomicBoolean(true);

    ResolvedPolarisEntity entity =
        this.cache.get(
            new IdKey(entityId),
            () -> {
              cacheHit.set(false);
              ResolvedEntityResult result =
                  polarisMetaStoreManager.loadResolvedEntityById(
                      callContext, entityCatalogId, entityId, entityType);
              if (!result.isSuccess()) {
                return null;
              }
              callContext
                  .getDiagServices()
                  .checkNotNull(result.getEntity(), "entity_should_loaded");
              callContext
                  .getDiagServices()
                  .checkNotNull(
                      result.getEntityGrantRecords(), "entity_grant_records_should_loaded");
              return new ResolvedPolarisEntity(
                  callContext.getDiagServices(),
                  result.getEntity(),
                  result.getEntityGrantRecords(),
                  result.getGrantRecordsVersion());
            });

    if (entity == null) {
      return null;
    } else {
      return new EntityCacheLookupResult(entity, cacheHit.get());
    }
  }

  /**
   * Get the specified entity by name from the cache or load it if it is not in cache.
   *
   * <p>This method attempts at first loading the entity from the cache using its name mapping. If
   * the entity is not found in the cache, it is loaded from the backend and added to the cache
   * using both its id and name mappings. Then it is returned.
   *
   * @param callContext the Polaris call context
   * @param entityNameKey name of the entity to load
   * @return null if the entity does not exist or was dropped. Else return the entry for that
   *     entity, either as found in the cache or loaded from the backend
   */
  public @Nullable EntityCacheLookupResult getOrLoadEntityByName(
      @Nonnull PolarisCallContext callContext, @Nonnull EntityCacheByNameKey entityNameKey) {
    final AtomicBoolean cacheHit = new AtomicBoolean(true);
    ResolvedPolarisEntity entity =
        this.cache.get(
            new NameKey(entityNameKey),
            () -> {
              cacheHit.set(false);
              ResolvedEntityResult result =
                  polarisMetaStoreManager.loadResolvedEntityByName(
                      callContext,
                      entityNameKey.getCatalogId(),
                      entityNameKey.getParentId(),
                      entityNameKey.getType(),
                      entityNameKey.getName());
              if (!result.isSuccess()) {
                return null;
              }
              callContext
                  .getDiagServices()
                  .checkNotNull(result.getEntity(), "entity_should_loaded");
              callContext
                  .getDiagServices()
                  .checkNotNull(
                      result.getEntityGrantRecords(), "entity_grant_records_should_loaded");
              return new ResolvedPolarisEntity(
                  callContext.getDiagServices(),
                  result.getEntity(),
                  result.getEntityGrantRecords(),
                  result.getGrantRecordsVersion());
            });

    if (entity == null) {
      return null;
    } else {
      return new EntityCacheLookupResult(entity, cacheHit.get());
    }
  }
}
