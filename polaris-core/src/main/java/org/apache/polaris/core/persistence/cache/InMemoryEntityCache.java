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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.collect.HashBiMap;
import com.google.common.util.concurrent.MoreExecutors;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import net.jcip.annotations.GuardedBy;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.BehaviorChangeConfiguration;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfiguration;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;

/** The entity cache, can be private or shared */
public class InMemoryEntityCache implements EntityCache {
  // cache mode
  private EntityCacheMode cacheMode;

  // the meta store manager
  private final PolarisMetaStoreManager polarisMetaStoreManager;

  // Caffeine cache to keep entries
  @GuardedBy("lock")
  private final Cache<Long, ResolvedPolarisEntity> cache;

  // Map of entity names to entity ids
  @GuardedBy("lock")
  private final HashBiMap<Long, EntityCacheByNameKey> nameToIdMap;

  // Locks to ensure that an entity can only be refreshed by one thread at a time
  private final ReentrantReadWriteLock lock;

  /**
   * Constructor. Cache can be private or shared
   *
   * @param polarisMetaStoreManager the meta store manager implementation
   */
  public InMemoryEntityCache(@Nonnull PolarisMetaStoreManager polarisMetaStoreManager) {
    // remember the meta store manager
    this.polarisMetaStoreManager = polarisMetaStoreManager;

    // enabled by default
    this.cacheMode = EntityCacheMode.ENABLE;

    this.nameToIdMap = HashBiMap.create();
    this.lock = new ReentrantReadWriteLock();

    long weigherTarget =
        PolarisConfiguration.loadConfig(FeatureConfiguration.ENTITY_CACHE_WEIGHER_TARGET);
    Caffeine<Long, ResolvedPolarisEntity> cacheBuilder =
        Caffeine.newBuilder()
            .maximumWeight(weigherTarget)
            .weigher(EntityWeigher.asWeigher())
            .expireAfterAccess(1, TimeUnit.HOURS) // Expire entries after 1 hour of no access
            // Add a removal listener so that the name-to-id mapping is also purged after every
            // cache
            // replacement, invalidation or expiration .  Configure a direct executor so that the
            // removal
            // listener is invoked by the same thread that is updating the cache.
            .removalListener(this::remove)
            .executor(MoreExecutors.directExecutor());

    if (PolarisConfiguration.loadConfig(BehaviorChangeConfiguration.ENTITY_CACHE_SOFT_VALUES)) {
      cacheBuilder.softValues();
    }

    this.cache = cacheBuilder.build();
  }

  private void remove(Long id, ResolvedPolarisEntity ignored1, RemovalCause ignored2) {
    lock.writeLock().lock();
    try {
      cache.invalidate(id);
      nameToIdMap.remove(id);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Remove the specified cache entry from the cache. This applies both to the cache entry indexed
   * by its id and by its name.
   *
   * @param cacheEntry cache entry to remove
   */
  @Override
  public void removeCacheEntry(@Nonnull ResolvedPolarisEntity cacheEntry) {
    remove(cacheEntry.getEntity().getId(), cacheEntry, RemovalCause.EXPLICIT);
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
    lock.readLock().lock();
    try {
      return cache.getIfPresent(entityId);
    } finally {
      lock.readLock().unlock();
    }
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
    lock.readLock().lock();
    try {
      Long entityId = nameToIdMap.inverse().get(entityNameKey);
      return entityId == null ? null : cache.getIfPresent(entityId);
    } finally {
      lock.readLock().unlock();
    }
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
  @Override
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
    lock.writeLock().lock();
    try {
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

      // Either confirmed cache miss, or the entity is stale. Invalidate it just in case.
      cache.invalidate(entityId);

      // Get the entity from the cache or reload it now that it has been invalidated
      EntityCacheLookupResult cacheLookupResult =
          this.maybeLoadEntityById(
              callContext, entityToValidate.getCatalogId(), entityId, entityToValidate.getType());
      if (cacheLookupResult == null) {
        // Entity has been purged, and we have already cleaned the cache and the name-to-id mapping,
        // nothing else to do.
        return null;
      }

      ResolvedPolarisEntity entity = cacheLookupResult.getCacheEntry();

      // assert version is set, the other assertions have already been performed
      callContext
          .getDiagServices()
          .check(
              entity.getEntity().getGrantRecordsVersion() > 0,
              "unexpected_null_grant_records_version");

      return entity;
    } finally {
      lock.writeLock().unlock();
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
  @Override
  public @Nullable EntityCacheLookupResult getOrLoadEntityById(
      @Nonnull PolarisCallContext callContext,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    ResolvedPolarisEntity cachedEntity = getEntityById(entityId);
    if (cachedEntity != null) {
      // Cache hit, nothing more to do
      return new EntityCacheLookupResult(cachedEntity, true);
    }

    // Cache miss, we may have to load the entity from the metastore
    return maybeLoadEntityById(callContext, entityCatalogId, entityId, entityType);
  }

  private EntityCacheLookupResult maybeLoadEntityById(
      PolarisCallContext callContext,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    lock.writeLock().lock();
    try {
      ResolvedPolarisEntity cachedEntity = getEntityById(entityId);
      if (cachedEntity != null) {
        // Cache was populated concurrently, nothing more to do, exit the critical section as
        // quickly as possible
        return new EntityCacheLookupResult(cachedEntity, true);
      }

      ResolvedEntityResult result =
          polarisMetaStoreManager.loadResolvedEntityById(
              callContext, entityCatalogId, entityId, entityType);
      if (!result.isSuccess()) {
        // Entity not found, and it was not in the cache, nothing else to do
        return null;
      }

      PolarisBaseEntity entity = result.getEntity();
      callContext.getDiagServices().checkNotNull(entity, "entity_should_loaded");
      callContext
          .getDiagServices()
          .checkNotNull(result.getEntityGrantRecords(), "entity_grant_records_should_loaded");

      // Entity found, add it to the cache and update the name-to-id mapping
      // If the name was already associated with another ID, overwrite the mapping
      cachedEntity =
          new ResolvedPolarisEntity(
              callContext.getDiagServices(),
              entity,
              result.getEntityGrantRecords(),
              result.getGrantRecordsVersion());
      cacheEntity(entityId, new EntityCacheByNameKey(entity), cachedEntity);

      return new EntityCacheLookupResult(cachedEntity, false);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void cacheEntity(
      long entityId, EntityCacheByNameKey entityName, ResolvedPolarisEntity cachedEntity) {
    lock.writeLock().lock();
    try {
      Long previouslyAssociatedId = nameToIdMap.inverse().get(entityName);
      if (previouslyAssociatedId != null && previouslyAssociatedId != entityId) {
        // That name was already associated with another entity, and the cache is still representing
        // that state, invalidate it.  The name-to-id mapping will be cleared automatically.
        cache.invalidate(previouslyAssociatedId);
      }
      cache.put(entityId, cachedEntity);
      nameToIdMap.put(entityId, entityName);
    } finally {
      lock.writeLock().unlock();
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
  @Override
  public @Nullable EntityCacheLookupResult getOrLoadEntityByName(
      @Nonnull PolarisCallContext callContext, @Nonnull EntityCacheByNameKey entityNameKey) {
    ResolvedPolarisEntity cachedEntity = getEntityByName(entityNameKey);
    if (cachedEntity != null) {
      // Cache hit, nothing more to do
      return new EntityCacheLookupResult(cachedEntity, true);
    }

    // Cache miss, we may have to load the entity from the metastore
    return maybeLoadEntityByName(callContext, entityNameKey);
  }

  private EntityCacheLookupResult maybeLoadEntityByName(
      PolarisCallContext callContext, EntityCacheByNameKey entityNameKey) {
    lock.writeLock().lock();
    try {
      ResolvedPolarisEntity cachedEntity = getEntityByName(entityNameKey);
      if (cachedEntity != null) {
        // Cache was populated concurrently, nothing more to do, exit the critical section as
        // quickly as possible
        return new EntityCacheLookupResult(cachedEntity, true);
      }

      ResolvedEntityResult result =
          polarisMetaStoreManager.loadResolvedEntityByName(
              callContext,
              entityNameKey.getCatalogId(),
              entityNameKey.getParentId(),
              entityNameKey.getType(),
              entityNameKey.getName());
      if (!result.isSuccess()) {
        // Entity not found, and it was not in the cache, nothing else to do
        return null;
      }

      PolarisBaseEntity entity = result.getEntity();
      callContext.getDiagServices().checkNotNull(entity, "entity_should_loaded");
      callContext
          .getDiagServices()
          .checkNotNull(result.getEntityGrantRecords(), "entity_grant_records_should_loaded");

      // Entity found, add it to the cache and update the name-to-id mapping
      // If the name was already associated with another ID, overwrite the mapping
      cachedEntity =
          new ResolvedPolarisEntity(
              callContext.getDiagServices(),
              entity,
              result.getEntityGrantRecords(),
              result.getGrantRecordsVersion());
      cacheEntity(entity.getId(), new EntityCacheByNameKey(entity), cachedEntity);

      return new EntityCacheLookupResult(cachedEntity, false);
    } finally {
      lock.writeLock().unlock();
    }
  }
}
