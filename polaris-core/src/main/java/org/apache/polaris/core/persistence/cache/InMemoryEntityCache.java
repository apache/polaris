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
import com.github.benmanes.caffeine.cache.RemovalListener;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.AbstractMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.BehaviorChangeConfiguration;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;

/** An in-memory entity cache with a limit of 100k entities and a 1h TTL. */
public class InMemoryEntityCache implements EntityCache {

  // cache mode
  private EntityCacheMode cacheMode;

  // the meta store manager
  private final PolarisMetaStoreManager polarisMetaStoreManager;

  // Caffeine cache to keep entries by id
  private final Cache<Long, ResolvedPolarisEntity> byId;

  // index by name
  private final AbstractMap<EntityCacheByNameKey, ResolvedPolarisEntity> byName;

  /**
   * Constructor. Cache can be private or shared
   *
   * @param polarisMetaStoreManager the meta store manager implementation
   */
  public InMemoryEntityCache(
      @Nonnull RealmConfig realmConfig, @Nonnull PolarisMetaStoreManager polarisMetaStoreManager) {

    // by name cache
    this.byName = new ConcurrentHashMap<>();

    // When an entry is removed, we simply remove it from the byName map
    RemovalListener<Long, ResolvedPolarisEntity> removalListener =
        (key, value, cause) -> {
          if (value != null) {
            // compute name key
            EntityCacheByNameKey nameKey = new EntityCacheByNameKey(value.getEntity());

            // if it is still active, remove it from the name key
            this.byName.remove(nameKey, value);
          }
        };

    long weigherTarget = realmConfig.getConfig(FeatureConfiguration.ENTITY_CACHE_WEIGHER_TARGET);
    Caffeine<Long, ResolvedPolarisEntity> byIdBuilder =
        Caffeine.newBuilder()
            .maximumWeight(weigherTarget)
            .weigher(EntityWeigher.asWeigher())
            .expireAfterAccess(1, TimeUnit.HOURS) // Expire entries after 1 hour of no access
            .removalListener(removalListener); // Set the removal listener

    boolean useSoftValues =
        realmConfig.getConfig(BehaviorChangeConfiguration.ENTITY_CACHE_SOFT_VALUES);
    if (useSoftValues) {
      byIdBuilder.softValues();
    }

    // use a Caffeine cache to purge entries when those have not been used for a long time.
    this.byId = byIdBuilder.build();

    // remember the meta store manager
    this.polarisMetaStoreManager = polarisMetaStoreManager;

    // enabled by default
    this.cacheMode = EntityCacheMode.ENABLE;
  }

  /**
   * Remove the specified cache entry from the cache
   *
   * @param cacheEntry cache entry to remove
   */
  @Override
  public void removeCacheEntry(@Nonnull ResolvedPolarisEntity cacheEntry) {
    // compute name key
    EntityCacheByNameKey nameKey = new EntityCacheByNameKey(cacheEntry.getEntity());

    // remove this old entry, this will immediately remove the named entry
    this.byId.asMap().remove(cacheEntry.getEntity().getId(), cacheEntry);

    // remove it from the name key
    this.byName.remove(nameKey, cacheEntry);
  }

  /**
   * Cache new entry
   *
   * @param cacheEntry new cache entry
   */
  private void cacheNewEntry(@Nonnull ResolvedPolarisEntity cacheEntry) {

    // compute name key
    EntityCacheByNameKey nameKey = new EntityCacheByNameKey(cacheEntry.getEntity());

    // get old value if one exist
    ResolvedPolarisEntity oldCacheEntry = this.byId.getIfPresent(cacheEntry.getEntity().getId());

    // put new entry, only if really newer one
    this.byId
        .asMap()
        .merge(
            cacheEntry.getEntity().getId(),
            cacheEntry,
            (oldValue, newValue) -> this.isNewer(newValue, oldValue) ? newValue : oldValue);

    // only update the name key if this entity was not dropped
    if (!cacheEntry.getEntity().isDropped()) {
      // here we don't really care about concurrent update to the key. Basically if we are
      // pointing to the wrong entry, we will detect this and fix the issue
      this.byName.put(nameKey, cacheEntry);
    }

    // remove old name if it has changed
    if (oldCacheEntry != null) {
      // old name
      EntityCacheByNameKey oldNameKey = new EntityCacheByNameKey(oldCacheEntry.getEntity());
      if (!oldNameKey.equals(nameKey)) {
        this.byName.remove(oldNameKey, oldCacheEntry);
      }
    }
  }

  /**
   * Determine if the newer value is really newer
   *
   * @param newValue new cache entry
   * @param oldValue old cache entry
   * @return true if the newer cache entry
   */
  private boolean isNewer(ResolvedPolarisEntity newValue, ResolvedPolarisEntity oldValue) {
    return (newValue.getEntity().getEntityVersion() > oldValue.getEntity().getEntityVersion()
        || newValue.getEntity().getGrantRecordsVersion()
            > oldValue.getEntity().getGrantRecordsVersion());
  }

  /**
   * Replace an old entry with a new one
   *
   * @param oldCacheEntry old entry
   * @param newCacheEntry new entry
   */
  private void replaceCacheEntry(
      @Nullable ResolvedPolarisEntity oldCacheEntry, @Nonnull ResolvedPolarisEntity newCacheEntry) {

    // need to remove old?
    if (oldCacheEntry != null) {
      // only replace if there is a difference
      if (this.entityNameKeyMismatch(oldCacheEntry.getEntity(), newCacheEntry.getEntity())
          || oldCacheEntry.getEntity().getEntityVersion()
              < newCacheEntry.getEntity().getEntityVersion()
          || oldCacheEntry.getEntity().getGrantRecordsVersion()
              < newCacheEntry.getEntity().getGrantRecordsVersion()) {
        // write new one
        this.cacheNewEntry(newCacheEntry);

        // delete the old one assuming it has not been replaced by the above new entry
        this.removeCacheEntry(oldCacheEntry);
      }
    } else {
      // write new one
      this.cacheNewEntry(newCacheEntry);
    }
  }

  /**
   * Check if two entities have different cache keys (either by id or by name)
   *
   * @param entity the entity
   * @param otherEntity the other entity
   * @return true if there is a mismatch
   */
  private boolean entityNameKeyMismatch(
      @Nonnull PolarisBaseEntity entity, @Nonnull PolarisBaseEntity otherEntity) {
    return entity.getId() != otherEntity.getId()
        || entity.getParentId() != otherEntity.getParentId()
        || !entity.getName().equals(otherEntity.getName())
        || entity.getTypeCode() != otherEntity.getTypeCode();
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
   * Get a cache entity entry given the id of the entity
   *
   * @param entityId entity id
   * @return the cache entry or null if not found
   */
  public @Nullable ResolvedPolarisEntity getEntityById(long entityId) {
    return byId.getIfPresent(entityId);
  }

  /**
   * Get a cache entity entry given the name key of the entity
   *
   * @param entityNameKey entity name key
   * @return the cache entry or null if not found
   */
  public @Nullable ResolvedPolarisEntity getEntityByName(
      @Nonnull EntityCacheByNameKey entityNameKey) {
    return byName.get(entityNameKey);
  }

  /**
   * Refresh the cache if needs be with a version of the entity/grant records matching the minimum
   * specified version.
   *
   * @param callContext the Polaris call context
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
    long entityCatalogId = entityToValidate.getCatalogId();
    long entityId = entityToValidate.getId();
    PolarisEntityType entityType = entityToValidate.getType();

    // first lookup the cache to find the existing cache entry
    ResolvedPolarisEntity existingCacheEntry = this.getEntityById(entityId);

    // the caller's fetched entity may have come from a stale lookup byName; we should consider
    // the existingCacheEntry to be the older of the two for purposes of invalidation to make
    // sure when we replaceCacheEntry we're also removing the old name if it's no longer valid
    EntityCacheByNameKey nameKey = new EntityCacheByNameKey(entityToValidate);
    ResolvedPolarisEntity existingCacheEntryByName = this.getEntityByName(nameKey);
    if (existingCacheEntryByName != null
        && existingCacheEntry != null
        && isNewer(existingCacheEntry, existingCacheEntryByName)) {
      existingCacheEntry = existingCacheEntryByName;
    }

    // the new one to be returned
    final ResolvedPolarisEntity newCacheEntry;

    // see if we need to load or refresh that entity
    if (existingCacheEntry == null
        || existingCacheEntry.getEntity().getEntityVersion() < entityMinVersion
        || existingCacheEntry.getEntity().getGrantRecordsVersion() < entityGrantRecordsMinVersion) {

      // the refreshed entity
      final ResolvedEntityResult refreshedCacheEntry;

      // was not found in the cache?
      final PolarisBaseEntity entity;
      final List<PolarisGrantRecord> grantRecords;
      final int grantRecordsVersion;
      if (existingCacheEntry == null) {
        // try to load it
        refreshedCacheEntry =
            this.polarisMetaStoreManager.loadResolvedEntityById(
                callContext, entityCatalogId, entityId, entityType);
        if (refreshedCacheEntry.isSuccess()) {
          entity = refreshedCacheEntry.getEntity();
          grantRecords = refreshedCacheEntry.getEntityGrantRecords();
          grantRecordsVersion = refreshedCacheEntry.getGrantRecordsVersion();
        } else {
          return null;
        }
      } else {
        // refresh it
        refreshedCacheEntry =
            this.polarisMetaStoreManager.refreshResolvedEntity(
                callContext,
                existingCacheEntry.getEntity().getEntityVersion(),
                existingCacheEntry.getEntity().getGrantRecordsVersion(),
                entityType,
                entityCatalogId,
                entityId);
        if (refreshedCacheEntry.isSuccess()) {
          entity =
              (refreshedCacheEntry.getEntity() != null)
                  ? refreshedCacheEntry.getEntity()
                  : existingCacheEntry.getEntity();
          if (refreshedCacheEntry.getEntityGrantRecords() != null) {
            grantRecords = refreshedCacheEntry.getEntityGrantRecords();
            grantRecordsVersion = refreshedCacheEntry.getGrantRecordsVersion();
          } else {
            grantRecords = existingCacheEntry.getAllGrantRecords();
            grantRecordsVersion = existingCacheEntry.getEntity().getGrantRecordsVersion();
          }
        } else {
          // entity has been purged, remove it
          this.removeCacheEntry(existingCacheEntry);
          return null;
        }
      }

      // assert that entity, grant records and version are all set
      callContext.getDiagServices().checkNotNull(entity, "unexpected_null_entity");
      callContext.getDiagServices().checkNotNull(grantRecords, "unexpected_null_grant_records");
      callContext
          .getDiagServices()
          .check(grantRecordsVersion > 0, "unexpected_null_grant_records_version");

      // create new cache entry
      newCacheEntry =
          new ResolvedPolarisEntity(
              callContext.getDiagServices(), entity, grantRecords, grantRecordsVersion);

      // insert cache entry
      this.replaceCacheEntry(existingCacheEntry, newCacheEntry);
    } else {
      // found it in the cache and it is up-to-date, simply return it
      newCacheEntry = existingCacheEntry;
    }

    return newCacheEntry;
  }

  /**
   * Get the specified entity by name and load it if it is not found.
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

    // if it exists, we are set
    ResolvedPolarisEntity entry = this.getEntityById(entityId);
    final boolean cacheHit;

    // we need to load it if it does not exist
    if (entry == null) {
      // this is a miss
      cacheHit = false;

      // load it
      ResolvedEntityResult result =
          polarisMetaStoreManager.loadResolvedEntityById(
              callContext, entityCatalogId, entityId, entityType);

      // not found, exit
      if (!result.isSuccess()) {
        return null;
      }

      // if found, setup entry
      callContext.getDiagServices().checkNotNull(result.getEntity(), "entity_should_loaded");
      callContext
          .getDiagServices()
          .checkNotNull(result.getEntityGrantRecords(), "entity_grant_records_should_loaded");
      entry =
          new ResolvedPolarisEntity(
              callContext.getDiagServices(),
              result.getEntity(),
              result.getEntityGrantRecords(),
              result.getGrantRecordsVersion());

      // the above loading could take a long time so check again if the entry exists and only
      this.cacheNewEntry(entry);
    } else {
      cacheHit = true;
    }

    // return what we found
    return new EntityCacheLookupResult(entry, cacheHit);
  }

  /**
   * Get the specified entity by name and load it if it is not found.
   *
   * @param callContext the Polaris call context
   * @param entityNameKey name of the entity to load
   * @return null if the entity does not exist or was dropped. Else return the entry for that
   *     entity, either as found in the cache or loaded from the backend
   */
  @Override
  public @Nullable EntityCacheLookupResult getOrLoadEntityByName(
      @Nonnull PolarisCallContext callContext, @Nonnull EntityCacheByNameKey entityNameKey) {

    // if it exists, we are set
    ResolvedPolarisEntity entry = this.getEntityByName(entityNameKey);
    final boolean cacheHit;

    // we need to load it if it does not exist
    if (entry == null) {
      // this is a miss
      cacheHit = false;

      // load it
      ResolvedEntityResult result =
          polarisMetaStoreManager.loadResolvedEntityByName(
              callContext,
              entityNameKey.getCatalogId(),
              entityNameKey.getParentId(),
              entityNameKey.getType(),
              entityNameKey.getName());

      // not found, exit
      if (!result.isSuccess()) {
        return null;
      }

      // validate return
      callContext.getDiagServices().checkNotNull(result.getEntity(), "entity_should_loaded");
      callContext
          .getDiagServices()
          .checkNotNull(result.getEntityGrantRecords(), "entity_grant_records_should_loaded");

      // if found, setup entry
      entry =
          new ResolvedPolarisEntity(
              callContext.getDiagServices(),
              result.getEntity(),
              result.getEntityGrantRecords(),
              result.getGrantRecordsVersion());

      // the above loading could take a long time so check again if the entry exists and only
      this.cacheNewEntry(entry);
    } else {
      cacheHit = true;
    }

    // return what we found
    return new EntityCacheLookupResult(entry, cacheHit);
  }
}
