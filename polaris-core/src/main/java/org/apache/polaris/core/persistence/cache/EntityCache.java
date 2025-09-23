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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;

/** Interface for a Polaris entity cache */
public interface EntityCache {
  /**
   * Remove the specified cache entry from the cache
   *
   * @param cacheEntry cache entry to remove
   */
  void removeCacheEntry(@Nonnull ResolvedPolarisEntity cacheEntry);

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
  @Nullable
  ResolvedPolarisEntity getAndRefreshIfNeeded(
      @Nonnull PolarisCallContext callContext,
      @Nonnull PolarisBaseEntity entityToValidate,
      int entityMinVersion,
      int entityGrantRecordsMinVersion);

  /**
   * Get the specified entity by name and load it if it is not found.
   *
   * @param callContext the Polaris call context
   * @param entityCatalogId id of the catalog where this entity resides or NULL_ID if top-level
   * @param entityId id of the entity to lookup
   * @return null if the entity does not exist or was dropped. Else return the entry for that
   *     entity, either as found in the cache or loaded from the backend
   */
  @Nullable
  EntityCacheLookupResult getOrLoadEntityById(
      @Nonnull PolarisCallContext callContext,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType);

  /**
   * Get the specified entity by name and load it if it is not found.
   *
   * @param callContext the Polaris call context
   * @param entityNameKey name of the entity to load
   * @return null if the entity does not exist or was dropped. Else return the entry for that
   *     entity, either as found in the cache or loaded from the backend
   */
  @Nullable
  EntityCacheLookupResult getOrLoadEntityByName(
      @Nonnull PolarisCallContext callContext, @Nonnull EntityCacheByNameKey entityNameKey);

  /**
   * Load multiple entities by id, returning those found in the cache and loading those not found.
   *
   * <p>Cached entity versions and grant versions must be verified against the versions returned by
   * the {@link
   * org.apache.polaris.core.persistence.PolarisMetaStoreManager#loadEntitiesChangeTracking(PolarisCallContext,
   * List)} API to ensure the returned entities are consistent with the current state of the
   * metastore. Cache implementations must never return a mix of stale entities and fresh entities,
   * as authorization or table conflict decisions could be made based on inconsistent data. For
   * example, a Principal may have a grant to a Principal Role in a cached entry, but that grant may
   * be revoked prior to the Principal Role being granted a privilege on a Catalog. If the Principal
   * record is stale, but the Principal Role is refreshed, the Principal may be incorrectly
   * authorized to access the Catalog.
   *
   * @param callCtx the Polaris call context
   * @param entityType the entity type
   * @param entityIds the list of entity ids to load
   * @return the list of resolved entities, in the same order as the requested entity ids. As in
   *     {@link
   *     org.apache.polaris.core.persistence.PolarisMetaStoreManager#loadResolvedEntities(PolarisCallContext,
   *     PolarisEntityType, List)}, elements in the returned list may be null if the corresponding
   *     entity id does not exist.
   */
  List<EntityCacheLookupResult> getOrLoadResolvedEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityType entityType,
      @Nonnull List<PolarisEntityId> entityIds);

  /**
   * Load multiple entities by {@link EntityNameLookupRecord}, returning those found in the cache
   * and loading those not found.
   *
   * <p>see note in {@link #getOrLoadResolvedEntities(PolarisCallContext, PolarisEntityType, List)}
   * re: the need to validate cache contents against the {@link
   * org.apache.polaris.core.persistence.PolarisMetaStoreManager#loadEntitiesChangeTracking(PolarisCallContext,
   * List)} to avoid invalid authorization or conflict detection decisions based on stale entries.
   *
   * @param callCtx the Polaris call context
   * @param lookupRecords the list of entity name to load
   * @return the list of resolved entities, in the same order as the requested entity records. As in
   *     {@link
   *     org.apache.polaris.core.persistence.PolarisMetaStoreManager#loadResolvedEntities(PolarisCallContext,
   *     PolarisEntityType, List)}, elements in the returned list may be null if the corresponding
   *     entity id does not exist.
   */
  List<EntityCacheLookupResult> getOrLoadResolvedEntities(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<EntityNameLookupRecord> lookupRecords);
}
