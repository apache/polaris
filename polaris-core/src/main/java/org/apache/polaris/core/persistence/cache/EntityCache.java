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
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
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
}
