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
package org.apache.polaris.core.persistence.resolver;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.cache.EntityCacheEntry;

/**
 * REST request resolver, allows to resolve all entities referenced directly or indirectly by in
 * incoming rest request, Once resolved, the request can be authorized.
 */
public interface Resolver {

  // TODO The contract of this interface (was a concrete class before) is not to deal with cache
  //  details, and callers almost always extract the `PolarisBaseEntity` from the
  //  `EntityCacheEntry`, or build a `ResolvedPolarisEntity` from it. The indirection via
  //  `EntityCacheEntry` leads to boilerplate code exposing internal details, and should really be
  //  avoided.

  /**
   * @return the principal we resolved
   */
  @Nonnull
  EntityCacheEntry getResolvedCallerPrincipal();

  /**
   * @return all principal roles which were activated. The list can be empty
   */
  @Nonnull
  List<EntityCacheEntry> getResolvedCallerPrincipalRoles();

  /**
   * @return the reference catalog which has been resolved. Will be null if null was passed in for
   *     the parameter referenceCatalogName when the Resolver was constructed.
   */
  @Nullable
  EntityCacheEntry getResolvedReferenceCatalog();

  /**
   * Empty map if no catalog was resolved. Else the list of catalog roles which are activated by the
   * caller
   *
   * @return map of activated catalog roles or null if no referenceCatalogName was specified
   */
  @Nullable
  Map<Long, EntityCacheEntry> getResolvedCatalogRoles();

  /**
   * Get path which has been resolved, should be used only when a single path was added to the
   * resolver. If the path to resolve was optional, only the prefix that was resolved will be
   * returned.
   *
   * @return single resolved path
   */
  @Nonnull
  List<EntityCacheEntry> getResolvedPath();

  /**
   * One of more resolved path, in the order they were added to the resolver.
   *
   * @return list of resolved path
   */
  @Nonnull
  List<List<EntityCacheEntry>> getResolvedPaths();

  /**
   * Get resolved entity associated to the specified type and name or null if not found
   *
   * @param entityType type of the entity, cannot be a NAMESPACE or a TABLE_LIKE entity. If it is a
   *     top-level catalog entity (i.e. CATALOG_ROLE), a reference catalog must have been specified
   *     at construction time.
   * @param entityName name of the entity.
   * @return the entity which has been resolved or null if that entity does not exist
   */
  @Nullable
  EntityCacheEntry getResolvedEntity(
      @Nonnull PolarisEntityType entityType, @Nonnull String entityName);
}
