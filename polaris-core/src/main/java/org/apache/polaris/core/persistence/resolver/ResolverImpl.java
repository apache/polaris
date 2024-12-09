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
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.cache.EntityCacheByNameKey;
import org.apache.polaris.core.persistence.cache.EntityCacheEntry;

final class ResolverImpl implements Resolver {
  private final String referenceCatalogName;
  private final @Nonnull PolarisDiagnostics diagnostics;
  private final EntityCacheEntry resolvedCallerPrincipal;
  private final List<EntityCacheEntry> resolvedCallerPrincipalRoles;
  private final EntityCacheEntry resolvedReferenceCatalog;
  private final Map<Long, EntityCacheEntry> resolvedCatalogRoles;
  private final List<List<EntityCacheEntry>> resolvedPaths;
  private final Map<EntityCacheByNameKey, EntityCacheEntry> resolvedEntriesByName;

  ResolverImpl(
      String referenceCatalogName,
      @Nonnull PolarisDiagnostics diagnostics,
      EntityCacheEntry resolvedCallerPrincipal,
      List<EntityCacheEntry> resolvedCallerPrincipalRoles,
      EntityCacheEntry resolvedReferenceCatalog,
      Map<Long, EntityCacheEntry> resolvedCatalogRoles,
      List<List<EntityCacheEntry>> resolvedPaths,
      Map<EntityCacheByNameKey, EntityCacheEntry> resolvedEntriesByName) {
    this.referenceCatalogName = referenceCatalogName;
    this.diagnostics = diagnostics;
    this.resolvedCallerPrincipal = resolvedCallerPrincipal;
    this.resolvedCallerPrincipalRoles = resolvedCallerPrincipalRoles;
    this.resolvedReferenceCatalog = resolvedReferenceCatalog;
    this.resolvedCatalogRoles = resolvedCatalogRoles;
    this.resolvedPaths = resolvedPaths;
    this.resolvedEntriesByName = resolvedEntriesByName;
  }

  @Override
  @Nonnull
  public EntityCacheEntry getResolvedCallerPrincipal() {
    return resolvedCallerPrincipal;
  }

  @Override
  @Nonnull
  public List<EntityCacheEntry> getResolvedCallerPrincipalRoles() {
    return resolvedCallerPrincipalRoles;
  }

  @Override
  @Nullable
  public EntityCacheEntry getResolvedReferenceCatalog() {
    return resolvedReferenceCatalog;
  }

  @Override
  @Nullable
  public Map<Long, EntityCacheEntry> getResolvedCatalogRoles() {
    return resolvedCatalogRoles;
  }

  @Override
  @Nonnull
  public List<EntityCacheEntry> getResolvedPath() {
    this.diagnostics.check(this.resolvedPaths.size() == 1, "only_if_single");

    return resolvedPaths.get(0);
  }

  @Override
  @Nonnull
  public List<List<EntityCacheEntry>> getResolvedPaths() {
    this.diagnostics.check(!this.resolvedPaths.isEmpty(), "no_path_resolved");

    return resolvedPaths;
  }

  @Override
  @Nullable
  public EntityCacheEntry getResolvedEntity(
      @Nonnull PolarisEntityType entityType, @Nonnull String entityName) {
    // validate input
    diagnostics.check(
        entityType != PolarisEntityType.NAMESPACE && entityType != PolarisEntityType.TABLE_LIKE,
        "cannot_be_path");
    diagnostics.check(
        entityType.isTopLevel() || this.referenceCatalogName != null, "reference_catalog_expected");

    if (entityType.isTopLevel()) {
      return this.resolvedEntriesByName.get(new EntityCacheByNameKey(entityType, entityName));
    } else {
      long catalogId = this.resolvedReferenceCatalog.getEntity().getId();
      return this.resolvedEntriesByName.get(
          new EntityCacheByNameKey(catalogId, catalogId, entityType, entityName));
    }
  }
}
