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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.PolarisGrantManager;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.BaseResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * PolarisGrantManger implementation that uses an EntityCache to retrieve entities and is backed by
 * a delegate grant manager for persisting grant changes. This allows consumers to reuse cache
 * entities without necessarily being aware of the {@link EntityCache} or the {@link
 * EntityCacheEntry} specifics.
 */
public class EntityCacheGrantManager implements PolarisGrantManager {
  private final PolarisGrantManager delegateGrantManager;
  private final EntityCache entityCache;

  public static final class EntityCacheGrantManagerFactory implements PolarisGrantManager.Factory {
    private final PolarisGrantManager.Factory delegateGrantManagerFactory;
    private final RealmEntityCacheFactory realmEntityCacheFactory;

    public EntityCacheGrantManagerFactory(
        Factory delegateGrantManagerFactory, RealmEntityCacheFactory realmEntityCacheFactory) {
      this.delegateGrantManagerFactory = delegateGrantManagerFactory;
      this.realmEntityCacheFactory = realmEntityCacheFactory;
    }

    @Override
    public PolarisGrantManager getGrantManagerForRealm(RealmContext realm) {
      return new EntityCacheGrantManager(
          delegateGrantManagerFactory.getGrantManagerForRealm(realm),
          realmEntityCacheFactory.getOrCreateEntityCache(realm));
    }
  }

  public EntityCacheGrantManager(
      PolarisGrantManager delegateGrantManager, EntityCache entityCache) {
    this.delegateGrantManager = delegateGrantManager;
    this.entityCache = entityCache;
  }

  @Override
  public @NotNull PrivilegeResult grantUsageOnRoleToGrantee(
      @NotNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NotNull PolarisEntityCore role,
      @NotNull PolarisEntityCore grantee) {
    try {
      return delegateGrantManager.grantUsageOnRoleToGrantee(callCtx, catalog, role, grantee);
    } finally {
      invalidate(role);
      invalidate(grantee);
    }
  }

  private void invalidate(@NotNull PolarisEntityCore role) {
    EntityCacheEntry roleEntity = entityCache.getEntityById(role.getId());
    if (roleEntity != null) {
      entityCache.removeCacheEntry(roleEntity);
    }
  }

  @Override
  public @NotNull PrivilegeResult revokeUsageOnRoleFromGrantee(
      @NotNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NotNull PolarisEntityCore role,
      @NotNull PolarisEntityCore grantee) {
    try {
      return delegateGrantManager.revokeUsageOnRoleFromGrantee(callCtx, catalog, role, grantee);
    } finally {
      invalidate(role);
      invalidate(grantee);
    }
  }

  @Override
  public @NotNull PrivilegeResult grantPrivilegeOnSecurableToRole(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore securable,
      @NotNull PolarisPrivilege privilege) {
    try {
      return delegateGrantManager.grantPrivilegeOnSecurableToRole(
          callCtx, grantee, catalogPath, securable, privilege);
    } finally {
      invalidate(securable);
      invalidate(grantee);
    }
  }

  @Override
  public @NotNull PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore securable,
      @NotNull PolarisPrivilege privilege) {
    try {
      return delegateGrantManager.revokePrivilegeOnSecurableFromRole(
          callCtx, grantee, catalogPath, securable, privilege);
    } finally {
      invalidate(securable);
      invalidate(grantee);
    }
  }

  @Override
  public @NotNull LoadGrantsResult loadGrantsOnSecurable(
      @NotNull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    EntityCacheLookupResult lookupResult =
        entityCache.getOrLoadEntityById(callCtx, securableCatalogId, securableId);
    if (lookupResult == null || lookupResult.getCacheEntry() == null) {
      return new LoadGrantsResult(BaseResult.ReturnStatus.GRANT_NOT_FOUND, null);
    }
    List<PolarisBaseEntity> granteeList =
        lookupResult.getCacheEntry().getGrantRecordsAsSecurable().stream()
            .map(
                gr ->
                    entityCache.getOrLoadEntityById(
                        callCtx, gr.getGranteeCatalogId(), gr.getGranteeId()))
            .filter(lr -> lr != null && lr.getCacheEntry() != null)
            .map(lr -> lr.getCacheEntry().getEntity())
            .collect(Collectors.toList());
    return new LoadGrantsResult(
        lookupResult.getCacheEntry().getEntity().getGrantRecordsVersion(),
        lookupResult.getCacheEntry().getGrantRecordsAsSecurable(),
        granteeList);
  }

  @Override
  public @NotNull LoadGrantsResult loadGrantsToGrantee(
      PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    EntityCacheLookupResult lookupResult =
        entityCache.getOrLoadEntityById(callCtx, granteeCatalogId, granteeId);
    if (lookupResult == null || lookupResult.getCacheEntry() == null) {
      return new LoadGrantsResult(BaseResult.ReturnStatus.GRANT_NOT_FOUND, null);
    }
    List<PolarisBaseEntity> granteeList =
        lookupResult.getCacheEntry().getGrantRecordsAsGrantee().stream()
            .map(
                gr ->
                    entityCache.getOrLoadEntityById(
                        callCtx, gr.getSecurableCatalogId(), gr.getSecurableId()))
            .takeWhile(
                lr -> {
                  if (lr == null || lr.getCacheEntry() == null) {
                    throw new IllegalStateException("Grantee not found");
                  }
                  return true;
                })
            .map(lr -> lr.getCacheEntry().getEntity())
            .collect(Collectors.toList());
    return new LoadGrantsResult(
        lookupResult.getCacheEntry().getEntity().getGrantRecordsVersion(),
        lookupResult.getCacheEntry().getGrantRecordsAsGrantee(),
        granteeList);
  }
}
