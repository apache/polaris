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

package org.apache.polaris.core.persistence.dao;

import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.ChangeTrackingResult;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.apache.polaris.core.persistence.transactional.FdbCatalogDaoImpl;
import org.apache.polaris.core.persistence.transactional.FdbNamespaceDaoImpl;
import org.apache.polaris.core.persistence.transactional.FdbTableLikeDaoImpl;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class servers as a bridge so that we defer the business logic refactor
 */
public class PolarisMetastoreManagerDao extends BaseMetaStoreManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(PolarisMetastoreManagerDao.class);

    // TODO, using factory or CDI to create following instances, so that the implementation can be injected.
    FdbCatalogDaoImpl catalogDao = new FdbCatalogDaoImpl();
    FdbNamespaceDaoImpl namespaceDao = new FdbNamespaceDaoImpl();
    FdbTableLikeDaoImpl tableLikeDao = new FdbTableLikeDaoImpl();

    @NotNull
    @Override
    public BaseResult bootstrapPolarisService(@NotNull PolarisCallContext callCtx) {
        return null;
    }

    @NotNull
    @Override
    public BaseResult purge(@NotNull PolarisCallContext callCtx) {
        return null;
    }

    @NotNull
    @Override
    public EntityResult readEntityByName(@NotNull PolarisCallContext callCtx, @Nullable List<PolarisEntityCore> catalogPath, @NotNull PolarisEntityType entityType, @NotNull PolarisEntitySubType entitySubType, @NotNull String name) {
        switch (entityType) {
            case CATALOG:
                return catalogDao.readEntityByName(callCtx, catalogPath, name);
            case NAMESPACE:
                return namespaceDao.readEntityByName(callCtx, catalogPath, name);
            case TABLE_LIKE:
                return tableLikeDao.readEntityByName(callCtx, catalogPath, entitySubType, name);
        }
        throw new IllegalArgumentException("Unknown entity type: " + entityType);
    }

    @NotNull
    @Override
    public ListEntitiesResult listEntities(@NotNull PolarisCallContext callCtx, @Nullable List<PolarisEntityCore> catalogPath, @NotNull PolarisEntityType entityType, @NotNull PolarisEntitySubType entitySubType) {
        switch (entityType) {
            case CATALOG:
                return catalogDao.listEntities(callCtx);
            case NAMESPACE:
                return namespaceDao.listEntities(callCtx, catalogPath);
            case TABLE_LIKE:
                return tableLikeDao.listEntities(callCtx, catalogPath, entitySubType);
        }
        throw new IllegalArgumentException("Unknown entity type: " + entityType);
    }

    @NotNull
    @Override
    public GenerateEntityIdResult generateNewEntityId(@NotNull PolarisCallContext callCtx) {
        return null;
    }

    @NotNull
    @Override
    public CreatePrincipalResult createPrincipal(@NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity principal) {
        return null;
    }

    @NotNull
    @Override
    public CreateCatalogResult createCatalog(@NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity catalog, @NotNull List<PolarisEntityCore> principalRoles) {
        return catalogDao.createCatalog(callCtx, catalog, principalRoles);
    }

    @NotNull
    @Override
    public EntityResult createEntityIfNotExists(@NotNull PolarisCallContext callCtx, @Nullable List<PolarisEntityCore> catalogPath, @NotNull PolarisBaseEntity entity) {
        return null;
    }

    @NotNull
    @Override
    public EntitiesResult createEntitiesIfNotExist(@NotNull PolarisCallContext callCtx, @Nullable List<PolarisEntityCore> catalogPath, @NotNull List<? extends PolarisBaseEntity> entities) {
        return null;
    }

    @NotNull
    @Override
    public EntityResult updateEntityPropertiesIfNotChanged(@NotNull PolarisCallContext callCtx, @Nullable List<PolarisEntityCore> catalogPath, @NotNull PolarisBaseEntity entity) {
        return null;
    }

    @NotNull
    @Override
    public EntitiesResult updateEntitiesPropertiesIfNotChanged(@NotNull PolarisCallContext callCtx, @NotNull List<EntityWithPath> entities) {
        return null;
    }

    @NotNull
    @Override
    public EntityResult renameEntity(@NotNull PolarisCallContext callCtx, @Nullable List<PolarisEntityCore> catalogPath, @NotNull PolarisEntityCore entityToRename, @Nullable List<PolarisEntityCore> newCatalogPath, @NotNull PolarisEntity renamedEntity) {
        return null;
    }

    @NotNull
    @Override
    public DropEntityResult dropEntityIfExists(@NotNull PolarisCallContext callCtx, @Nullable List<PolarisEntityCore> catalogPath, @NotNull PolarisEntityCore entityToDrop, @Nullable Map<String, String> cleanupProperties, boolean cleanup) {
        return null;
    }

    @NotNull
    @Override
    public EntityResult loadEntity(@NotNull PolarisCallContext callCtx, long entityCatalogId, long entityId, @NotNull PolarisEntityType entityType) {
        return null;
    }

    @NotNull
    @Override
    public EntitiesResult loadTasks(@NotNull PolarisCallContext callCtx, String executorId, int limit) {
        return null;
    }

    @NotNull
    @Override
    public ChangeTrackingResult loadEntitiesChangeTracking(@NotNull PolarisCallContext callCtx, @NotNull List<PolarisEntityId> entityIds) {
        return null;
    }

    @NotNull
    @Override
    public ResolvedEntityResult loadResolvedEntityById(@NotNull PolarisCallContext callCtx, long entityCatalogId, long entityId, PolarisEntityType entityType) {
        return null;
    }

    @NotNull
    @Override
    public ResolvedEntityResult loadResolvedEntityByName(@NotNull PolarisCallContext callCtx, long entityCatalogId, long parentId, @NotNull PolarisEntityType entityType, @NotNull String entityName) {
        return null;
    }

    @NotNull
    @Override
    public ResolvedEntityResult refreshResolvedEntity(@NotNull PolarisCallContext callCtx, int entityVersion, int entityGrantRecordsVersion, @NotNull PolarisEntityType entityType, long entityCatalogId, long entityId) {
        return null;
    }

    @NotNull
    @Override
    public PrivilegeResult grantUsageOnRoleToGrantee(@NotNull PolarisCallContext callCtx, @Nullable PolarisEntityCore catalog, @NotNull PolarisEntityCore role, @NotNull PolarisEntityCore grantee) {
        return null;
    }

    @NotNull
    @Override
    public PrivilegeResult revokeUsageOnRoleFromGrantee(@NotNull PolarisCallContext callCtx, @Nullable PolarisEntityCore catalog, @NotNull PolarisEntityCore role, @NotNull PolarisEntityCore grantee) {
        return null;
    }

    @NotNull
    @Override
    public PrivilegeResult grantPrivilegeOnSecurableToRole(@NotNull PolarisCallContext callCtx, @NotNull PolarisEntityCore grantee, @Nullable List<PolarisEntityCore> catalogPath, @NotNull PolarisEntityCore securable, @NotNull PolarisPrivilege privilege) {
        return null;
    }

    @NotNull
    @Override
    public PrivilegeResult revokePrivilegeOnSecurableFromRole(@NotNull PolarisCallContext callCtx, @NotNull PolarisEntityCore grantee, @Nullable List<PolarisEntityCore> catalogPath, @NotNull PolarisEntityCore securable, @NotNull PolarisPrivilege privilege) {
        return null;
    }

    @NotNull
    @Override
    public LoadGrantsResult loadGrantsOnSecurable(@NotNull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
        return null;
    }

    @NotNull
    @Override
    public LoadGrantsResult loadGrantsToGrantee(PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
        return null;
    }

    @NotNull
    @Override
    public PrincipalSecretsResult loadPrincipalSecrets(@NotNull PolarisCallContext callCtx, @NotNull String clientId) {
        return null;
    }

    @NotNull
    @Override
    public PrincipalSecretsResult rotatePrincipalSecrets(@NotNull PolarisCallContext callCtx, @NotNull String clientId, long principalId, boolean reset, @NotNull String oldSecretHash) {
        return null;
    }

    @NotNull
    @Override
    public ScopedCredentialsResult getSubscopedCredsForEntity(@NotNull PolarisCallContext callCtx, long catalogId, long entityId, PolarisEntityType entityType, boolean allowListOperation, @NotNull Set<String> allowedReadLocations, @NotNull Set<String> allowedWriteLocations) {
        return null;
    }

    @NotNull
    @Override
    public ValidateAccessResult validateAccessToLocations(@NotNull PolarisCallContext callCtx, long catalogId, long entityId, PolarisEntityType entityType, @NotNull Set<PolarisStorageActions> actions, @NotNull Set<String> locations) {
        return null;
    }
}
