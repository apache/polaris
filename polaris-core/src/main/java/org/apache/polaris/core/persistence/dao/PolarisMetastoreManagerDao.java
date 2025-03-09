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

import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.polaris.core.persistence.transactional.FdbCatalogRoleDaoImpl;
import org.apache.polaris.core.persistence.transactional.FdbCommonDaoImpl;
import org.apache.polaris.core.persistence.transactional.FdbCredentialVendorDaoImpl;
import org.apache.polaris.core.persistence.transactional.FdbGrantRecordDaoImpl;
import org.apache.polaris.core.persistence.transactional.FdbNamespaceDaoImpl;
import org.apache.polaris.core.persistence.transactional.FdbPrincipalDaoImpl;
import org.apache.polaris.core.persistence.transactional.FdbPrincipalRoleDaoImpl;
import org.apache.polaris.core.persistence.transactional.FdbPrincipalSecretsDaoImpl;
import org.apache.polaris.core.persistence.transactional.FdbTableLikeDaoImpl;
import org.apache.polaris.core.persistence.transactional.FdbTaskDaoImpl;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class servers as a bridge so that we defer the business logic refactor */
public class PolarisMetastoreManagerDao extends BaseMetaStoreManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisMetastoreManagerDao.class);

  // TODO, using factory or CDI to create following instances, so that the implementation can be
  // injected.
  FdbCatalogDaoImpl catalogDao = new FdbCatalogDaoImpl();
  FdbNamespaceDaoImpl namespaceDao = new FdbNamespaceDaoImpl();
  FdbTableLikeDaoImpl tableLikeDao = new FdbTableLikeDaoImpl();
  FdbCatalogRoleDaoImpl catalogRoleDao = new FdbCatalogRoleDaoImpl();
  FdbPrincipalRoleDaoImpl principalRoleDao = new FdbPrincipalRoleDaoImpl();
  FdbPrincipalDaoImpl principalDao = new FdbPrincipalDaoImpl();
  FdbTaskDaoImpl taskDao = new FdbTaskDaoImpl();
  FdbPrincipalSecretsDaoImpl principalSecretsDao = new FdbPrincipalSecretsDaoImpl();
  FdbGrantRecordDaoImpl grantRecordDao = new FdbGrantRecordDaoImpl();
  FdbCommonDaoImpl commonDao = new FdbCommonDaoImpl();
  FdbCredentialVendorDaoImpl credentialVendorDao = new FdbCredentialVendorDaoImpl();

  @NotNull
  @Override
  public BaseResult bootstrapPolarisService(@NotNull PolarisCallContext callCtx) {
    return commonDao.bootstrapPolarisService(callCtx);
  }

  @NotNull
  @Override
  public BaseResult purge(@NotNull PolarisCallContext callCtx) {
    return commonDao.purge(callCtx);
  }

  @NotNull
  @Override
  public EntityResult readEntityByName(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityType entityType,
      @NotNull PolarisEntitySubType entitySubType,
      @NotNull String name) {
    switch (entityType) {
      case CATALOG:
        return catalogDao.readEntityByName(callCtx, catalogPath, name);
      case NAMESPACE:
        return namespaceDao.readEntityByName(callCtx, catalogPath, name);
      case TABLE_LIKE:
        return tableLikeDao.readEntityByName(callCtx, catalogPath, entitySubType, name);
      case CATALOG_ROLE:
        return catalogRoleDao.readEntityByName(callCtx, catalogPath, name);
      case PRINCIPAL:
        return principalDao.readEntityByName(callCtx, name);
      case PRINCIPAL_ROLE:
        return principalRoleDao.readEntityByName(callCtx, name);
      case TASK:
        return taskDao.readEntityByName(callCtx, name);
      default:
        throw new IllegalArgumentException("Unknown entity type: " + entityType);
    }
  }

  @NotNull
  @Override
  public ListEntitiesResult listEntities(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityType entityType,
      @NotNull PolarisEntitySubType entitySubType) {
    switch (entityType) {
      case CATALOG:
        return catalogDao.listEntities(callCtx);
      case NAMESPACE:
        return namespaceDao.listEntities(callCtx, catalogPath);
      case TABLE_LIKE:
        return tableLikeDao.listEntities(callCtx, catalogPath, entitySubType);
      case CATALOG_ROLE:
        return catalogRoleDao.listEntities(callCtx, catalogPath);
      case PRINCIPAL:
        return principalDao.listEntities(callCtx);
      case PRINCIPAL_ROLE:
        return principalRoleDao.listEntities(callCtx);
      case TASK:
        return taskDao.listEntities(callCtx);
      default:
        throw new IllegalArgumentException("Unknown entity type: " + entityType);
    }
  }

  @NotNull
  @Override
  public GenerateEntityIdResult generateNewEntityId(@NotNull PolarisCallContext callCtx) {
    return commonDao.generateNewEntityId(callCtx);
  }

  @NotNull
  @Override
  public CreatePrincipalResult createPrincipal(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity principal) {
    return principalDao.createPrincipal(callCtx, principal);
  }

  @NotNull
  @Override
  public CreateCatalogResult createCatalog(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisBaseEntity catalog,
      @NotNull List<PolarisEntityCore> principalRoles) {
    return catalogDao.createCatalog(callCtx, catalog, principalRoles);
  }

  @NotNull
  @Override
  public EntityResult createEntityIfNotExists(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisBaseEntity entity) {
    switch (entity.getType()) {
      case NAMESPACE:
        return namespaceDao.createEntityIfNotExists(callCtx, catalogPath, entity);
      case TABLE_LIKE:
        return tableLikeDao.createEntityIfNotExists(callCtx, catalogPath, entity);
      case CATALOG_ROLE:
        return catalogRoleDao.createEntityIfNotExists(callCtx, catalogPath, entity);
      case PRINCIPAL_ROLE:
        return principalRoleDao.createEntityIfNotExists(callCtx, catalogPath, entity);
      case TASK:
        return taskDao.createEntityIfNotExists(callCtx, catalogPath, entity);
      default:
        throw new IllegalArgumentException("Unknown entity type: " + entity.getType());
    }
  }

  @NotNull
  @Override
  public EntitiesResult createEntitiesIfNotExist(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull List<? extends PolarisBaseEntity> entities) {
    // only for tasks
    return taskDao.createEntitiesIfNotExist(callCtx, catalogPath, entities);
  }

  @NotNull
  @Override
  public EntityResult updateEntityPropertiesIfNotChanged(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisBaseEntity entity) {
    switch (entity.getType()) {
      case CATALOG:
        return catalogDao.updateEntityPropertiesIfNotChanged(callCtx, catalogPath, entity);
      case NAMESPACE:
        return namespaceDao.updateEntityPropertiesIfNotChanged(callCtx, catalogPath, entity);
      case TABLE_LIKE:
        return tableLikeDao.updateEntityPropertiesIfNotChanged(callCtx, catalogPath, entity);
      case CATALOG_ROLE:
        return catalogRoleDao.updateEntityPropertiesIfNotChanged(callCtx, catalogPath, entity);
      case PRINCIPAL:
        return principalDao.updateEntityPropertiesIfNotChanged(callCtx, catalogPath, entity);
      case PRINCIPAL_ROLE:
        return principalRoleDao.updateEntityPropertiesIfNotChanged(callCtx, catalogPath, entity);
      default:
        throw new IllegalArgumentException("Unknown entity type: " + entity.getType());
    }
  }

  @NotNull
  @Override
  public EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @NotNull PolarisCallContext callCtx, @NotNull List<EntityWithPath> entities) {
    // only for multi-table transactions
    return tableLikeDao.updateEntitiesPropertiesIfNotChanged(callCtx, entities);
  }

  @NotNull
  @Override
  public EntityResult renameEntity(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @NotNull PolarisEntity renamedEntity) {
    // only tableLike is supported
    return tableLikeDao.renameEntity(
        callCtx, catalogPath, entityToRename, newCatalogPath, renamedEntity);
  }

  @NotNull
  @Override
  public DropEntityResult dropEntityIfExists(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    switch (entityToDrop.getType()) {
      case CATALOG:
        return catalogDao.dropEntityIfExists(callCtx, entityToDrop, cleanupProperties, cleanup);
      case NAMESPACE:
        return namespaceDao.dropEntityIfExists(
            callCtx, catalogPath, entityToDrop, cleanupProperties, cleanup);
      case TABLE_LIKE:
        return tableLikeDao.dropEntityIfExists(
            callCtx, catalogPath, entityToDrop, cleanupProperties, cleanup);
      case CATALOG_ROLE:
        return catalogRoleDao.dropEntityIfExists(
            callCtx, catalogPath, entityToDrop, cleanupProperties, cleanup);
      case PRINCIPAL:
        return principalDao.dropEntityIfExists(callCtx, entityToDrop, cleanupProperties, cleanup);
      case PRINCIPAL_ROLE:
        return principalRoleDao.dropEntityIfExists(
            callCtx, entityToDrop, cleanupProperties, cleanup);
      case TASK:
        return taskDao.dropEntityIfExists(callCtx, entityToDrop, cleanupProperties, cleanup);
      default:
        throw new IllegalArgumentException("Unknown entity type: " + entityToDrop.getType());
    }
  }

  @NotNull
  @Override
  public EntityResult loadEntity(
      @NotNull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      @NotNull PolarisEntityType entityType) {
    switch (entityType) {
      case CATALOG:
        return catalogDao.loadEntity(callCtx, entityCatalogId, entityId);
      case NAMESPACE:
        return namespaceDao.loadEntity(callCtx, entityCatalogId, entityId);
      case TABLE_LIKE:
        return tableLikeDao.loadEntity(callCtx, entityCatalogId, entityId);
      case CATALOG_ROLE:
        return catalogRoleDao.loadEntity(callCtx, entityCatalogId, entityId);
      case PRINCIPAL:
        return principalDao.loadEntity(callCtx, entityCatalogId, entityId);
      case PRINCIPAL_ROLE:
        return principalRoleDao.loadEntity(callCtx, entityCatalogId, entityId);
      case TASK:
        return taskDao.loadEntity(callCtx, entityCatalogId, entityId);
      case NULL_TYPE:
        return commonDao.loadEntity(callCtx, entityCatalogId, entityId);
      default:
        throw new IllegalArgumentException("Unknown entity type: " + entityType);
    }
  }

  @NotNull
  @Override
  public EntitiesResult loadTasks(
      @NotNull PolarisCallContext callCtx, String executorId, int limit) {
    return taskDao.loadTasks(callCtx, executorId, limit);
  }

  @NotNull
  @Override
  public ChangeTrackingResult loadEntitiesChangeTracking(
      @NotNull PolarisCallContext callCtx, @NotNull List<PolarisEntityId> entityIds) {
    // todo we need to figure out how to handle type-specific one later
    return commonDao.loadEntitiesChangeTracking(callCtx, entityIds);
  }

  @NotNull
  @Override
  public ResolvedEntityResult loadResolvedEntityById(
      @NotNull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    switch (entityType) {
      case CATALOG:
        return catalogDao.loadResolvedEntityById(callCtx, entityCatalogId, entityId);
      case NAMESPACE:
        return namespaceDao.loadResolvedEntityById(callCtx, entityCatalogId, entityId);
      case TABLE_LIKE:
        return tableLikeDao.loadResolvedEntityById(callCtx, entityCatalogId, entityId);
      case PRINCIPAL:
        return principalDao.loadResolvedEntityById(callCtx, entityCatalogId, entityId);
      case PRINCIPAL_ROLE:
        return principalRoleDao.loadResolvedEntityById(callCtx, entityCatalogId, entityId);
      case CATALOG_ROLE:
        return catalogRoleDao.loadResolvedEntityById(callCtx, entityCatalogId, entityId);
      default:
        throw new IllegalArgumentException("Unknown entity type: " + entityType);
    }
  }

  @NotNull
  @Override
  public ResolvedEntityResult loadResolvedEntityByName(
      @NotNull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @NotNull PolarisEntityType entityType,
      @NotNull String entityName) {
    switch (entityType) {
      case NAMESPACE:
        return namespaceDao.loadResolvedEntityByName(
            callCtx, entityCatalogId, parentId, entityName);
      case TABLE_LIKE:
        return tableLikeDao.loadResolvedEntityByName(
            callCtx, entityCatalogId, parentId, entityName);
      case PRINCIPAL:
        return principalDao.loadResolvedEntityByName(
            callCtx, entityCatalogId, parentId, entityName);
      case PRINCIPAL_ROLE:
        return principalRoleDao.loadResolvedEntityByName(
            callCtx, entityCatalogId, parentId, entityName);
      case ROOT:
        return commonDao.loadResolvedEntityByName(callCtx, entityCatalogId, parentId, entityName);
      case CATALOG:
        return catalogDao.loadResolvedEntityByName(callCtx, entityCatalogId, parentId, entityName);
      case CATALOG_ROLE:
        return catalogRoleDao.loadResolvedEntityByName(
            callCtx, entityCatalogId, parentId, entityName);
      default:
        throw new IllegalArgumentException("Unknown entity type: " + entityType);
    }
  }

  @NotNull
  @Override
  public ResolvedEntityResult refreshResolvedEntity(
      @NotNull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      @NotNull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {
    switch (entityType) {
      case CATALOG:
        return catalogDao.refreshResolvedEntity(
            callCtx, entityVersion, entityGrantRecordsVersion, entityCatalogId, entityId);
      case NAMESPACE:
        return namespaceDao.refreshResolvedEntity(
            callCtx, entityVersion, entityGrantRecordsVersion, entityCatalogId, entityId);
      case TABLE_LIKE:
        return tableLikeDao.refreshResolvedEntity(
            callCtx, entityVersion, entityGrantRecordsVersion, entityCatalogId, entityId);
      case PRINCIPAL:
        return principalDao.refreshResolvedEntity(
            callCtx, entityVersion, entityGrantRecordsVersion, entityCatalogId, entityId);
      case PRINCIPAL_ROLE:
        return principalRoleDao.refreshResolvedEntity(
            callCtx, entityVersion, entityGrantRecordsVersion, entityCatalogId, entityId);
      case CATALOG_ROLE:
        return catalogRoleDao.refreshResolvedEntity(
            callCtx, entityVersion, entityGrantRecordsVersion, entityCatalogId, entityId);
      case ROOT:
        return commonDao.refreshResolvedEntity(
            callCtx, entityVersion, entityGrantRecordsVersion, entityCatalogId, entityId);
      default:
        throw new IllegalArgumentException("Unknown entity type: " + entityType);
    }
  }

  @NotNull
  @Override
  public PrivilegeResult grantUsageOnRoleToGrantee(
      @NotNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NotNull PolarisEntityCore role,
      @NotNull PolarisEntityCore grantee) {
    return grantRecordDao.grantUsageOnRoleToGrantee(callCtx, catalog, role, grantee);
  }

  @NotNull
  @Override
  public PrivilegeResult revokeUsageOnRoleFromGrantee(
      @NotNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NotNull PolarisEntityCore role,
      @NotNull PolarisEntityCore grantee) {
    return grantRecordDao.revokeUsageOnRoleFromGrantee(callCtx, catalog, role, grantee);
  }

  @NotNull
  @Override
  public PrivilegeResult grantPrivilegeOnSecurableToRole(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore securable,
      @NotNull PolarisPrivilege privilege) {
    return grantRecordDao.grantPrivilegeOnSecurableToRole(
        callCtx, grantee, catalogPath, securable, privilege);
  }

  @NotNull
  @Override
  public PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore securable,
      @NotNull PolarisPrivilege privilege) {
    return grantRecordDao.revokePrivilegeOnSecurableFromRole(
        callCtx, grantee, catalogPath, securable, privilege);
  }

  @NotNull
  @Override
  public LoadGrantsResult loadGrantsOnSecurable(
      @NotNull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    return grantRecordDao.loadGrantsOnSecurable(callCtx, securableCatalogId, securableId);
  }

  @NotNull
  @Override
  public LoadGrantsResult loadGrantsToGrantee(
      PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    return grantRecordDao.loadGrantsToGrantee(callCtx, granteeCatalogId, granteeId);
  }

  @NotNull
  @Override
  public PrincipalSecretsResult loadPrincipalSecrets(
      @NotNull PolarisCallContext callCtx, @NotNull String clientId) {
    return principalSecretsDao.loadPrincipalSecrets(callCtx, clientId);
  }

  @NotNull
  @Override
  public PrincipalSecretsResult rotatePrincipalSecrets(
      @NotNull PolarisCallContext callCtx,
      @NotNull String clientId,
      long principalId,
      boolean reset,
      @NotNull String oldSecretHash) {
    return principalSecretsDao.rotatePrincipalSecrets(
        callCtx, clientId, principalId, reset, oldSecretHash);
  }

  @NotNull
  @Override
  public ScopedCredentialsResult getSubscopedCredsForEntity(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      PolarisEntityType entityType,
      boolean allowListOperation,
      @NotNull Set<String> allowedReadLocations,
      @NotNull Set<String> allowedWriteLocations) {
    return credentialVendorDao.getSubscopedCredsForEntity(
        callCtx,
        catalogId,
        entityId,
        entityType,
        allowListOperation,
        allowedReadLocations,
        allowedWriteLocations);
  }

  @NotNull
  @Override
  public ValidateAccessResult validateAccessToLocations(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      PolarisEntityType entityType,
      @NotNull Set<PolarisStorageActions> actions,
      @NotNull Set<String> locations) {
    return credentialVendorDao.validateAccessToLocations(
        callCtx, catalogId, entityId, entityType, actions, locations);
  }
}
