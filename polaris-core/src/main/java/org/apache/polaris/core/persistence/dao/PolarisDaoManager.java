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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
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
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
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

/**
 * This class acts as a temporary bridge to defer refactoring of business logic. It currently
 * implements PolarisMetaStoreManager, but it is no longer be necessary after refactoring.
 * Post-refactor, callers can directly interact with individual DAO objects as needed.
 */
public class PolarisDaoManager implements PolarisMetaStoreManager {
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

  @Nonnull
  @Override
  public BaseResult bootstrapPolarisService(@Nonnull PolarisCallContext callCtx) {
    return commonDao.bootstrapPolarisService(callCtx);
  }

  @Nonnull
  @Override
  public BaseResult purge(@Nonnull PolarisCallContext callCtx) {
    return commonDao.purge(callCtx);
  }

  @Nonnull
  @Override
  public EntityResult readEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name) {
    switch (entityType) {
      case CATALOG:
        return catalogDao.readEntityByName(callCtx, name);
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

  @Nonnull
  @Override
  public ListEntitiesResult listEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType) {
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

  @Nonnull
  @Override
  public GenerateEntityIdResult generateNewEntityId(@Nonnull PolarisCallContext callCtx) {
    return commonDao.generateNewEntityId(callCtx);
  }

  @Nonnull
  @Override
  public CreatePrincipalResult createPrincipal(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity principal) {
    return principalDao.createPrincipal(callCtx, principal);
  }

  @Nonnull
  @Override
  public CreateCatalogResult createCatalog(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity catalog,
      @Nonnull List<PolarisEntityCore> principalRoles) {
    return catalogDao.createCatalog(callCtx, catalog, principalRoles);
  }

  @Nonnull
  @Override
  public EntityResult createEntityIfNotExists(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    switch (entity.getType()) {
      case NAMESPACE:
        return namespaceDao.createEntityIfNotExists(callCtx, catalogPath, entity);
      case TABLE_LIKE:
        return tableLikeDao.createEntityIfNotExists(callCtx, catalogPath, entity);
      case CATALOG_ROLE:
        return catalogRoleDao.createEntityIfNotExists(callCtx, catalogPath, entity);
      case PRINCIPAL_ROLE:
        return principalRoleDao.createEntityIfNotExists(callCtx, entity);
      case TASK:
        return taskDao.createEntityIfNotExists(callCtx, entity);
      default:
        throw new IllegalArgumentException("Unknown entity type: " + entity.getType());
    }
  }

  @Nonnull
  @Override
  public EntitiesResult createEntitiesIfNotExist(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull List<? extends PolarisBaseEntity> entities) {
    // only for tasks
    return taskDao.createTasksIfNotExist(callCtx, entities);
  }

  @Nonnull
  @Override
  public EntityResult updateEntityPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    switch (entity.getType()) {
      case CATALOG:
        return catalogDao.updateEntityPropertiesIfNotChanged(callCtx, entity);
      case NAMESPACE:
        return namespaceDao.updateEntityPropertiesIfNotChanged(callCtx, catalogPath, entity);
      case TABLE_LIKE:
        return tableLikeDao.updateEntityPropertiesIfNotChanged(callCtx, catalogPath, entity);
      case CATALOG_ROLE:
        return catalogRoleDao.updateEntityPropertiesIfNotChanged(callCtx, catalogPath, entity);
      case PRINCIPAL:
        return principalDao.updateEntityPropertiesIfNotChanged(callCtx, entity);
      case PRINCIPAL_ROLE:
        return principalRoleDao.updateEntityPropertiesIfNotChanged(callCtx, entity);
      default:
        throw new IllegalArgumentException("Unknown entity type: " + entity.getType());
    }
  }

  @Nonnull
  @Override
  public EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<EntityWithPath> entities) {
    // only for multi-table transactions
    return tableLikeDao.updateEntitiesPropertiesIfNotChanged(callCtx, entities);
  }

  @Nonnull
  @Override
  public EntityResult renameEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @Nonnull PolarisEntity renamedEntity) {
    // only tableLike is supported
    return tableLikeDao.renameEntity(
        callCtx, catalogPath, entityToRename, newCatalogPath, renamedEntity);
  }

  @Nonnull
  @Override
  public DropEntityResult dropEntityIfExists(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore entityToDrop,
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

  @Nonnull
  @Override
  public EntityResult loadEntity(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      @Nonnull PolarisEntityType entityType) {
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
        return principalDao.loadEntity(callCtx, entityId);
      case PRINCIPAL_ROLE:
        return principalRoleDao.loadEntity(callCtx, entityId);
      case TASK:
        return taskDao.loadEntity(callCtx, entityId);
      case NULL_TYPE:
        return commonDao.loadEntity(callCtx, entityCatalogId, entityId);
      default:
        throw new IllegalArgumentException("Unknown entity type: " + entityType);
    }
  }

  @Nonnull
  @Override
  public EntitiesResult loadTasks(
      @Nonnull PolarisCallContext callCtx, String executorId, int limit) {
    return taskDao.loadTasks(callCtx, executorId, limit);
  }

  @Nonnull
  @Override
  public ChangeTrackingResult loadEntitiesChangeTracking(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<PolarisEntityId> entityIds) {
    // todo we need to figure out how to handle type-specific one later
    return commonDao.loadEntitiesChangeTracking(callCtx, entityIds);
  }

  @Nonnull
  @Override
  public ResolvedEntityResult loadResolvedEntityById(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    switch (entityType) {
      case CATALOG:
        return catalogDao.loadResolvedEntityById(callCtx, entityId);
      case NAMESPACE:
        return namespaceDao.loadResolvedEntityById(callCtx, entityCatalogId, entityId);
      case TABLE_LIKE:
        return tableLikeDao.loadResolvedEntityById(callCtx, entityCatalogId, entityId);
      case PRINCIPAL:
        return principalDao.loadResolvedEntityById(callCtx, entityId);
      case PRINCIPAL_ROLE:
        return principalRoleDao.loadResolvedEntityById(callCtx, entityId);
      case CATALOG_ROLE:
        return catalogRoleDao.loadResolvedEntityById(callCtx, entityCatalogId, entityId);
      default:
        throw new IllegalArgumentException("Unknown entity type: " + entityType);
    }
  }

  @Nonnull
  @Override
  public ResolvedEntityResult loadResolvedEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull String entityName) {
    switch (entityType) {
      case NAMESPACE:
        return namespaceDao.loadResolvedEntityByName(
            callCtx, entityCatalogId, parentId, entityName);
      case TABLE_LIKE:
        return tableLikeDao.loadResolvedEntityByName(
            callCtx, entityCatalogId, parentId, entityName);
      case PRINCIPAL:
        return principalDao.loadResolvedEntityByName(callCtx, parentId, entityName);
      case PRINCIPAL_ROLE:
        return principalRoleDao.loadResolvedEntityByName(callCtx, parentId, entityName);
      case ROOT:
        return commonDao.loadResolvedEntityByName(callCtx, entityCatalogId, parentId, entityName);
      case CATALOG:
        return catalogDao.loadResolvedEntityByName(callCtx, parentId, entityName);
      case CATALOG_ROLE:
        return catalogRoleDao.loadResolvedEntityByName(
            callCtx, entityCatalogId, parentId, entityName);
      default:
        throw new IllegalArgumentException("Unknown entity type: " + entityType);
    }
  }

  @Nonnull
  @Override
  public ResolvedEntityResult refreshResolvedEntity(
      @Nonnull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      @Nonnull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {
    switch (entityType) {
      case CATALOG:
        return catalogDao.refreshResolvedEntity(
            callCtx, entityVersion, entityGrantRecordsVersion, entityId);
      case NAMESPACE:
        return namespaceDao.refreshResolvedEntity(
            callCtx, entityVersion, entityGrantRecordsVersion, entityCatalogId, entityId);
      case TABLE_LIKE:
        return tableLikeDao.refreshResolvedEntity(
            callCtx, entityVersion, entityGrantRecordsVersion, entityCatalogId, entityId);
      case PRINCIPAL:
        return principalDao.refreshResolvedEntity(
            callCtx, entityVersion, entityGrantRecordsVersion, entityId);
      case PRINCIPAL_ROLE:
        return principalRoleDao.refreshResolvedEntity(
            callCtx, entityVersion, entityGrantRecordsVersion, entityId);
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

  @Nonnull
  @Override
  public PrivilegeResult grantUsageOnRoleToGrantee(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    return grantRecordDao.grantUsageOnRoleToGrantee(callCtx, catalog, role, grantee);
  }

  @Nonnull
  @Override
  public PrivilegeResult revokeUsageOnRoleFromGrantee(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    return grantRecordDao.revokeUsageOnRoleFromGrantee(callCtx, catalog, role, grantee);
  }

  @Nonnull
  @Override
  public PrivilegeResult grantPrivilegeOnSecurableToRole(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    return grantRecordDao.grantPrivilegeOnSecurableToRole(
        callCtx, grantee, catalogPath, securable, privilege);
  }

  @Nonnull
  @Override
  public PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    return grantRecordDao.revokePrivilegeOnSecurableFromRole(
        callCtx, grantee, catalogPath, securable, privilege);
  }

  @Nonnull
  @Override
  public LoadGrantsResult loadGrantsOnSecurable(
      @Nonnull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    return grantRecordDao.loadGrantsOnSecurable(callCtx, securableCatalogId, securableId);
  }

  @Nonnull
  @Override
  public LoadGrantsResult loadGrantsToGrantee(
      PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    return grantRecordDao.loadGrantsToGrantee(callCtx, granteeCatalogId, granteeId);
  }

  @Nonnull
  @Override
  public PrincipalSecretsResult loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    return principalSecretsDao.loadPrincipalSecrets(callCtx, clientId);
  }

  @Nonnull
  @Override
  public PrincipalSecretsResult rotatePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {
    return principalSecretsDao.rotatePrincipalSecrets(
        callCtx, clientId, principalId, reset, oldSecretHash);
  }

  @Nonnull
  @Override
  public ScopedCredentialsResult getSubscopedCredsForEntity(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      PolarisEntityType entityType,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations) {
    return credentialVendorDao.getSubscopedCredsForEntity(
        callCtx,
        catalogId,
        entityId,
        entityType,
        allowListOperation,
        allowedReadLocations,
        allowedWriteLocations);
  }

  @Nonnull
  @Override
  public ValidateAccessResult validateAccessToLocations(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      PolarisEntityType entityType,
      @Nonnull Set<PolarisStorageActions> actions,
      @Nonnull Set<String> locations) {
    return credentialVendorDao.validateAccessToLocations(
        callCtx, catalogId, entityId, entityType, actions, locations);
  }
}
