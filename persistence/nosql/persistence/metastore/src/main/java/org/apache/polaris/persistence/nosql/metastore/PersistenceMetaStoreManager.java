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
package org.apache.polaris.persistence.nosql.metastore;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisObjectMapperUtil;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
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
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.apache.polaris.core.persistence.dao.entity.LoadPolicyMappingsResult;
import org.apache.polaris.core.persistence.dao.entity.PolicyAttachmentResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.apache.polaris.core.persistence.dao.entity.ScopedCredentialsResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.persistence.nosql.api.Persistence;

class PersistenceMetaStoreManager implements PolarisMetaStoreManager {
  private final Persistence persistence;
  private final Supplier<BaseResult> purgeRealm;
  private final RootCredentialsSet rootCredentialsSet;
  private final PersistenceMetaStoreManagerFactory factory;

  PersistenceMetaStoreManager(
      Persistence persistence,
      Supplier<BaseResult> purgeRealm,
      RootCredentialsSet rootCredentialsSet,
      PersistenceMetaStoreManagerFactory factory) {
    this.persistence = persistence;
    this.purgeRealm = purgeRealm;
    this.rootCredentialsSet = rootCredentialsSet;
    this.factory = factory;
  }

  PersistenceMetaStore ms(PolarisCallContext callCtx) {
    var ms =
        callCtx != null ? callCtx.getMetaStore() : factory.newPersistenceMetaStore(persistence);
    checkState(ms instanceof PersistenceMetaStore, "Not a persistence metastore");
    return (PersistenceMetaStore) ms;
  }

  // Realms

  @Nonnull
  @Override
  public BaseResult bootstrapPolarisService(@Nonnull PolarisCallContext callCtx) {
    bootstrapPolarisServiceInternal(callCtx);
    return new BaseResult(BaseResult.ReturnStatus.SUCCESS);
  }

  Optional<CreatePrincipalResult> bootstrapPolarisServiceInternal(
      @Nonnull PolarisCallContext callCtx) {
    // This function is idempotent, already existing entities will not be created again.

    var ms = ms(callCtx);

    // Create the root-container, if not already present
    var rootContainer =
        ms.lookupRoot()
            .orElseGet(
                () -> {
                  var newRoot =
                      new PolarisBaseEntity(
                          PolarisEntityConstants.getNullId(),
                          PolarisEntityConstants.getRootEntityId(),
                          PolarisEntityType.ROOT,
                          PolarisEntitySubType.NULL_SUBTYPE,
                          PolarisEntityConstants.getRootEntityId(),
                          PolarisEntityConstants.getRootContainerName());
                  ms.createEntity(newRoot);
                  return newRoot;
                });

    // Create the root-principal, if not already present
    var rootPrincipal =
        ms.lookupEntityByName(
            callCtx,
            0L,
            0L,
            PolarisEntityType.PRINCIPAL.getCode(),
            PolarisEntityConstants.getRootPrincipalName());
    var createPrincipalResult = Optional.<CreatePrincipalResult>empty();
    if (rootPrincipal == null) {
      var rootPrincipalId = ms.generateNewId(callCtx);
      rootPrincipal =
          new PolarisBaseEntity(
              PolarisEntityConstants.getNullId(),
              rootPrincipalId,
              PolarisEntityType.PRINCIPAL,
              PolarisEntitySubType.NULL_SUBTYPE,
              PolarisEntityConstants.getRootEntityId(),
              PolarisEntityConstants.getRootPrincipalName());

      createPrincipalResult = Optional.of(ms.createPrincipal(rootPrincipal, rootCredentialsSet));
    }

    // Create the service-admin principal-role, if not already present
    var serviceAdminPrincipalRole =
        ms.lookupEntityByName(
            callCtx,
            0L,
            0L,
            PolarisEntityType.PRINCIPAL_ROLE.getCode(),
            PolarisEntityConstants.getNameOfPrincipalServiceAdminRole());
    if (serviceAdminPrincipalRole == null) {
      // now create the account admin principal role
      var serviceAdminPrincipalRoleId = ms.generateNewId(callCtx);
      serviceAdminPrincipalRole =
          new PolarisBaseEntity(
              PolarisEntityConstants.getNullId(),
              serviceAdminPrincipalRoleId,
              PolarisEntityType.PRINCIPAL_ROLE,
              PolarisEntitySubType.NULL_SUBTYPE,
              PolarisEntityConstants.getRootEntityId(),
              PolarisEntityConstants.getNameOfPrincipalServiceAdminRole());
      ms.createEntity(serviceAdminPrincipalRole);
    }

    // Persisting already existing grants is an idempotent operation
    ms.persistGrantsOrRevokes(
        0L,
        true,
        // we also need to grant usage on the account-admin principal to the principal
        new Grant(serviceAdminPrincipalRole, rootPrincipal, PolarisPrivilege.PRINCIPAL_ROLE_USAGE),
        // grant SERVICE_MANAGE_ACCESS on the rootContainer to the serviceAdminPrincipalRole
        new Grant(
            rootContainer, serviceAdminPrincipalRole, PolarisPrivilege.SERVICE_MANAGE_ACCESS));

    return createPrincipalResult;
  }

  @Nonnull
  @Override
  public BaseResult purge(@Nonnull PolarisCallContext callCtx) {
    return purgeRealm.get();
  }

  // Catalog

  @Nonnull
  @Override
  public CreateCatalogResult createCatalog(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity catalog,
      @Nonnull List<PolarisEntityCore> principalRoles) {
    var ms = ms(callCtx);

    var internalProp = catalog.getInternalPropertiesAsMap();
    var integrationIdentifierOrId =
        internalProp.get(PolarisEntityConstants.getStorageIntegrationIdentifierPropertyName());
    var storageConfigInfoStr =
        internalProp.get(PolarisEntityConstants.getStorageConfigInfoPropertyName());

    var integration =
        // storageConfigInfo's presence is needed to create a storage integration
        // and the catalog should not have an internal property of storage identifier or id yet
        (storageConfigInfoStr != null && integrationIdentifierOrId == null)
            ? ms.createStorageIntegration(
                callCtx,
                catalog.getCatalogId(),
                catalog.getId(),
                PolarisStorageConfigurationInfo.deserialize(
                    callCtx.getDiagServices(), storageConfigInfoStr))
            : null;

    var prRoles = principalRoles.stream().map(PolarisBaseEntity.class::cast).toList();

    return ms.createCatalog(callCtx, catalog, prRoles, integration);
  }

  // Generic entities

  @Nonnull
  @Override
  public EntityResult createEntityIfNotExists(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    var ms = ms(callCtx);
    return ms.createEntity(entity);
  }

  @SuppressWarnings("unchecked")
  @Nonnull
  @Override
  public EntitiesResult createEntitiesIfNotExist(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull List<? extends PolarisBaseEntity> entities) {
    var ms = ms(callCtx);
    return ms.createEntities((List<PolarisBaseEntity>) entities);
  }

  @Nonnull
  @Override
  public EntityResult updateEntityPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    var ms = ms(callCtx);
    return ms.updateEntity(entity);
  }

  @Nonnull
  @Override
  public EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<EntityWithPath> entities) {
    var ms = ms(callCtx);
    return ms.updateEntities(entities);
  }

  @Nonnull
  @Override
  public EntityResult renameEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @Nonnull PolarisEntity renamedEntity) {
    var ms = ms(callCtx);
    if (newCatalogPath != null && !newCatalogPath.isEmpty()) {
      var last = newCatalogPath.getLast();
      // At least BasePolarisMetaStoreManagerTest comes with the wrong parentId in renamedEntity
      if (renamedEntity.getParentId() != last.getId()) {
        renamedEntity = new PolarisEntity.Builder(renamedEntity).setParentId(last.getId()).build();
      }
    }
    return ms.updateEntity(renamedEntity);
  }

  @Nonnull
  @Override
  public DropEntityResult dropEntityIfExists(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    return ms(callCtx).dropEntity(callCtx, entityToDrop, cleanupProperties, cleanup);
  }

  @Nonnull
  @Override
  public EntityResult readEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name) {
    var ms = ms(callCtx);
    var catalogId = 0L;
    var parentId = 0L;
    if (catalogPath != null && !catalogPath.isEmpty()) {
      catalogId = catalogPath.getFirst().getId();
      parentId = catalogPath.getLast().getId();
    }
    var entity = ms.lookupEntityByName(callCtx, catalogId, parentId, entityType.getCode(), name);
    return entity != null
        ? new EntityResult(entity)
        : new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
  }

  @Nonnull
  @Override
  public ListEntitiesResult listEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull PageToken pageToken) {
    var ms = ms(callCtx);
    return ms.listEntities(catalogPath, entityType, entitySubType, pageToken);
  }

  @Nonnull
  @Override
  public GenerateEntityIdResult generateNewEntityId(@Nonnull PolarisCallContext callCtx) {
    return new GenerateEntityIdResult(persistence.generateId());
  }

  @Nonnull
  @Override
  public ResolvedEntityResult loadResolvedEntityById(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    var ms = ms(callCtx);

    // load that entity
    PolarisBaseEntity entity =
        ms.lookupEntity(callCtx, entityCatalogId, entityId, entityType.getCode());

    // if entity not found, return null
    if (entity == null) {
      return new ResolvedEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // load the grant records
    var grantRecords = ms.allGrantRecords(entity);

    // return the result
    return new ResolvedEntityResult(entity, entity.getGrantRecordsVersion(), grantRecords);
  }

  @Nonnull
  @Override
  public ResolvedEntityResult loadResolvedEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull String entityName) {
    var ms = ms(callCtx);

    // load that entity
    var entity =
        ms.lookupEntityByName(callCtx, entityCatalogId, parentId, entityType.getCode(), entityName);

    // null if entity not found
    if (entity == null) {
      return new ResolvedEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // load the grant records
    var grantRecords = ms.allGrantRecords(entity);

    // return the result
    return new ResolvedEntityResult(entity, entity.getGrantRecordsVersion(), grantRecords);
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
    return loadResolvedEntityById(callCtx, entityCatalogId, entityId, entityType);
  }

  // Principals & Polaris GrantManager

  @Nonnull
  @Override
  public PrivilegeResult grantUsageOnRoleToGrantee(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    var privilege =
        (grantee.getType() == PolarisEntityType.PRINCIPAL_ROLE)
            ? PolarisPrivilege.CATALOG_ROLE_USAGE
            : PolarisPrivilege.PRINCIPAL_ROLE_USAGE;

    return grantPrivilegeOnSecurableToRole(callCtx, grantee, null, role, privilege);
  }

  @Nonnull
  @Override
  public PrivilegeResult revokeUsageOnRoleFromGrantee(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    var privilege =
        (grantee.getType() == PolarisEntityType.PRINCIPAL_ROLE)
            ? PolarisPrivilege.CATALOG_ROLE_USAGE
            : PolarisPrivilege.PRINCIPAL_ROLE_USAGE;

    return revokePrivilegeOnSecurableFromRole(callCtx, grantee, null, role, privilege);
  }

  @Nonnull
  @Override
  public PrivilegeResult grantPrivilegeOnSecurableToRole(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    return grantOrRevokePrivilegeOnSecurableToRole(
        callCtx, true, grantee, catalogPath, securable, privilege);
  }

  @Nonnull
  @Override
  public PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    return grantOrRevokePrivilegeOnSecurableToRole(
        callCtx, false, grantee, catalogPath, securable, privilege);
  }

  private PrivilegeResult grantOrRevokePrivilegeOnSecurableToRole(
      PolarisCallContext callCtx,
      boolean grant,
      PolarisEntityCore grantee,
      List<PolarisEntityCore> catalogPath,
      PolarisEntityCore securable,
      PolarisPrivilege privilege) {
    var ms = ms(callCtx);
    var catalogId =
        catalogPath != null && !catalogPath.isEmpty() ? catalogPath.getFirst().getId() : 0L;
    ms.persistGrantsOrRevokes(catalogId, grant, new Grant(securable, grantee, privilege));

    var grantRecord =
        new PolarisGrantRecord(
            securable.getCatalogId(),
            securable.getId(),
            grantee.getCatalogId(),
            grantee.getId(),
            privilege.getCode());
    return new PrivilegeResult(grantRecord);
  }

  @Nonnull
  @Override
  public LoadGrantsResult loadGrantsOnSecurable(
      @Nonnull PolarisCallContext callCtx, PolarisEntityCore securable) {
    var ms = ms(callCtx);
    return ms.loadGrants(
        securable.getCatalogId(), securable.getId(), securable.getTypeCode(), true);
  }

  @Nonnull
  @Override
  public LoadGrantsResult loadGrantsToGrantee(
      @Nonnull PolarisCallContext callCtx, PolarisEntityCore grantee) {
    var ms = ms(callCtx);
    return ms.loadGrants(grantee.getCatalogId(), grantee.getId(), grantee.getTypeCode(), false);
  }

  @Nonnull
  @Override
  public ChangeTrackingResult loadEntitiesChangeTracking(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<PolarisEntityId> entityIds) {
    throw new UnsupportedOperationException("No change tracking - do not call this function");
  }

  @Nonnull
  @Override
  public EntityResult loadEntity(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      @Nonnull PolarisEntityType entityType) {
    var ms = ms(callCtx);
    var entity = ms.lookupEntity(callCtx, entityCatalogId, entityId, entityType.getCode());
    return (entity != null)
        ? new EntityResult(entity)
        : new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
  }

  @Nonnull
  @Override
  public EntitiesResult loadTasks(
      @Nonnull PolarisCallContext callCtx, String executorId, PageToken pageToken) {
    var ms = ms(callCtx);

    // find all available tasks
    var availableTasks =
        ms.listEntities(
            callCtx,
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityType.TASK,
            entity -> {
              var taskState = PolarisObjectMapperUtil.parseTaskState(entity);
              long taskAgeTimeout =
                  callCtx
                      .getConfigurationStore()
                      .getConfiguration(
                          callCtx.getRealmContext(),
                          PolarisTaskConstants.TASK_TIMEOUT_MILLIS_CONFIG,
                          PolarisTaskConstants.TASK_TIMEOUT_MILLIS);
              return taskState == null
                  || taskState.executor == null
                  || callCtx.getClock().millis() - taskState.lastAttemptStartTime > taskAgeTimeout;
            },
            Function.identity(),
            PageToken.readEverything());

    // TODO the following loop is NOT a "load" - it's a mutation over all loaded tasks !!

    availableTasks.items.forEach(
        task -> {
          var newTask = new PolarisBaseEntity.Builder(task);
          var properties =
              PolarisObjectMapperUtil.deserializeProperties(callCtx, task.getProperties());
          properties.put(PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID, executorId);
          properties.put(
              PolarisTaskConstants.LAST_ATTEMPT_START_TIME,
              String.valueOf(callCtx.getClock().millis()));
          properties.put(
              PolarisTaskConstants.ATTEMPT_COUNT,
              String.valueOf(
                  Integer.parseInt(properties.getOrDefault(PolarisTaskConstants.ATTEMPT_COUNT, "0"))
                      + 1));
          newTask.entityVersion(task.getEntityVersion() + 1);
          newTask.properties(PolarisObjectMapperUtil.serializeProperties(callCtx, properties));
          ms.updateEntity(newTask.build());
        });
    return new EntitiesResult(availableTasks.items);
  }

  // Policies

  @Nonnull
  @Override
  public PolicyAttachmentResult attachPolicyToEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisEntityCore> targetCatalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy,
      Map<String, String> parameters) {
    if (parameters == null) {
      parameters = Map.of();
    }
    var ms = ms(callCtx);
    return ms.attachDetachPolicyOnEntity(
        policy.getCatalogId(),
        policy.getId(),
        policy.getPolicyType(),
        target.getCatalogId(),
        target.getId(),
        true,
        parameters);
  }

  @Nonnull
  @Override
  public PolicyAttachmentResult detachPolicyFromEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy) {
    var ms = ms(callCtx);
    return ms.attachDetachPolicyOnEntity(
        policy.getCatalogId(),
        policy.getId(),
        policy.getPolicyType(),
        target.getCatalogId(),
        target.getId(),
        false,
        Map.of());
  }

  @Nonnull
  @Override
  public LoadPolicyMappingsResult loadPoliciesOnEntity(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore target) {
    var ms = ms(callCtx);
    return ms.loadPoliciesOnEntity(
        target.getType(), target.getCatalogId(), target.getId(), Optional.empty());
  }

  @Nonnull
  @Override
  public LoadPolicyMappingsResult loadPoliciesOnEntityByType(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore target,
      @Nonnull PolicyType policyType) {
    var ms = ms(callCtx);
    return ms.loadPoliciesOnEntity(
        target.getType(), target.getCatalogId(), target.getId(), Optional.of(policyType));
  }

  // Principals & PolarisSecretsManager

  @Nonnull
  @Override
  public CreatePrincipalResult createPrincipal(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity principal) {
    return ms(callCtx).createPrincipal(principal, rootCredentialsSet);
  }

  @Nonnull
  @Override
  public PrincipalSecretsResult loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    var ms = ms(callCtx);

    var secrets = ms.loadPrincipalSecrets(callCtx, clientId);

    return (secrets == null)
        ? new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null)
        : new PrincipalSecretsResult(secrets);
  }

  @Nonnull
  @Override
  public PrincipalSecretsResult rotatePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {
    return ms(callCtx).rotatePrincipalSecrets(clientId, principalId, reset, oldSecretHash);
  }

  // PolarisCredentialVendor

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

    var ms = ms(callCtx);

    checkArgument(
        !allowedReadLocations.isEmpty() || !allowedWriteLocations.isEmpty(),
        "allowed_locations_to_subscope_is_required");

    // reload the entity, error out if not found
    var reloadedEntity = loadEntity(callCtx, catalogId, entityId, entityType);
    if (reloadedEntity.getReturnStatus() != BaseResult.ReturnStatus.SUCCESS) {
      return new ScopedCredentialsResult(
          reloadedEntity.getReturnStatus(), reloadedEntity.getExtraInformation());
    }

    // get storage integration
    var storageIntegration = ms.loadPolarisStorageIntegration(callCtx, reloadedEntity.getEntity());

    // cannot be null
    checkNotNull(
        storageIntegration,
        "storage_integration_not_exists, catalogId=%s, entityId=%s",
        catalogId,
        entityId);

    var storageConfigurationInfo =
        BaseMetaStoreManager.extractStorageConfiguration(callCtx, reloadedEntity.getEntity());
    try {
      var creds =
          storageIntegration.getSubscopedCreds(
              callCtx,
              storageConfigurationInfo,
              allowListOperation,
              allowedReadLocations,
              allowedWriteLocations);
      return new ScopedCredentialsResult(creds);
    } catch (Exception ex) {
      return new ScopedCredentialsResult(
          BaseResult.ReturnStatus.SUBSCOPE_CREDS_ERROR, ex.getMessage());
    }
  }

  @Override
  public boolean requiresEntityReload() {
    return false;
  }
}
