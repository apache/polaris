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
import static org.apache.polaris.persistence.nosql.metastore.TypeMapping.mapToEntity;
import static org.apache.polaris.persistence.nosql.metastore.TypeMapping.mapToEntityNameLookupRecord;
import static org.apache.polaris.persistence.nosql.metastore.TypeMapping.principalObjToPolarisPrincipalSecrets;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.PrincipalEntity;
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
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;

class PersistenceMetaStoreManager implements PolarisMetaStoreManager {
  private final Supplier<BaseResult> purgeRealm;
  private final RootCredentialsSet rootCredentialsSet;
  private final Supplier<PersistenceMetaStore> metaStoreSupplier;
  private final Clock clock;

  PersistenceMetaStoreManager(
      Supplier<BaseResult> purgeRealm,
      RootCredentialsSet rootCredentialsSet,
      Supplier<PersistenceMetaStore> metaStoreSupplier,
      Clock clock) {
    this.purgeRealm = purgeRealm;
    this.rootCredentialsSet = rootCredentialsSet;
    this.metaStoreSupplier = metaStoreSupplier;
    this.clock = clock;
  }

  PersistenceMetaStore ms() {
    return metaStoreSupplier.get();
  }

  // Realms

  @Nonnull
  @Override
  public BaseResult bootstrapPolarisService(@Nonnull PolarisCallContext callCtx) {
    bootstrapPolarisServiceInternal();
    return new BaseResult(BaseResult.ReturnStatus.SUCCESS);
  }

  Optional<CreatePrincipalResult> bootstrapPolarisServiceInternal() {
    // This function is idempotent, already existing entities will not be created again.

    var ms = ms();

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
            0L,
            0L,
            PolarisEntityType.PRINCIPAL.getCode(),
            PolarisEntityConstants.getRootPrincipalName());
    var createPrincipalResult = Optional.<CreatePrincipalResult>empty();
    if (rootPrincipal == null) {
      var rootPrincipalId = ms.generateNewId();
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
            0L,
            0L,
            PolarisEntityType.PRINCIPAL_ROLE.getCode(),
            PolarisEntityConstants.getNameOfPrincipalServiceAdminRole());
    if (serviceAdminPrincipalRole == null) {
      // now create the account admin principal role
      var serviceAdminPrincipalRoleId = ms.generateNewId();
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
    var ms = ms();

    var internalProp = catalog.getInternalPropertiesAsMap();
    var integrationIdentifierOrId =
        internalProp.get(PolarisEntityConstants.getStorageIntegrationIdentifierPropertyName());
    var storageConfigInfoStr =
        internalProp.get(PolarisEntityConstants.getStorageConfigInfoPropertyName());

    var integration =
        // storageConfigInfo's presence is needed to create a storage integration,
        // and the catalog should not have an internal property of storage identifier or id yet
        (storageConfigInfoStr != null && integrationIdentifierOrId == null)
            ? ms.createStorageIntegration(
                callCtx,
                catalog.getCatalogId(),
                catalog.getId(),
                PolarisStorageConfigurationInfo.deserialize(storageConfigInfoStr))
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
    var ms = ms();
    return ms.createEntity(entity);
  }

  @SuppressWarnings("unchecked")
  @Nonnull
  @Override
  public EntitiesResult createEntitiesIfNotExist(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull List<? extends PolarisBaseEntity> entities) {
    var ms = ms();
    return ms.createEntities((List<PolarisBaseEntity>) entities);
  }

  @Nonnull
  @Override
  public EntityResult updateEntityPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    var ms = ms();
    return ms.updateEntity(entity);
  }

  @Nonnull
  @Override
  public EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<EntityWithPath> entities) {
    var ms = ms();
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
    var ms = ms();
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
    return ms().dropEntity(entityToDrop, cleanupProperties, cleanup);
  }

  @Nonnull
  @Override
  public EntityResult readEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name) {
    return readEntityByName(catalogPath, entityType, name);
  }

  EntityResult readEntityByName(
      List<PolarisEntityCore> catalogPath, PolarisEntityType entityType, String name) {
    var ms = ms();
    var catalogId = 0L;
    var parentId = 0L;
    if (catalogPath != null && !catalogPath.isEmpty()) {
      catalogId = catalogPath.getFirst().getId();
      parentId = catalogPath.getLast().getId();
    }
    var entity = ms.lookupEntityByName(catalogId, parentId, entityType.getCode(), name);
    return entity != null
        ? new EntityResult(entity)
        : new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
  }

  @Nonnull
  @Override
  public Page<PolarisBaseEntity> loadEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull PageToken pageToken) {
    var catalogStableId =
        (catalogPath != null && !catalogPath.isEmpty()) ? catalogPath.getFirst().getId() : 0L;

    var parentId =
        (catalogPath != null && catalogPath.size() > 1) ? catalogPath.getLast().getId() : 0L;

    return ms().fetchEntitiesAsPage(
            catalogStableId,
            parentId,
            entityType,
            entitySubType,
            pageToken,
            objBase -> mapToEntity(objBase, catalogStableId),
            entity -> true,
            Function.identity());
  }

  @Nonnull
  @Override
  public ListEntitiesResult listEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull PageToken pageToken) {
    var catalogStableId =
        (catalogPath != null && !catalogPath.isEmpty()) ? catalogPath.getFirst().getId() : 0L;

    var parentId =
        (catalogPath != null && catalogPath.size() > 1) ? catalogPath.getLast().getId() : 0L;

    var page =
        ms().fetchEntitiesAsPage(
                catalogStableId,
                parentId,
                entityType,
                entitySubType,
                pageToken,
                objBase -> mapToEntityNameLookupRecord(objBase, catalogStableId),
                entity -> true,
                Function.identity());

    return new ListEntitiesResult(page);
  }

  @Nonnull
  @Override
  public GenerateEntityIdResult generateNewEntityId(@Nonnull PolarisCallContext callCtx) {
    return new GenerateEntityIdResult(metaStoreSupplier.get().generateNewId(callCtx));
  }

  @Nonnull
  @Override
  public ResolvedEntityResult loadResolvedEntityById(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    var ms = ms();

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
    var ms = ms();

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
        true, grantee, catalogPath, securable, privilege);
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
        false, grantee, catalogPath, securable, privilege);
  }

  private PrivilegeResult grantOrRevokePrivilegeOnSecurableToRole(
      boolean grant,
      PolarisEntityCore grantee,
      List<PolarisEntityCore> catalogPath,
      PolarisEntityCore securable,
      PolarisPrivilege privilege) {
    var ms = ms();
    var catalogId =
        catalogPath != null && !catalogPath.isEmpty() ? catalogPath.getFirst().getId() : 0L;
    if (!ms.persistGrantsOrRevokes(catalogId, grant, new Grant(securable, grantee, privilege))) {
      return new PrivilegeResult(BaseResult.ReturnStatus.GRANT_NOT_FOUND, "");
    }

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
    var ms = ms();
    return ms.loadGrants(
        securable.getCatalogId(), securable.getId(), securable.getTypeCode(), true);
  }

  @Nonnull
  @Override
  public LoadGrantsResult loadGrantsToGrantee(
      @Nonnull PolarisCallContext callCtx, PolarisEntityCore grantee) {
    var ms = ms();
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
    var ms = ms();
    var entity = ms.lookupEntity(callCtx, entityCatalogId, entityId, entityType.getCode());
    return (entity != null)
        ? new EntityResult(entity)
        : new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
  }

  @Override
  public <T extends PolarisEntity & LocationBasedEntity>
      Optional<Optional<String>> hasOverlappingSiblings(
          @Nonnull PolarisCallContext callContext, T entity) {
    return Optional.of(ms().hasOverlappingSiblings(entity));
  }

  @Nonnull
  @Override
  public EntitiesResult loadTasks(
      @Nonnull PolarisCallContext callCtx, String executorId, PageToken pageToken) {
    var ms = ms();

    // find all available tasks
    var availableTasks =
        ms.loadEntities(
            callCtx,
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityType.TASK,
            PolarisEntitySubType.ANY_SUBTYPE,
            entity -> {
              var taskState = PolarisObjectMapperUtil.parseTaskState(entity);
              long taskAgeTimeout =
                  callCtx
                      .getRealmConfig()
                      .getConfig(
                          PolarisTaskConstants.TASK_TIMEOUT_MILLIS_CONFIG,
                          PolarisTaskConstants.TASK_TIMEOUT_MILLIS);
              return taskState == null
                  || taskState.executor == null
                  || clock.millis() - taskState.lastAttemptStartTime > taskAgeTimeout;
            },
            Function.identity(),
            PageToken.readEverything());

    // TODO the following loop is NOT a "load" - it's a mutation over all loaded tasks !!

    availableTasks
        .items()
        .forEach(
            task -> {
              var newTask = new PolarisBaseEntity.Builder(task);
              var properties = PolarisObjectMapperUtil.deserializeProperties(task.getProperties());
              properties.put(PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID, executorId);
              properties.put(
                  PolarisTaskConstants.LAST_ATTEMPT_START_TIME, String.valueOf(clock.millis()));
              properties.put(
                  PolarisTaskConstants.ATTEMPT_COUNT,
                  String.valueOf(
                      Integer.parseInt(
                              properties.getOrDefault(PolarisTaskConstants.ATTEMPT_COUNT, "0"))
                          + 1));
              newTask.entityVersion(task.getEntityVersion() + 1);
              newTask.properties(PolarisObjectMapperUtil.serializeProperties(properties));
              ms.updateEntity(newTask.build());
            });
    return new EntitiesResult(Page.fromItems(availableTasks.items()));
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
    var ms = ms();
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
    var ms = ms();
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
    var ms = ms();
    return ms.loadPoliciesOnEntity(
        target.getType(), target.getCatalogId(), target.getId(), Optional.empty());
  }

  @Nonnull
  @Override
  public LoadPolicyMappingsResult loadPoliciesOnEntityByType(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore target,
      @Nonnull PolicyType policyType) {
    var ms = ms();
    return ms.loadPoliciesOnEntity(
        target.getType(), target.getCatalogId(), target.getId(), Optional.of(policyType));
  }

  // Principals & PolarisSecretsManager

  @Nonnull
  @Override
  public CreatePrincipalResult createPrincipal(
      @Nonnull PolarisCallContext callCtx, @Nonnull PrincipalEntity principal) {
    return ms().createPrincipal(principal, rootCredentialsSet);
  }

  @Nonnull
  @Override
  public PrincipalSecretsResult loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    var ms = ms();

    var secrets = ms.loadPrincipalSecrets(clientId);

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
    return rotatePrincipalSecrets(principalId, reset, oldSecretHash);
  }

  PrincipalSecretsResult rotatePrincipalSecrets(
      long principalId, boolean reset, String oldSecretHash) {
    try {
      return ms().updatePrincipalSecrets(
              PrincipalSecretsResult.class,
              "rotatePrincipalSecrets",
              principalId,
              (principal, updatedPrincipalBuilder) -> {
                var principalSecrets = principalObjToPolarisPrincipalSecrets(principal);

                // rotate the secrets
                principalSecrets.rotateSecrets(oldSecretHash);
                if (reset) {
                  principalSecrets.rotateSecrets(principalSecrets.getMainSecretHash());
                }

                updatedPrincipalBuilder
                    .entityVersion(principal.entityVersion() + 1)
                    .credentialRotationRequired(reset && !principal.credentialRotationRequired())
                    .clientId(principalSecrets.getPrincipalClientId())
                    .mainSecretHash(principalSecrets.getMainSecretHash())
                    .secondarySecretHash(principalSecrets.getSecondarySecretHash())
                    .secretSalt(principalSecrets.getSecretSalt());

                return new PrincipalSecretsResult(principalSecrets);
              });
    } catch (PersistenceMetaStore.PrincipalNotFoundException ignore) {
      return new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }
  }

  @Nonnull
  @Override
  public PrincipalSecretsResult resetPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      long principalId,
      String resolvedClientId,
      String customClientSecret) {
    try {
      return ms().updatePrincipalSecrets(
              PrincipalSecretsResult.class,
              "resetPrincipalSecrets",
              principalId,
              (principal, updatedPrincipalBuilder) -> {
                var principalSecrets =
                    new PolarisPrincipalSecrets(
                        principal.stableId(), resolvedClientId, customClientSecret);
                updatedPrincipalBuilder
                    .entityVersion(principal.entityVersion() + 1)
                    .clientId(principalSecrets.getPrincipalClientId())
                    .mainSecretHash(principalSecrets.getMainSecretHash())
                    .secondarySecretHash(principalSecrets.getSecondarySecretHash())
                    .secretSalt(principalSecrets.getSecretSalt());

                return new PrincipalSecretsResult(principalSecrets);
              });
    } catch (PersistenceMetaStore.PrincipalNotFoundException ignore) {
      return new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }
  }

  @Override
  public void deletePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId, long principalId) {
    ms().updatePrincipalSecrets(
            String.class,
            "deletePrincipalSecrets",
            principalId,
            (principal, updatedPrincipalBuilder) -> {
              // Do NOT update the entityVersion
              updatedPrincipalBuilder
                  .clientId(Optional.empty())
                  .secretSalt(Optional.empty())
                  .mainSecretHash(Optional.empty())
                  .secondarySecretHash(Optional.empty());
              return ""; // need some non-null return value
            });
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
      @Nonnull Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint) {

    var ms = ms();

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

    try {
      var creds =
          storageIntegration.getSubscopedCreds(
              callCtx.getRealmConfig(),
              allowListOperation,
              allowedReadLocations,
              allowedWriteLocations,
              refreshCredentialsEndpoint);
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

  @Override
  public void writeEvents(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<PolarisEvent> polarisEvents) {
    throw new UnsupportedOperationException("Events not supported in NoSQL persistence");
  }
}
