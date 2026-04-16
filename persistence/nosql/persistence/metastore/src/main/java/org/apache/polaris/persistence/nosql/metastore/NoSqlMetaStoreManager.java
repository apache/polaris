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
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.ENTITY_NOT_FOUND;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.GRANT_NOT_FOUND;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.SUBSCOPE_CREDS_ERROR;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.mapToEntity;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.mapToEntityNameLookupRecord;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.principalObjToPolarisPrincipalSecrets;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
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
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.apache.polaris.core.persistence.dao.entity.ScopedCredentialsResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.persistence.nosql.metastore.privs.SecurableGranteePrivilegeTuple;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

record NoSqlMetaStoreManager(
    Supplier<BaseResult> purgeRealm, RootCredentialsSet rootCredentialsSet, Clock clock)
    implements PolarisMetaStoreManager {

  NoSqlMetaStore ms(PolarisCallContext callContext) {
    var existing = callContext.getMetaStore();
    checkArgument(existing instanceof NoSqlMetaStore, "No meta store found in call context");
    return (NoSqlMetaStore) existing;
  }

  // Realms

  @NonNull
  @Override
  public BaseResult purge(@NonNull PolarisCallContext callCtx) {
    return purgeRealm.get();
  }

  // Catalog

  @NonNull
  @Override
  public CreateCatalogResult createCatalog(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity catalog,
      @NonNull List<PolarisEntityCore> principalRoles) {
    var prRoles = principalRoles.stream().map(PolarisBaseEntity.class::cast).toList();

    return ms(callCtx).createCatalog(catalog, prRoles);
  }

  // Generic entities

  @NonNull
  @Override
  public EntityResult createEntityIfNotExists(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity) {
    return ms(callCtx).createEntity(entity);
  }

  @SuppressWarnings("unchecked")
  @NonNull
  @Override
  public EntitiesResult createEntitiesIfNotExist(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull List<? extends PolarisBaseEntity> entities) {
    return ms(callCtx).createEntities((List<PolarisBaseEntity>) entities);
  }

  @NonNull
  @Override
  public EntityResult updateEntityPropertiesIfNotChanged(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity) {
    return ms(callCtx).updateEntity(entity);
  }

  @NonNull
  @Override
  public EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @NonNull PolarisCallContext callCtx, @NonNull List<EntityWithPath> entities) {
    return ms(callCtx).updateEntities(entities);
  }

  @NonNull
  @Override
  public EntityResult renameEntity(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @NonNull PolarisEntity renamedEntity) {
    if (newCatalogPath != null && !newCatalogPath.isEmpty()) {
      var last = newCatalogPath.getLast();
      // At least BasePolarisMetaStoreManagerTest comes with the wrong parentId in renamedEntity
      if (renamedEntity.getParentId() != last.getId()) {
        renamedEntity = new PolarisEntity.Builder(renamedEntity).setParentId(last.getId()).build();
      }
    }
    return ms(callCtx).updateEntity(renamedEntity);
  }

  @NonNull
  @Override
  public DropEntityResult dropEntityIfExists(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    return ms(callCtx).dropEntity(entityToDrop, cleanupProperties, cleanup);
  }

  @NonNull
  @Override
  public EntityResult readEntityByName(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull String name) {
    return readEntityByName(ms(callCtx), catalogPath, entityType, name);
  }

  EntityResult readEntityByName(
      NoSqlMetaStore ms,
      List<PolarisEntityCore> catalogPath,
      PolarisEntityType entityType,
      String name) {
    var catalogId = 0L;
    var parentId = 0L;
    if (catalogPath != null && !catalogPath.isEmpty()) {
      catalogId = catalogPath.getFirst().getId();
      parentId = catalogPath.getLast().getId();
    }
    var entity = ms.lookupEntityByName(catalogId, parentId, entityType.getCode(), name);
    return entity != null ? new EntityResult(entity) : new EntityResult(ENTITY_NOT_FOUND, null);
  }

  @NonNull
  @Override
  public Page<PolarisBaseEntity> listFullEntities(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
    var catalogStableId =
        (catalogPath != null && !catalogPath.isEmpty()) ? catalogPath.getFirst().getId() : 0L;

    var parentId =
        (catalogPath != null && catalogPath.size() > 1) ? catalogPath.getLast().getId() : 0L;

    return ms(callCtx)
        .fetchEntitiesAsPage(
            catalogStableId,
            parentId,
            entityType,
            entitySubType,
            pageToken,
            objBase -> mapToEntity(objBase, catalogStableId),
            entity -> true,
            Function.identity());
  }

  @NonNull
  @Override
  public ListEntitiesResult listEntities(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
    var catalogStableId =
        (catalogPath != null && !catalogPath.isEmpty()) ? catalogPath.getFirst().getId() : 0L;

    var parentId =
        (catalogPath != null && catalogPath.size() > 1) ? catalogPath.getLast().getId() : 0L;

    var page =
        ms(callCtx)
            .fetchEntitiesAsPage(
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

  @NonNull
  @Override
  public GenerateEntityIdResult generateNewEntityId(@NonNull PolarisCallContext callCtx) {
    return new GenerateEntityIdResult(ms(callCtx).generateNewId());
  }

  @NonNull
  @Override
  public ResolvedEntitiesResult loadResolvedEntities(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityType entityType,
      @NonNull List<PolarisEntityId> entityIds) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @NonNull
  @Override
  public ResolvedEntityResult loadResolvedEntityById(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    var ms = ms(callCtx);

    // load that entity
    PolarisBaseEntity entity = ms.lookupEntity(entityCatalogId, entityId, entityType.getCode());

    // if entity not found, return null
    if (entity == null) {
      return new ResolvedEntityResult(ENTITY_NOT_FOUND, null);
    }

    // load the grant records
    var grantRecords = ms.allGrantRecords(entity);

    // return the result
    return new ResolvedEntityResult(entity, entity.getGrantRecordsVersion(), grantRecords);
  }

  @NonNull
  @Override
  public ResolvedEntityResult loadResolvedEntityByName(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @NonNull PolarisEntityType entityType,
      @NonNull String entityName) {
    var ms = ms(callCtx);

    // load that entity
    var entity = ms.lookupEntityByName(entityCatalogId, parentId, entityType.getCode(), entityName);

    // null if entity not found
    if (entity == null) {
      return new ResolvedEntityResult(ENTITY_NOT_FOUND, null);
    }

    // load the grant records
    var grantRecords = ms.allGrantRecords(entity);

    // return the result
    return new ResolvedEntityResult(entity, entity.getGrantRecordsVersion(), grantRecords);
  }

  @NonNull
  @Override
  public ResolvedEntityResult refreshResolvedEntity(
      @NonNull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      @NonNull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {
    return loadResolvedEntityById(callCtx, entityCatalogId, entityId, entityType);
  }

  // Principals & Polaris GrantManager

  @NonNull
  @Override
  public PrivilegeResult grantUsageOnRoleToGrantee(
      @NonNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NonNull PolarisEntityCore role,
      @NonNull PolarisEntityCore grantee) {
    var privilege =
        (grantee.getType() == PolarisEntityType.PRINCIPAL_ROLE)
            ? PolarisPrivilege.CATALOG_ROLE_USAGE
            : PolarisPrivilege.PRINCIPAL_ROLE_USAGE;

    return grantPrivilegeOnSecurableToRole(callCtx, grantee, null, role, privilege);
  }

  @NonNull
  @Override
  public PrivilegeResult revokeUsageOnRoleFromGrantee(
      @NonNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NonNull PolarisEntityCore role,
      @NonNull PolarisEntityCore grantee) {
    var privilege =
        (grantee.getType() == PolarisEntityType.PRINCIPAL_ROLE)
            ? PolarisPrivilege.CATALOG_ROLE_USAGE
            : PolarisPrivilege.PRINCIPAL_ROLE_USAGE;

    return revokePrivilegeOnSecurableFromRole(callCtx, grantee, null, role, privilege);
  }

  @NonNull
  @Override
  public PrivilegeResult grantPrivilegeOnSecurableToRole(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityCore securable,
      @NonNull PolarisPrivilege privilege) {
    return grantOrRevokePrivilegeOnSecurableToRole(callCtx, true, grantee, securable, privilege);
  }

  @NonNull
  @Override
  public PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityCore securable,
      @NonNull PolarisPrivilege privilege) {
    return grantOrRevokePrivilegeOnSecurableToRole(callCtx, false, grantee, securable, privilege);
  }

  private PrivilegeResult grantOrRevokePrivilegeOnSecurableToRole(
      PolarisCallContext callCtx,
      boolean grant,
      PolarisEntityCore grantee,
      PolarisEntityCore securable,
      PolarisPrivilege privilege) {
    if (!ms(callCtx)
            .persistGrantsOrRevokes(
                grant, new SecurableGranteePrivilegeTuple(securable, grantee, privilege))
        && !grant) {
      return new PrivilegeResult(GRANT_NOT_FOUND, "");
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

  @NonNull
  @Override
  public LoadGrantsResult loadGrantsOnSecurable(
      @NonNull PolarisCallContext callCtx, PolarisEntityCore securable) {
    return ms(callCtx)
        .loadGrants(securable.getCatalogId(), securable.getId(), securable.getTypeCode(), true);
  }

  @NonNull
  @Override
  public LoadGrantsResult loadGrantsToGrantee(
      @NonNull PolarisCallContext callCtx, PolarisEntityCore grantee) {
    return ms(callCtx)
        .loadGrants(grantee.getCatalogId(), grantee.getId(), grantee.getTypeCode(), false);
  }

  @NonNull
  @Override
  public ChangeTrackingResult loadEntitiesChangeTracking(
      @NonNull PolarisCallContext callCtx, @NonNull List<PolarisEntityId> entityIds) {
    throw new UnsupportedOperationException("No change tracking - do not call this function");
  }

  @NonNull
  @Override
  public EntityResult loadEntity(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      @NonNull PolarisEntityType entityType) {
    var entity = ms(callCtx).lookupEntity(entityCatalogId, entityId, entityType.getCode());
    return (entity != null) ? new EntityResult(entity) : new EntityResult(ENTITY_NOT_FOUND, null);
  }

  @Override
  public <T extends PolarisEntity & LocationBasedEntity>
      Optional<Optional<String>> hasOverlappingSiblings(
          @NonNull PolarisCallContext callContext, T entity) {
    return Optional.of(ms(callContext).hasOverlappingSiblings(entity));
  }

  @NonNull
  @Override
  public EntitiesResult loadTasks(
      @NonNull PolarisCallContext callCtx, String executorId, PageToken pageToken) {
    var ms = ms(callCtx);

    // find all available tasks
    var availableTasks =
        ms.fetchEntitiesAsPage(
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityType.TASK,
            PolarisEntitySubType.ANY_SUBTYPE,
            pageToken,
            objBase -> mapToEntity(objBase, PolarisEntityConstants.getRootEntityId()),
            entity -> {
              var taskState = PolarisObjectMapperUtil.parseTaskState(entity);
              long taskAgeTimeout =
                  callCtx
                      .getRealmConfig()
                      .getConfig(FeatureConfiguration.POLARIS_TASK_TIMEOUT_MILLIS);
              return taskState == null
                  || taskState.executor == null
                  || clock.millis() - taskState.lastAttemptStartTime > taskAgeTimeout;
            },
            Function.identity());

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

  @NonNull
  @Override
  public PolicyAttachmentResult attachPolicyToEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisEntityCore> targetCatalogPath,
      @NonNull PolarisEntityCore target,
      @NonNull List<PolarisEntityCore> policyCatalogPath,
      @NonNull PolicyEntity policy,
      Map<String, String> parameters) {
    if (parameters == null) {
      parameters = Map.of();
    }
    return ms(callCtx)
        .attachDetachPolicyOnEntity(
            policy.getCatalogId(),
            policy.getId(),
            policy.getPolicyType(),
            target.getCatalogId(),
            target.getId(),
            true,
            parameters);
  }

  @NonNull
  @Override
  public PolicyAttachmentResult detachPolicyFromEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityCore target,
      @NonNull List<PolarisEntityCore> policyCatalogPath,
      @NonNull PolicyEntity policy) {
    return ms(callCtx)
        .attachDetachPolicyOnEntity(
            policy.getCatalogId(),
            policy.getId(),
            policy.getPolicyType(),
            target.getCatalogId(),
            target.getId(),
            false,
            Map.of());
  }

  @NonNull
  @Override
  public LoadPolicyMappingsResult loadPoliciesOnEntity(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisEntityCore target) {
    return ms(callCtx)
        .loadPoliciesOnEntity(
            target.getType(), target.getCatalogId(), target.getId(), Optional.empty());
  }

  @NonNull
  @Override
  public LoadPolicyMappingsResult loadPoliciesOnEntityByType(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore target,
      @NonNull PolicyType policyType) {
    return ms(callCtx)
        .loadPoliciesOnEntity(
            target.getType(), target.getCatalogId(), target.getId(), Optional.of(policyType));
  }

  // Principals & PolarisSecretsManager

  @NonNull
  @Override
  public CreatePrincipalResult createPrincipal(
      @NonNull PolarisCallContext callCtx, @NonNull PrincipalEntity principal) {
    return ms(callCtx).createPrincipal(principal, rootCredentialsSet);
  }

  @NonNull
  @Override
  public PrincipalSecretsResult loadPrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId) {
    var secrets = ms(callCtx).loadPrincipalSecrets(clientId);

    return (secrets == null)
        ? new PrincipalSecretsResult(ENTITY_NOT_FOUND, null)
        : new PrincipalSecretsResult(secrets);
  }

  @NonNull
  @Override
  public PrincipalSecretsResult rotatePrincipalSecrets(
      @NonNull PolarisCallContext callCtx,
      @NonNull String clientId,
      long principalId,
      boolean reset,
      @NonNull String oldSecretHash) {
    return rotatePrincipalSecrets(ms(callCtx), principalId, reset, oldSecretHash);
  }

  PrincipalSecretsResult rotatePrincipalSecrets(
      NoSqlMetaStore ms, long principalId, boolean reset, String oldSecretHash) {
    try {
      return ms.updatePrincipalSecrets(
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
    } catch (NoSqlMetaStore.PrincipalNotFoundException ignore) {
      return new PrincipalSecretsResult(ENTITY_NOT_FOUND, null);
    }
  }

  @NonNull
  @Override
  public PrincipalSecretsResult resetPrincipalSecrets(
      @NonNull PolarisCallContext callCtx,
      long principalId,
      @NonNull String resolvedClientId,
      String customClientSecret) {
    try {
      return ms(callCtx)
          .updatePrincipalSecrets(
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
    } catch (NoSqlMetaStore.PrincipalNotFoundException ignore) {
      return new PrincipalSecretsResult(ENTITY_NOT_FOUND, null);
    }
  }

  @Override
  public void deletePrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId, long principalId) {
    ms(callCtx)
        .updatePrincipalSecrets(
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

  @NonNull
  @Override
  public ScopedCredentialsResult getSubscopedCredsForEntity(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      @NonNull PolarisEntityType entityType,
      boolean allowListOperation,
      @NonNull Set<String> allowedReadLocations,
      @NonNull Set<String> allowedWriteLocations,
      @NonNull PolarisPrincipal polarisPrincipal,
      Optional<String> refreshCredentialsEndpoint,
      @NonNull CredentialVendingContext credentialVendingContext) {

    checkArgument(
        !allowedReadLocations.isEmpty() || !allowedWriteLocations.isEmpty(),
        "allowed_locations_to_subscope_is_required");

    // reload the entity or error out if not found
    var reloadedEntity = loadEntity(callCtx, catalogId, entityId, entityType);
    if (reloadedEntity.getReturnStatus() != BaseResult.ReturnStatus.SUCCESS) {
      return new ScopedCredentialsResult(
          reloadedEntity.getReturnStatus(), reloadedEntity.getExtraInformation());
    }

    // get storage integration
    var storageIntegration = ms(callCtx).loadPolarisStorageIntegration(reloadedEntity.getEntity());

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
              polarisPrincipal,
              refreshCredentialsEndpoint,
              credentialVendingContext);
      return new ScopedCredentialsResult(creds);
    } catch (Exception ex) {
      return new ScopedCredentialsResult(SUBSCOPE_CREDS_ERROR, ex.getMessage());
    }
  }

  @Override
  public boolean requiresEntityReload() {
    return false;
  }

  @Override
  public void writeEvents(
      @NonNull PolarisCallContext callCtx, @NonNull List<PolarisEvent> polarisEvents) {
    throw new UnsupportedOperationException("Events not supported in NoSQL persistence");
  }
}
