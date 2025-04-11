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
package org.apache.polaris.persistence.bridge;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.POLICY_MAPPING_OF_SAME_TYPE_ALREADY_EXISTS;
import static org.apache.polaris.persistence.api.index.IndexContainer.newUpdatableIndex;
import static org.apache.polaris.persistence.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.bridge.Identifier.identifierToIndexKey;
import static org.apache.polaris.persistence.bridge.Identifier.indexKeyToIdentifier;
import static org.apache.polaris.persistence.bridge.Identifier.indexKeyToIdentifierBuilder;
import static org.apache.polaris.persistence.bridge.MemoizedIndexedAccess.newMemoizedIndexedAccess;
import static org.apache.polaris.persistence.bridge.MutationResults.newMutableMutationResults;
import static org.apache.polaris.persistence.bridge.MutationResults.singleEntityResult;
import static org.apache.polaris.persistence.bridge.TypeMapping.containerTypeForEntityType;
import static org.apache.polaris.persistence.bridge.TypeMapping.entitySubTypeCodeFromObjType;
import static org.apache.polaris.persistence.bridge.TypeMapping.initializeCatalogIfNecessary;
import static org.apache.polaris.persistence.bridge.TypeMapping.isCatalogContent;
import static org.apache.polaris.persistence.bridge.TypeMapping.mapToEntity;
import static org.apache.polaris.persistence.bridge.TypeMapping.mapToEntityNameLookupRecord;
import static org.apache.polaris.persistence.bridge.TypeMapping.mapToObj;
import static org.apache.polaris.persistence.bridge.TypeMapping.maybeObjToPolarisPrincipalSecrets;
import static org.apache.polaris.persistence.bridge.TypeMapping.newContainerBuilderForEntityType;
import static org.apache.polaris.persistence.bridge.TypeMapping.objTypeForPolarisType;
import static org.apache.polaris.persistence.bridge.TypeMapping.objTypeForPolarisTypeForFiltering;
import static org.apache.polaris.persistence.bridge.TypeMapping.polarisPrincipalSecretsToPrincipal;
import static org.apache.polaris.persistence.bridge.TypeMapping.principalObjToPolarisPrincipalSecrets;
import static org.apache.polaris.persistence.bridge.TypeMapping.referenceName;
import static org.apache.polaris.persistence.coretypes.realm.PolicyMapping.POLICY_MAPPING_SERIALIZER;
import static org.apache.polaris.persistence.coretypes.realm.PolicyMappingsObj.POLICY_MAPPINGS_REF_NAME;
import static org.apache.polaris.persistence.coretypes.realm.RealmGrantsObj.REALM_GRANTS_REF_NAME;
import static org.apache.polaris.persistence.coretypes.refs.References.perCatalogReferenceName;

import com.google.common.collect.Streams;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.authz.api.Privilege;
import org.apache.polaris.authz.api.PrivilegeSet;
import org.apache.polaris.authz.api.Privileges;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.IntegrationPersistence;
import org.apache.polaris.core.persistence.PolarisObjectMapperUtil;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.apache.polaris.core.persistence.dao.entity.LoadPolicyMappingsResult;
import org.apache.polaris.core.persistence.dao.entity.PolicyAttachmentResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.persistence.api.Persistence;
import org.apache.polaris.persistence.api.commit.CommitException;
import org.apache.polaris.persistence.api.commit.CommitRetryable;
import org.apache.polaris.persistence.api.commit.CommitterState;
import org.apache.polaris.persistence.api.index.Index;
import org.apache.polaris.persistence.api.index.IndexContainer;
import org.apache.polaris.persistence.api.index.IndexKey;
import org.apache.polaris.persistence.api.index.UpdatableIndex;
import org.apache.polaris.persistence.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.api.obj.ObjRef;
import org.apache.polaris.persistence.api.obj.ObjTypes;
import org.apache.polaris.persistence.coretypes.ContainerObj;
import org.apache.polaris.persistence.coretypes.ObjBase;
import org.apache.polaris.persistence.coretypes.acl.AclObj;
import org.apache.polaris.persistence.coretypes.acl.GrantsObj;
import org.apache.polaris.persistence.coretypes.catalog.CatalogObj;
import org.apache.polaris.persistence.coretypes.catalog.CatalogRoleObj;
import org.apache.polaris.persistence.coretypes.catalog.CatalogRolesObj;
import org.apache.polaris.persistence.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.coretypes.catalog.CatalogsObj;
import org.apache.polaris.persistence.coretypes.changes.Change;
import org.apache.polaris.persistence.coretypes.changes.ChangeAdd;
import org.apache.polaris.persistence.coretypes.changes.ChangeRemove;
import org.apache.polaris.persistence.coretypes.changes.ChangeRename;
import org.apache.polaris.persistence.coretypes.changes.ChangeUpdate;
import org.apache.polaris.persistence.coretypes.principals.PrincipalObj;
import org.apache.polaris.persistence.coretypes.principals.PrincipalsObj;
import org.apache.polaris.persistence.coretypes.realm.PolicyMapping;
import org.apache.polaris.persistence.coretypes.realm.PolicyMappingsObj;
import org.apache.polaris.persistence.coretypes.realm.RealmGrantsObj;
import org.apache.polaris.persistence.coretypes.realm.RootObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PersistenceMetaStore implements BasePersistence, IntegrationPersistence {
  private static final Logger LOGGER = LoggerFactory.getLogger(PersistenceMetaStore.class);

  private final Persistence persistence;
  private final Privileges privileges;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;
  private final MemoizedIndexedAccess memoizedIndexedAccess;

  PersistenceMetaStore(
      Persistence persistence,
      Privileges privileges,
      PolarisStorageIntegrationProvider storageIntegrationProvider) {
    this.persistence = persistence;
    this.privileges = privileges;
    this.storageIntegrationProvider = storageIntegrationProvider;
    this.memoizedIndexedAccess = newMemoizedIndexedAccess(persistence);
  }

  private <RESULT> RESULT performPrincipalChange(
      @Nonnull Class<RESULT> resultType,
      @Nonnull PrincipalsChangeCommitter<RESULT> commitRetryable) {
    try {
      return persistence
          .createCommitter(PrincipalsObj.PRINCIPALS_REF_NAME, PrincipalsObj.class, resultType)
          .synchronizingLocally()
          .commitRuntimeException(new PrincipalsChangeCommitterWrapper<>(commitRetryable))
          .orElseThrow();
    } finally {
      memoizedIndexedAccess.invalidateIndexedAccess(0L, PolarisEntityType.PRINCIPAL.getCode());
    }
  }

  <REF_OBJ extends ContainerObj, B extends ContainerObj.Builder<REF_OBJ, B>, RESULT>
      RESULT performChange(
          @Nonnull PolarisEntityType entityType,
          @Nonnull Class<REF_OBJ> referencedObjType,
          @Nonnull Class<RESULT> resultType,
          long catalogStableId,
          @Nonnull ChangeCommitter<REF_OBJ, RESULT> changeCommitter) {
    try {
      var committer =
          persistence
              .createCommitter(
                  referenceName(entityType, catalogStableId), referencedObjType, resultType)
              .synchronizingLocally();
      @SuppressWarnings("unchecked")
      var commitRetryable =
          new ChangeCommitterWrapper<>(
              changeCommitter, () -> (B) newContainerBuilderForEntityType(entityType));
      return committer.commitRuntimeException(commitRetryable).orElseThrow();
    } finally {
      memoizedIndexedAccess.invalidateIndexedAccess(catalogStableId, entityType.getCode());
    }
  }

  @Override
  public long generateNewId(@Nonnull PolarisCallContext callCtx) {
    return persistence.generateId();
  }

  void initializeCatalogsIfNecessary() {
    memoizedIndexedAccess
        .indexedAccess(0, PolarisEntityType.CATALOG.getCode())
        .nameIndex()
        .ifPresent(
            names ->
                names.forEach(
                    e -> {
                      var catalogObj = persistence.fetch(e.getValue(), CatalogObj.class);
                      if (catalogObj != null) {
                        LOGGER.debug("Initializing catalog {} if necessary", catalogObj.name());
                        initializeCatalogIfNecessary(persistence, catalogObj);
                      }
                    }));
  }

  CreateCatalogResult createCatalog(
      @Nonnull PolarisCallContext callCtx,
      PolarisBaseEntity catalog,
      List<PolarisBaseEntity> principalRoles,
      PolarisStorageIntegration<PolarisStorageConfigurationInfo> integration) {
    checkArgument(catalog != null && catalog.getType() == PolarisEntityType.CATALOG);

    LOGGER.debug("create catalog #{} '{}'", catalog.getId(), catalog.getName());

    return performChange(
        PolarisEntityType.CATALOG,
        CatalogsObj.class,
        CreateCatalogResult.class,
        0L,
        ((state, ref, byName, byId) -> {
          var nameKey = IndexKey.key(catalog.getName());
          var idKey = IndexKey.key(catalog.getId());

          // check if that catalog has already been created
          var existing = byName.get(nameKey);
          var persistence = state.persistence();
          var catalogObj = existing != null ? persistence.fetch(existing, CatalogObj.class) : null;

          // if found, probably a retry, simply return the previously created catalog
          // TODO not sure how a "retry" could happen with the same ID though (see
          //  PolarisMetaStoreManagerImpl.createCatalog())...
          if (catalogObj != null && catalogObj.stableId() != catalog.getId()) {
            // catalog with the same name already exists (different ID)
            return new ChangeResult.NoChange<>(
                new CreateCatalogResult(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, null));
          }
          if (catalogObj == null) {
            catalogObj =
                TypeMapping.<CatalogObj, CatalogObj.Builder>mapToObj(catalog, Optional.empty())
                    .id(persistence.generateId())
                    .build();
            state.writeOrReplace("catalog", catalogObj);
          }

          initializeCatalogIfNecessary(persistence, catalogObj);

          checkState(!byId.contains(idKey), "Catalog ID %s already used", catalog.getId());

          // 'persistStorageIntegrationIfNeeded' is a no-op in all implementations ?!?!?
          persistStorageIntegrationIfNeeded(callCtx, catalog, integration);

          var catalogAdminRoleObj =
              createCatalogRoleIdempotent(
                  catalogObj,
                  persistence.generateId(),
                  PolarisEntityConstants.getNameOfCatalogAdminRole());

          var catalogAdminRole = mapToEntity(catalogAdminRoleObj, catalogObj.stableId());

          var grants = new ArrayList<Grant>();

          // grant the catalog admin role access-management on the catalog
          grants.add(new Grant(catalog, catalogAdminRole, PolarisPrivilege.CATALOG_MANAGE_ACCESS));
          // grant the catalog admin role metadata-management on the catalog; this one is revocable
          grants.add(
              new Grant(catalog, catalogAdminRole, PolarisPrivilege.CATALOG_MANAGE_METADATA));

          var effRoles =
              principalRoles.isEmpty()
                  ? List.of(
                      requireNonNull(
                          lookupEntityByName(
                              callCtx,
                              0L,
                              0L,
                              PolarisEntityType.PRINCIPAL_ROLE.getCode(),
                              PolarisEntityConstants.getNameOfPrincipalServiceAdminRole())))
                  : principalRoles;

          for (PolarisBaseEntity effRole : effRoles) {
            grants.add(new Grant(catalogAdminRole, effRole, PolarisPrivilege.CATALOG_ROLE_USAGE));
          }

          persistGrantsOrRevokes(0L, true, grants.toArray(Grant[]::new));

          byName.put(nameKey, objRef(catalogObj));
          byId.put(idKey, nameKey);

          if (existing == null) {
            // created
            return new ChangeResult.CommitChange<>(
                new CreateCatalogResult(catalog, catalogAdminRole));
          }
          // retry
          return new ChangeResult.NoChange<>(
              new CreateCatalogResult(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, null));
        }));
  }

  CatalogRoleObj createCatalogRoleIdempotent(
      @Nonnull CatalogObj catalogObj, long catalogRoleStableId, @Nonnull String roleName) {
    return performChange(
        PolarisEntityType.CATALOG_ROLE,
        CatalogRolesObj.class,
        CatalogRoleObj.class,
        catalogObj.stableId(),
        ((state, ref, byName, byId) -> {
          var nameKey = IndexKey.key(roleName);
          var idKey = IndexKey.key(catalogRoleStableId);
          var nameRef = byName.get(nameKey);
          var persistence = state.persistence();

          if (nameRef != null) {
            var role = persistence.fetch(nameRef, CatalogRoleObj.class);
            requireNonNull(role);
            return new ChangeResult.NoChange<>(role);
          }

          checkState(!byId.contains(idKey), "Catalog role ID %s already used", catalogRoleStableId);

          var now = persistence.currentInstant();
          var roleObj =
              CatalogRoleObj.builder()
                  .id(persistence.generateId())
                  .name(roleName)
                  .createTimestamp(now)
                  .updateTimestamp(now)
                  .stableId(catalogRoleStableId)
                  .parentStableId(catalogObj.stableId())
                  .build();

          state.writeOrReplace("role", roleObj);

          byName.put(nameKey, objRef(roleObj));
          byId.put(idKey, nameKey);

          return new ChangeResult.CommitChange<>(roleObj);
        }));
  }

  @Override
  public void writeEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      // nameOrParentChanged is true, if originalEntity==null or the parentId or the name changed
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity) {
    throw useMetaStoreManager("create/update/rename/delete");
  }

  @Override
  public void writeEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisBaseEntity> entities,
      @Nullable List<PolarisBaseEntity> originalEntities) {
    throw useMetaStoreManager("create/update/rename/delete");
  }

  private String logEntityInfo(PolarisEntityCore e) {
    return format("%s #%d catalog:%d '%s'", e.getType(), e.getId(), e.getCatalogId(), e.getName());
  }

  private String logEntitiesInfo(List<? extends PolarisEntityCore> entities) {
    return entities.stream()
        .map(this::logEntityInfo)
        .collect(Collectors.joining(", ", "(" + entities.size() + ") ", ""));
  }

  EntityResult createEntity(PolarisBaseEntity entity) {
    LOGGER.atDebug().addArgument(() -> logEntityInfo(entity)).log("create entity: {}");

    return createOrUpdateEntity(EntityUpdate.Operation.CREATE, entity);
  }

  EntitiesResult createEntities(List<PolarisBaseEntity> entities) {
    LOGGER.atDebug().addArgument(() -> logEntitiesInfo(entities)).log("create entities: {}");

    return createOrUpdateEntities(entities.stream(), EntityUpdate.Operation.CREATE);
  }

  EntityResult updateEntity(PolarisBaseEntity entity) {
    LOGGER.atDebug().addArgument(() -> logEntityInfo(entity)).log("update entity: {}");
    return createOrUpdateEntity(EntityUpdate.Operation.UPDATE, entity);
  }

  EntitiesResult updateEntities(List<EntityWithPath> entities) {
    LOGGER
        .atDebug()
        .addArgument(
            () -> logEntitiesInfo(entities.stream().map(EntityWithPath::getEntity).toList()))
        .log("update entities: {}");

    return createOrUpdateEntities(
        entities.stream().map(EntityWithPath::getEntity), EntityUpdate.Operation.UPDATE);
  }

  private EntityResult createOrUpdateEntity(EntityUpdate.Operation op, PolarisBaseEntity entity) {
    var mutationResults =
        performEntityMutations(Concern.forEntity(entity), List.of(new EntityUpdate(op, entity)));
    return (EntityResult) mutationResults.results().getFirst();
  }

  private EntitiesResult createOrUpdateEntities(
      Stream<PolarisBaseEntity> entitiesStream, EntityUpdate.Operation op) {
    var byConcern =
        entitiesStream
            .map(e -> new EntityUpdate(op, e))
            .collect(Collectors.groupingBy(u -> Concern.forEntity(u.entity())));

    if (byConcern.size() > 1) {
      // TODO remove this check??
      throw new UnsupportedOperationException(
          "Cannot atomically create entities against multiple targets: " + byConcern.keySet());
    }

    for (var concernChanges : byConcern.entrySet()) {
      var results = performEntityMutations(concernChanges.getKey(), concernChanges.getValue());
      var firstFailure = results.firstFailure();
      if (firstFailure.isPresent()) {
        var failure = firstFailure.get();
        return new EntitiesResult(failure.getReturnStatus(), failure.getExtraInformation());
      }

      return new EntitiesResult(
          results.results().stream()
              .map(EntityResult.class::cast)
              .map(EntityResult::getEntity)
              .collect(Collectors.toList()));
    }

    return new EntitiesResult(List.of());
  }

  DropEntityResult dropEntity(
      PolarisCallContext callCtx,
      PolarisBaseEntity entityToDrop,
      Map<String, String> cleanupProperties,
      boolean cleanup) {
    requireNonNull(entityToDrop);

    LOGGER.atDebug().addArgument(() -> logEntityInfo(entityToDrop)).log("drop entity: {}");

    var results =
        performEntityMutations(
            Concern.forEntity(entityToDrop),
            List.of(new EntityUpdate(EntityUpdate.Operation.DELETE, entityToDrop)));

    var result = results.results().getFirst();
    if (result.isSuccess() && cleanup) {
      // if cleanup, schedule a cleanup task for the entity. do this here, so that drop and
      // scheduling the cleanup task is transactional. Otherwise, we'll be unable to schedule the
      // cleanup task
      var dropped = results.droppedEntities().getFirst();

      PolarisBaseEntity taskEntity =
          new PolarisEntity.Builder()
              .setId(persistence.generateId())
              .setCatalogId(0L)
              .setName("entityCleanup_" + entityToDrop.getId())
              .setType(PolarisEntityType.TASK)
              .setSubType(PolarisEntitySubType.NULL_SUBTYPE)
              .setCreateTimestamp(persistence.currentTimeMillis())
              .build();

      Map<String, String> properties = new HashMap<>();
      properties.put(
          PolarisTaskConstants.TASK_TYPE,
          String.valueOf(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER.typeCode()));
      properties.put("data", PolarisObjectMapperUtil.serialize(callCtx, dropped));
      taskEntity.setPropertiesAsMap(properties);
      if (cleanupProperties != null) {
        taskEntity.setInternalPropertiesAsMap(cleanupProperties);
      }

      try {
        performEntityMutations(
            new Concern(PolarisEntityType.TASK, 0L, false),
            List.of(new EntityUpdate(EntityUpdate.Operation.CREATE, taskEntity)));
        return new DropEntityResult(taskEntity.getId());
      } catch (Exception e) {
        LOGGER.warn("Failed to write cleanup task entity for dropped entity", e);
      }
    }

    return (DropEntityResult) result;
  }

  private MutationResults performEntityMutations(Concern concern, List<EntityUpdate> updates) {
    LOGGER
        .atDebug()
        .addArgument(updates.size())
        .addArgument(concern.entityType())
        .addArgument(concern.catalogId())
        .addArgument(concern.catalogContent() ? "catalog-content" : "non-catalog-content")
        .addArgument(
            () ->
                updates.stream()
                    .map(
                        u ->
                            format(
                                "%s: %s #%s '%s'",
                                u.operation(),
                                u.entity().getType(),
                                u.entity().getId(),
                                u.entity().getName()))
                    .collect(Collectors.joining("\n    ", "\n    ", "")))
        .log("Applying {} updates to {} entities in catalog id {} as {} : {}");

    if (concern.entityType() == PolarisEntityType.ROOT) {
      checkArgument(updates.size() == 1, "Cannot write multiple root entities");
      try {
        var update = updates.getFirst();
        checkArgument(
            update.operation() == EntityUpdate.Operation.CREATE,
            "Cannot update or delete the root entity");
        return persistence
            .createCommitter(RootObj.ROOT_REF_NAME, RootObj.class, MutationResults.class)
            .synchronizingLocally()
            .commitRuntimeException(
                (state, refObjSupplier) -> mutationAttemptForRoot(state, refObjSupplier, update))
            .orElseThrow();
      } finally {
        memoizedIndexedAccess.invalidateIndexedAccess(0L, PolarisEntityType.ROOT.getCode());
      }
    }

    var mutationResults = (MutationResults) null;

    if (concern.catalogContent()) {
      try {
        var committer =
            persistence
                .createCommitter(
                    perCatalogReferenceName(
                        CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN, concern.catalogId()),
                    CatalogStateObj.class,
                    MutationResults.class)
                .synchronizingLocally();
        var commitRetryable =
            new CatalogChangeCommitterWrapper<MutationResults>(
                ((state, ref, byName, byId, changes) ->
                    mutationAttempt(concern, updates, state, byName, byId, changes)));
        mutationResults = committer.commitRuntimeException(commitRetryable).orElseThrow();
      } finally {
        memoizedIndexedAccess.invalidateCatalogContent(concern.catalogId());
      }
    } else {
      mutationResults =
          performChange(
              concern.entityType(),
              containerTypeForEntityType(concern.entityType(), false),
              MutationResults.class,
              concern.catalogId(),
              ((state, ref, byName, byId) ->
                  mutationAttempt(concern, updates, state, byName, byId, null)));
    }

    // TODO populate MutationResults.aclsToRemove and handle those, also need a maintenance
    //  operation to garbage-collect ACL entries for no longer existing entities.

    return mutationResults;
  }

  private ChangeResult<MutationResults> mutationAttempt(
      Concern concern,
      List<EntityUpdate> updates,
      CommitterState<? extends ContainerObj, MutationResults> state,
      UpdatableIndex<ObjRef> byName,
      UpdatableIndex<IndexKey> byId,
      UpdatableIndex<Change> changes) {
    var mutationResults = newMutableMutationResults();
    for (var update : updates) {
      var entity = update.entity();
      var entityType = entity.getType();
      var persistence = state.persistence();
      var now = persistence.currentInstant();
      LOGGER.debug("Processing update {}", update);

      var entityParentId = entity.getParentId();

      var entityIdKey = IndexKey.key(entity.getId());
      var originalNameKey = byId.get(entityIdKey);

      switch (update.operation()) {
        case CREATE -> {
          if (entityType == PolarisEntityType.PRINCIPAL) {
            throw new IllegalArgumentException(
                "Use createPrincipal function instead of writeEntity");
          }

          var entityObjBuilder =
              mapToObj(entity, Optional.empty())
                  .id(persistence.generateId())
                  .createTimestamp(now)
                  .updateTimestamp(now)
                  .entityVersion(1);

          var nameKey = nameKeyForEntity(entity, byId, mutationResults::entityResult);
          if (nameKey == null) {
            break;
          }

          var entityObj = entityObjBuilder.build();
          var existingRef = byName.get(nameKey);
          if (existingRef != null || originalNameKey != null) {
            // PolarisMetaStoreManager.createEntityIfNotExists: if the entity already exists, just
            // return it.
            if (existingRef == null) {
              existingRef = byName.get(originalNameKey);
            }
            if (existingRef != null) {
              var originalObj =
                  (ObjBase)
                      state
                          .persistence()
                          .fetch(
                              existingRef,
                              objTypeForPolarisType(entityType, entity.getSubType()).targetClass());
              if (originalObj != null) {
                var unchangedCompareObj =
                    objForChangeComparison(entity, Optional.empty(), originalObj);
                if (unchangedCompareObj.equals(originalObj)) {
                  mutationResults.entityResultNoChange(entity);
                  break;
                }
              }
            }

            mutationResults.entityResult(
                BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS,
                entitySubTypeCodeFromObjType(existingRef));
            break;
          }

          mutationResults.entityResult(mapToEntity(entityObj, concern.catalogId()));
          state.writeOrReplace("entity-" + entityObj.stableId(), entityObj);

          byName.put(nameKey, objRef(entityObj));
          byId.put(entityIdKey, nameKey);

          if (changes != null) {
            checkState(
                changes.put(nameKey, ChangeAdd.builder().build()),
                "Entity '%s' updated more than once",
                nameKey);
          }

          LOGGER.debug(
              "Added {} '{}' with ID {}...",
              entityObj.type().name(),
              nameKey,
              entityObj.stableId());
        }
        case UPDATE -> {
          if (originalNameKey == null) {
            mutationResults.entityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND);
            break;
          }
          var originalRef = byName.get(originalNameKey);
          if (originalRef == null) {
            mutationResults.entityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND);
            break;
          }
          var originalObj =
              (ObjBase)
                  state
                      .persistence()
                      .fetch(
                          originalRef,
                          objTypeForPolarisType(entityType, entity.getSubType()).targetClass());
          if (originalObj == null) {
            mutationResults.entityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND);
            break;
          }
          if (entity.getEntityVersion() != originalObj.entityVersion()) {
            mutationResults.entityResult(
                BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED);
            break;
          }

          var currentSecrets = maybeObjToPolarisPrincipalSecrets(originalObj);

          var renameOrMove =
              entityParentId != originalObj.parentStableId()
                  || !entity.getName().equals(originalObj.name());

          if (renameOrMove) {
            if (entity.cannotBeDroppedOrRenamed()) {
              mutationResults.entityResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RENAMED);
              break;
            }
            if (!byName.remove(originalNameKey)) {
              mutationResults.entityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND);
              break;
            }

            var newNameKey = nameKeyForEntity(entity, byId, mutationResults::entityResult);
            if (newNameKey == null) {
              break;
            }

            var existingRef = byName.get(newNameKey);
            if (existingRef != null) {
              mutationResults.entityResult(
                  BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS,
                  entitySubTypeCodeFromObjType(existingRef));
              break;
            }

            var entityObj =
                mapToObj(entity, currentSecrets)
                    .id(persistence.generateId())
                    .updateTimestamp(now)
                    .entityVersion(originalObj.entityVersion() + 1)
                    .build();

            state.writeOrReplace("entity-" + entityObj.stableId(), entityObj);

            byName.put(newNameKey, objRef(entityObj));
            byId.put(entityIdKey, newNameKey);
            if (changes != null) {
              checkState(
                  changes.put(
                      newNameKey, ChangeRename.builder().renameFrom(originalNameKey).build()),
                  "Entity '%s' updated more than once",
                  newNameKey);
            }
            mutationResults.entityResult(mapToEntity(entityObj, concern.catalogId()));

            LOGGER.debug(
                "Renamed {} '{}' with ID {} to '{}'...",
                entityType,
                originalNameKey,
                entity.getId(),
                newNameKey);
          } else {
            // no rename/move

            var unchangedCompareObj = objForChangeComparison(entity, currentSecrets, originalObj);
            if (!unchangedCompareObj.equals(originalObj)) {
              var entityObj =
                  mapToObj(entity, currentSecrets)
                      .id(persistence.generateId())
                      .updateTimestamp(now)
                      .entityVersion(originalObj.entityVersion() + 1)
                      .build();

              state.writeOrReplace("entity-" + entityObj.stableId(), entityObj);
              byName.put(originalNameKey, objRef(entityObj));
              if (changes != null) {
                checkState(
                    changes.put(originalNameKey, ChangeUpdate.builder().build()),
                    "Entity '%s' updated more than once",
                    originalNameKey);
              }
              mutationResults.entityResult(mapToEntity(entityObj, concern.catalogId()));

              LOGGER.debug(
                  "Updated {} '{}' with ID {}...", entityType, originalNameKey, entity.getId());
            } else {
              mutationResults.unchangedEntityResult(entity);

              LOGGER.debug(
                  "Not updating {} '{}' with ID {} (no change)...",
                  entityType,
                  originalNameKey,
                  entity.getId());
            }
          }
        }
        case DELETE -> {
          if (originalNameKey == null) {
            mutationResults.dropResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND);
            break;
          }
          var originalRef = byName.get(originalNameKey);
          if (originalRef == null) {
            mutationResults.dropResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND);
            break;
          }
          var originalObj =
              (ObjBase)
                  state
                      .persistence()
                      .fetch(
                          originalRef,
                          objTypeForPolarisType(entityType, entity.getSubType()).targetClass());
          if (originalObj == null) {
            mutationResults.dropResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND);
            break;
          }
          if (entity.getEntityVersion() != originalObj.entityVersion()) {
            mutationResults.dropResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED);
            break;
          }
          if (entity.cannotBeDroppedOrRenamed()) {
            mutationResults.dropResult(BaseResult.ReturnStatus.ENTITY_UNDROPPABLE);
            break;
          }

          var ok =
              switch (entityType) {
                case NAMESPACE -> {
                  if (hasChildren(concern.catalogId(), byName, byId, entity.getId())) {
                    mutationResults.dropResult(BaseResult.ReturnStatus.NAMESPACE_NOT_EMPTY);
                    yield false;
                  }
                  yield true;
                }
                case CATALOG -> {
                  var catalogState = memoizedIndexedAccess.catalogContent(entity.getId());

                  if (catalogState.nameIndex().map(idx -> idx.iterator().hasNext()).orElse(false)) {
                    mutationResults.dropResult(BaseResult.ReturnStatus.NAMESPACE_NOT_EMPTY);
                    yield false;
                  }

                  // VALIDATION LOGIC COPIED

                  var catalogRolesAccess =
                      memoizedIndexedAccess.indexedAccess(
                          entity.getId(), PolarisEntityType.CATALOG_ROLE.getCode());
                  var numCatalogRoles =
                      catalogRolesAccess
                          .nameIndex()
                          .map(
                              idx -> {
                                var iter = idx.iterator();
                                var cnt = 0;
                                if (iter.hasNext()) {
                                  iter.next();
                                  cnt++;
                                }
                                if (iter.hasNext()) {
                                  iter.next();
                                  cnt++;
                                }
                                return cnt;
                              })
                          .orElse(0);

                  // if we have 2, we cannot drop the catalog. If only one left, better be the admin
                  // role
                  if (numCatalogRoles > 1) {
                    mutationResults.dropResult(BaseResult.ReturnStatus.CATALOG_NOT_EMPTY);
                    yield false;
                  }
                  // if 1, drop the last catalog role. Should be the catalog admin role but don't
                  // validate this
                  // (note: no need to drop the catalog role here, it'll be eventually done by
                  // persistence-maintenance!)

                  yield true;
                }
                default -> true;
              };
          if (ok) {
            byId.remove(entityIdKey);
            byName.remove(requireNonNull(originalNameKey));
            mutationResults.dropResult(entity);

            if (changes != null) {
              changes.put(originalNameKey, ChangeRemove.builder().build());
            }
          }
        }
        default -> throw new IllegalStateException("Unexpected operation " + update.operation());
      }
    }

    var doCommit = mutationResults.anyChange && !mutationResults.hardFailure;
    LOGGER.debug(
        "{} changes (has changes: {}, failures: {})",
        doCommit ? "Committing" : "Not committing",
        mutationResults.anyChange,
        mutationResults.failuresAsString());

    return doCommit
        ? new ChangeResult.CommitChange<>(mutationResults)
        : new ChangeResult.NoChange<>(mutationResults);
  }

  private static IndexKey nameKeyForEntity(
      PolarisEntityCore entity,
      UpdatableIndex<IndexKey> byId,
      Consumer<BaseResult.ReturnStatus> errorHandler) {
    var identifierBuilder = Identifier.builder();
    var entityParentId = entity.getParentId();
    if (entityParentId != 0L && entityParentId != entity.getCatalogId()) {
      var parentNameKey = byId.get(IndexKey.key(entityParentId));
      if (parentNameKey == null) {
        errorHandler.accept(BaseResult.ReturnStatus.ENTITY_NOT_FOUND);
        return null;
      }
      indexKeyToIdentifierBuilder(parentNameKey, identifierBuilder);
    }
    identifierBuilder.addElements(entity.getName());
    var identifier = identifierBuilder.build();
    return identifierToIndexKey(identifier);
  }

  private static ObjBase objForChangeComparison(
      PolarisBaseEntity entity,
      Optional<PolarisPrincipalSecrets> currentSecrets,
      ObjBase originalObj) {
    return mapToObj(entity, currentSecrets)
        .updateTimestamp(originalObj.createTimestamp())
        .id(originalObj.id())
        .numParts(originalObj.numParts())
        .entityVersion(originalObj.entityVersion())
        .createTimestamp(originalObj.createTimestamp())
        .build();
  }

  private static Optional<RootObj> mutationAttemptForRoot(
      CommitterState<RootObj, MutationResults> state,
      Supplier<Optional<RootObj>> refObjSupplier,
      EntityUpdate update) {
    var entity = update.entity();
    var ref = TypeMapping.<RootObj, RootObj.Builder>mapToObj(entity, Optional.empty());
    var refObj = refObjSupplier.get();
    return switch (update.operation()) {
      case CREATE -> {
        if (refObj.isPresent()) {
          yield state.noCommit(singleEntityResult(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS));
        }
        yield state.commitResult(singleEntityResult(entity), ref, refObj);
      }
      case UPDATE -> {
        if (refObj.isPresent()) {
          var rootObj = refObj.get();
          if (entity.getEntityVersion() != rootObj.entityVersion()) {
            yield state.noCommit(
                singleEntityResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED));
          }
        }
        yield state.commitResult(singleEntityResult(entity), ref, refObj);
      }
      default -> throw new IllegalStateException("Unexpected operation " + update.operation());
    };
  }

  @Override
  public void writeToGrantRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {
    throw unimplemented();
  }

  @Override
  public void deleteEntity(@Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    throw unimplemented();
  }

  @Override
  public void deleteFromGrantRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {
    throw unimplemented();
  }

  @Override
  public void deleteAllEntityGrantRecords(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore entity,
      @Nonnull List<PolarisGrantRecord> grantsOnGrantee,
      @Nonnull List<PolarisGrantRecord> grantsOnSecurable) {
    throw unimplemented();
  }

  @Nullable
  @Override
  public PolarisBaseEntity lookupEntity(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId, int entityTypeCode) {
    if (entityTypeCode == PolarisEntityType.ROOT.getCode()) {
      return (PolarisEntityConstants.getNullId() == catalogId
              && entityId == PolarisEntityConstants.getRootEntityId())
          ? lookupRoot().orElseThrow()
          : null;
    }
    if (entityTypeCode == PolarisEntityType.NULL_TYPE.getCode()) {
      return null;
    }
    if (entityTypeCode == PolarisEntityType.CATALOG.getCode()) {
      catalogId = 0L;
    }

    var access = memoizedIndexedAccess.indexedAccess(catalogId, entityTypeCode);
    var resolved = access.byId(entityId);

    LOGGER.debug(
        "lookupEntity result: entityTypeCode: {}, catalogId: {}, entityId: {} : {}",
        entityTypeCode,
        catalogId,
        entityId,
        resolved);

    return resolved.map(objBase -> mapToEntity(objBase, access.catalogStableId())).orElse(null);
  }

  @Nullable
  @Override
  public PolarisBaseEntity lookupEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int entityTypeCode,
      @Nonnull String name) {
    if (entityTypeCode == PolarisEntityType.ROOT.getCode()) {
      return (PolarisEntityConstants.getNullId() == catalogId
              && parentId == PolarisEntityConstants.getRootEntityId()
              && PolarisEntityConstants.getRootContainerName().equals(name))
          ? lookupRoot().orElseThrow()
          : null;
    }
    if (entityTypeCode == PolarisEntityType.NULL_TYPE.getCode()) {
      return null;
    }
    if (entityTypeCode == PolarisEntityType.CATALOG.getCode()) {
      catalogId = 0L;
    }

    var rootAccess = parentId == catalogId;
    var access = memoizedIndexedAccess.indexedAccess(catalogId, entityTypeCode);
    var resolved =
        rootAccess ? access.byNameOnRoot(name) : access.byParentIdAndName(parentId, name);

    LOGGER.debug(
        "lookupEntityByName result : entityTypeCode: {}, catalogId: {}, parentId: {}, name: {} : {}",
        entityTypeCode,
        catalogId,
        parentId,
        name,
        resolved);

    return resolved.map(objBase -> mapToEntity(objBase, access.catalogStableId())).orElse(null);
  }

  Optional<PolarisBaseEntity> lookupRoot() {
    return memoizedIndexedAccess
        .indexedAccess(0L, PolarisEntityType.ROOT.getCode())
        .byId(0L)
        .map(root -> TypeMapping.mapToEntity(root, 0L));
  }

  @Override
  public EntityNameLookupRecord lookupEntityIdAndSubTypeByName(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name) {
    if (typeCode == PolarisEntityType.NULL_TYPE.getCode()) {
      return null;
    }
    if (typeCode == PolarisEntityType.CATALOG.getCode()) {
      catalogId = 0L;
    }
    var ent = lookupEntityByName(callCtx, catalogId, parentId, typeCode, name);
    return ent != null ? new EntityNameLookupRecord(ent) : null;
  }

  @Nonnull
  @Override
  public List<PolarisBaseEntity> lookupEntities(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    throw unimplemented();
  }

  @Nonnull
  @Override
  public List<PolarisChangeTrackingVersions> lookupEntityVersions(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    throw unimplemented();
  }

  @Override
  public boolean hasChildren(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityType optionalEntityType,
      long catalogId,
      long parentId) {
    checkArgument(catalogId != 0L, "Must be called on a catalog");
    var access = memoizedIndexedAccess.catalogContent(catalogId);

    var nameIndex = access.nameIndex().orElse(null);
    var idIndex = access.stableIdIndex().orElse(null);
    return hasChildren(catalogId, nameIndex, idIndex, parentId);
  }

  boolean hasChildren(
      long catalogId, Index<ObjRef> nameIndex, Index<IndexKey> stableIdIndex, long parentId) {
    if (nameIndex != null) {
      if (parentId == 0L || parentId == catalogId) {
        return nameIndex.iterator().hasNext();
      } else {
        var parentNameKey =
            stableIdIndex != null ? stableIdIndex.get(IndexKey.key(parentId)) : null;
        if (parentNameKey != null) {
          var iter = nameIndex.iterator(parentNameKey, null, false);
          // skip the parent itself
          iter.next();
          if (iter.hasNext()) {
            var e = iter.next();
            var nextKey = e.getKey();
            var parentIdent = indexKeyToIdentifier(parentNameKey);
            var nextIdent = indexKeyToIdentifier(nextKey);
            return nextIdent.parent().equals(parentIdent);
          }
        }
      }
    }
    return false;
  }

  Stream<ObjBase> listEntitiesStream(
      long catalogStableId,
      long parentId,
      PolarisEntityType entityType,
      PolarisEntitySubType entitySubType,
      long limit) {

    LOGGER.debug(
        "listEntitiesStream, catalogId: {}, parentId: {}, entityType: {}, limit: {}",
        catalogStableId,
        parentId,
        entityType,
        limit);

    if (entityType == PolarisEntityType.NULL_TYPE) {
      return Stream.empty();
    }
    if (entityType == PolarisEntityType.CATALOG) {
      catalogStableId = 0L;
    }

    var catalogContent = isCatalogContent(entityType);
    var access =
        catalogContent
            ? memoizedIndexedAccess.catalogContent(catalogStableId)
            : memoizedIndexedAccess.indexedAccess(catalogStableId, entityType.getCode());
    var nameIndex = access.nameIndex().orElse(null);

    if (nameIndex != null) {
      var objRefs = Stream.<Map.Entry<IndexKey, ObjRef>>empty();
      if (catalogStableId != 0L) {
        if (parentId == 0L || parentId == catalogStableId) {
          // list on catalog root
          objRefs =
              Streams.stream(nameIndex.iterator())
                  .filter(
                      e -> {
                        var ident = indexKeyToIdentifier(e.getKey());
                        return ident.elements().size() == 1;
                      });
        } else {
          // list on namespace
          var prefixKeyOptional = access.nameKeyById(parentId);
          if (prefixKeyOptional.isPresent()) {
            var prefixKey = prefixKeyOptional.get();
            var prefix = indexKeyToIdentifier(prefixKey);
            var prefixElems = prefix.elements();
            var directChildLevel = prefixElems.size() + 1;
            objRefs =
                Streams.stream(nameIndex.iterator(prefixKey, null, false))
                    .takeWhile(
                        e -> {
                          var ident = indexKeyToIdentifier(e.getKey());
                          var identElems = ident.elements();
                          if (identElems.size() < prefixElems.size() + 1) {
                            return ident.equals(prefix);
                          }
                          return identElems.subList(0, prefixElems.size()).equals(prefixElems);
                        })
                    .filter(
                        e -> {
                          var ident = indexKeyToIdentifier(e.getKey());
                          return ident.elements().size() == directChildLevel;
                        });
          }
        }
      } else {
        objRefs = Streams.stream(nameIndex.iterator());
      }

      objRefs = objRefs.limit(limit);

      if (LOGGER.isDebugEnabled()) {
        objRefs =
            objRefs.peek(
                o ->
                    LOGGER.debug(
                        "  listEntitiesStream (before type filter): {} : {}",
                        o.getKey(),
                        o.getValue()));
      }

      var filterType = objTypeForPolarisTypeForFiltering(entityType, entitySubType);
      objRefs =
          objRefs.filter(
              o ->
                  filterType.isAssignableFrom(
                      ObjTypes.objTypeById(o.getValue().type()).targetClass()));

      // TODO fetch in small batches
      var objs =
          persistence.fetchMany(
              ObjBase.class, objRefs.map(Map.Entry::getValue).toArray(ObjRef[]::new));

      return Arrays.stream(objs);
    }

    return Stream.empty();
  }

  ListEntitiesResult listEntities(
      List<PolarisEntityCore> catalogPath,
      PolarisEntityType entityType,
      PolarisEntitySubType entitySubType) {
    var catalogStableId =
        (catalogPath != null && !catalogPath.isEmpty()) ? catalogPath.getFirst().getId() : 0L;

    var parentId =
        (catalogPath != null && catalogPath.size() > 1) ? catalogPath.getLast().getId() : 0L;

    var entities =
        listEntitiesStream(catalogStableId, parentId, entityType, entitySubType, Long.MAX_VALUE)
            .map(o -> mapToEntityNameLookupRecord(o, catalogStableId))
            // TODO make PolarisTestMetaStoreManager handle non-mutable list :(
            .collect(Collectors.toCollection(ArrayList::new));

    return new ListEntitiesResult(entities);
  }

  @Nonnull
  @Override
  public <T> List<T> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      int limit,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull Function<PolarisBaseEntity, T> transformer) {
    return listEntitiesStream(
            catalogId,
            parentId,
            entityType,
            PolarisEntitySubType.ANY_SUBTYPE,
            limit <= 0 ? Long.MAX_VALUE : limit)
        .map(o -> mapToEntity(o, catalogId))
        .filter(entityFilter)
        .map(transformer)
        .toList();
  }

  @Nonnull
  @Override
  public List<EntityNameLookupRecord> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType) {
    return listEntities(callCtx, catalogId, parentId, entityType, entity -> true);
  }

  @Nonnull
  @Override
  public List<EntityNameLookupRecord> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter) {
    return listEntities(
        callCtx,
        catalogId,
        parentId,
        entityType,
        -1,
        entityFilter,
        entity ->
            new EntityNameLookupRecord(
                entity.getCatalogId(),
                entity.getId(),
                entity.getParentId(),
                entity.getName(),
                entity.getTypeCode(),
                entity.getSubTypeCode()));
  }

  PolicyAttachmentResult attachDetachPolicyOnEntity(
      @Nonnull PolicyEntity policy,
      @Nonnull PolarisEntityCore target,
      boolean doAttach,
      @Nonnull Map<String, String> parameters) {

    // TODO Alternative approach 1:
    //  - separate reference, similar to grants
    //  - key by entity:
    //    - E/entityCatalogId/entityId/policyType/policyCatalogId/policyId
    //      VALUE: properties
    //  - key by policy:
    //    - P/policyType/policyCatalogId/policyId/entityCatalogId/entityId/entityType
    //      VALUE: (empty)
    //  - remove PolicyAttachableContentObj
    //  --> code should become simpler
    //  --> add to maintenance

    try {
      var committer =
          persistence
              .createCommitter(
                  POLICY_MAPPINGS_REF_NAME, PolicyMappingsObj.class, PolicyAttachmentResult.class)
              .synchronizingLocally();
      return committer
          .commitRuntimeException(
              (state, refObjSupplier) -> {
                var refObj = refObjSupplier.get();
                var index =
                    refObj
                        .map(
                            ref ->
                                ref.policyMappings()
                                    .asUpdatableIndex(
                                        state.persistence(), POLICY_MAPPING_SERIALIZER))
                        .orElseGet(
                            () ->
                                IndexContainer.newUpdatableIndex(
                                    persistence, POLICY_MAPPING_SERIALIZER));
                var builder = PolicyMappingsObj.builder();
                refObj.ifPresent(builder::from);

                var policyCatalogAccess =
                    memoizedIndexedAccess.catalogContent(policy.getCatalogId());
                var targetCatalogAccess =
                    target.getCatalogId() == policy.getCatalogId()
                        ? policyCatalogAccess
                        : memoizedIndexedAccess.catalogContent(target.getCatalogId());

                var policyOptional = policyCatalogAccess.byId(policy.getId());
                if (policyOptional.isEmpty()) {
                  return state.noCommit(
                      new PolicyAttachmentResult(
                          BaseResult.ReturnStatus.POLICY_MAPPING_NOT_FOUND, null));
                }
                var targetOptional = targetCatalogAccess.byId(target.getId());
                if (targetOptional.isEmpty()) {
                  return state.noCommit(
                      new PolicyAttachmentResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null));
                }

                var result =
                    new PolicyAttachmentResult(
                        new PolarisPolicyMappingRecord(
                            target.getCatalogId(),
                            target.getId(),
                            policy.getCatalogId(),
                            policy.getId(),
                            policy.getPolicyTypeCode(),
                            parameters));

                var keyByPolicy =
                    new PolicyMappingsObj.KeyByPolicy(
                        policy.getCatalogId(),
                        policy.getId(),
                        policy.getPolicyTypeCode(),
                        target.getCatalogId(),
                        target.getId());
                var keyByEntity =
                    new PolicyMappingsObj.KeyByEntity(
                        target.getCatalogId(),
                        target.getId(),
                        policy.getPolicyTypeCode(),
                        policy.getCatalogId(),
                        policy.getId());

                var changed = false;
                if (doAttach) {
                  if (policy.getPolicyType().isInheritable()) {
                    // Contract says that at max one policy of the same inheritable policy type must
                    // be attached to a single entity.
                    var policyPrefixKey = keyByEntity.toPolicyTypePartialIndexKey();
                    var iter = index.iterator(policyPrefixKey, policyPrefixKey, false);
                    if (iter.hasNext()) {
                      // same policy-type attached, error out
                      return state.noCommit(
                          new PolicyAttachmentResult(
                              POLICY_MAPPING_OF_SAME_TYPE_ALREADY_EXISTS, null));
                    }
                  }

                  // note: parameters are only added to the "by entity" entry
                  changed |= index.put(keyByPolicy.toIndexKey(), PolicyMapping.EMPTY);
                  changed |=
                      index.put(
                          keyByEntity.toIndexKey(),
                          PolicyMapping.builder().parameters(parameters).build());
                } else {
                  changed |= index.remove(keyByPolicy.toIndexKey());
                  changed |= index.remove(keyByEntity.toIndexKey());
                }

                if (changed) {
                  builder.policyMappings(index.toIndexed("mappings", state::writeOrReplace));
                  return state.commitResult(result, builder, refObj);
                }
                return state.noCommit(result);
              })
          .orElseThrow();
    } finally {
      memoizedIndexedAccess.invalidateReferenceHead(POLICY_MAPPINGS_REF_NAME);
    }
  }

  LoadPolicyMappingsResult loadPoliciesOnEntity(
      PolarisEntityCore target, Optional<PolicyType> policyType) {
    if (!isCatalogContent(target.getType())) {
      return new LoadPolicyMappingsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    return memoizedIndexedAccess
        .referenceHead(POLICY_MAPPINGS_REF_NAME, PolicyMappingsObj.class)
        .map(
            policyMappingsObj -> {
              var mappingRecords = new ArrayList<PolarisPolicyMappingRecord>();
              var policyEntities = new ArrayList<PolarisBaseEntity>();
              var seenPolicies = new HashSet<Long>();

              var index =
                  policyMappingsObj
                      .policyMappings()
                      .indexForRead(persistence, POLICY_MAPPING_SERIALIZER);

              // (Partial) index-key for the lookup
              var keyByEntityTemplate =
                  new PolicyMappingsObj.KeyByEntity(
                      target.getCatalogId(),
                      target.getId(),
                      policyType.map(PolicyType::getCode).orElse(0),
                      0L,
                      0L);

              // Construct the prefix-key, depending on whether to look for all attached policies or
              // attached policies having the given policy-type
              var prefixKey =
                  policyType.isPresent()
                      ? keyByEntityTemplate.toPolicyTypePartialIndexKey()
                      : keyByEntityTemplate.toEntityPartialIndexKey();

              for (var iter = index.iterator(prefixKey, prefixKey, false); iter.hasNext(); ) {
                var elem = iter.next();
                var key = PolicyMappingsObj.PolicyMappingKey.fromIndexKey(elem.getKey());
                if (key instanceof PolicyMappingsObj.KeyByEntity byEntity) {
                  if (seenPolicies.add(byEntity.policyId())) {
                    memoizedIndexedAccess
                        .catalogContent(byEntity.policyCatalogId())
                        .byId(byEntity.policyId())
                        .map(obj -> mapToEntity(obj, byEntity.policyCatalogId()))
                        .ifPresent(policyEntities::add);
                  }
                  mappingRecords.add(byEntity.toMappingRecord(elem.getValue()));
                } else {
                  // not what we're looking for (should actually never happen due to the prefix-key
                  break;
                }
              }

              return new LoadPolicyMappingsResult(mappingRecords, policyEntities);
            })
        .orElse(new LoadPolicyMappingsResult(List.of(), List.of()));
  }

  @Override
  public int lookupEntityGrantRecordsVersion(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId) {
    throw useMetaStoreManager("loadGrantsOnSecurable");
  }

  @Nullable
  @Override
  public PolarisGrantRecord lookupGrantRecord(
      @Nonnull PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    throw useMetaStoreManager("loadGrantsOnSecurable");
  }

  @Nonnull
  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      @Nonnull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    throw useMetaStoreManager("loadGrantsOnSecurable");
  }

  @Nonnull
  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      @Nonnull PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    throw useMetaStoreManager("loadGrantsToGrantee");
  }

  @FunctionalInterface
  interface AclEntryHandler {
    void handle(SecurableAndGrantee securableAndGrantee, PrivilegeSet granted);
  }

  List<PolarisGrantRecord> allGrantRecords(PolarisBaseEntity entity) {
    var catalogId = entity.getCatalogId();
    var aclName = GrantTriplet.forEntity(entity).toRoleName();

    LOGGER.debug("allGrantRecords for {}", aclName);

    var collector = new GrantRecordsCollector(catalogId);

    collectGrantRecords(
        catalogId,
        aclName,
        (securableAndGrantee, granted) -> {
          var indexedAccess =
              memoizedIndexedAccess.indexedAccess(
                  securableAndGrantee.securableCatalogId(),
                  securableAndGrantee.securableTypeCode());
          var existing = indexedAccess.nameKeyById(securableAndGrantee.securableId());
          if (existing.isPresent()) {
            collector.handle(securableAndGrantee, granted);
          }
        });

    var grantRecords = collector.grantRecords;
    LOGGER
        .atTrace()
        .addArgument(grantRecords.size())
        .addArgument(
            () ->
                grantRecords.stream()
                    .map(PolarisGrantRecord::toString)
                    .collect(Collectors.joining("\n    ", "\n    ", "")))
        .log("Returning {} grant records: {}");
    return grantRecords;
  }

  LoadGrantsResult loadGrants(long catalogId, long id, int entityTypeCode, boolean onSecurable) {
    LOGGER.debug(
        "loadGrants on {} for catalog:{}, id:{}, entityType:{}({})",
        onSecurable ? "securable" : "grantee",
        catalogId,
        id,
        PolarisEntityType.fromCode(entityTypeCode),
        entityTypeCode);
    var aclName = new GrantTriplet(true, catalogId, id, entityTypeCode).toRoleName();

    var collector = new GrantRecordsCollector(catalogId);
    var entities = new ArrayList<PolarisBaseEntity>();
    var ids = new HashSet<Long>();

    collectGrantRecords(
        catalogId,
        aclName,
        ((securableAndGrantee, granted) -> {
          var targetCatalogId =
              onSecurable
                  ? securableAndGrantee.granteeCatalogId()
                  : securableAndGrantee.securableCatalogId();
          var targetId =
              onSecurable ? securableAndGrantee.granteeId() : securableAndGrantee.securableId();
          var targetTypeCode =
              onSecurable
                  ? securableAndGrantee.granteeTypeCode()
                  : securableAndGrantee.securableTypeCode();

          var indexedAccess = memoizedIndexedAccess.indexedAccess(targetCatalogId, targetTypeCode);
          var entityOptional =
              indexedAccess.byId(targetId).map(o -> mapToEntity(o, targetCatalogId));

          if (entityOptional.isPresent()) {
            PolarisBaseEntity entity = entityOptional.get();
            LOGGER.trace(
                "    Adding entity to load-grants-result: catalog:{}, id:{}, type:{}",
                entity.getCatalogId(),
                entity.getId(),
                entity.getType());
            collector.handle(securableAndGrantee, granted);
            if (ids.add(targetId)) {
              entities.add(entity);
            }
          } else {
            LOGGER.trace("    Not returning stale entity reference");
          }
        }));

    LOGGER.trace(
        "Returning {} grant records for loadGrants for catalog:{}, id:{}, entityType:{}({})",
        collector.grantRecords.size(),
        catalogId,
        id,
        PolarisEntityType.fromCode(entityTypeCode),
        entityTypeCode);

    return new LoadGrantsResult(1, collector.grantRecords, entities);
  }

  private void collectGrantRecords(
      long catalogStableId, String aclName, AclEntryHandler aclEntryConsumer) {
    var refName = grantsRefName(catalogStableId);

    LOGGER.debug("Checking ACL '{}' on '{}'", aclName, refName);

    var head = memoizedIndexedAccess.referenceHead(refName, GrantsObj.class);
    if (head.isPresent()) {
      var grantsObj = head.get();
      var securablesIndex = grantsObj.acls().indexForRead(persistence, OBJ_REF_SERIALIZER);
      var securableKey = IndexKey.key(aclName);

      LOGGER.trace("Processing existing ACL {}", aclName);
      Optional.ofNullable(securablesIndex.get(securableKey))
          .flatMap(aclObjRef -> Optional.ofNullable(persistence.fetch(aclObjRef, AclObj.class)))
          .ifPresent(
              aclObj -> {
                var acl = aclObj.acl();

                acl.forEach(
                    (role, entry) -> {
                      var triplet = GrantTriplet.fromRoleName(role);
                      LOGGER
                          .atTrace()
                          .setMessage("  ACL has securable {} ({}) with privileges {}")
                          .addArgument(role)
                          .addArgument(PolarisEntityType.fromCode(triplet.typeCode()))
                          .addArgument(
                              () ->
                                  entry.granted().stream()
                                      .map(Privilege::name)
                                      .collect(Collectors.joining(", ")))
                          .log();

                      var securableAndGrantee =
                          SecurableAndGrantee.forTriplet(catalogStableId, aclObj, triplet);
                      aclEntryConsumer.handle(securableAndGrantee, entry.granted());
                    });
              });
    } else {
      LOGGER.trace("ACL {} does not exist", aclName);
    }
  }

  static class GrantRecordsCollector implements AclEntryHandler {
    final List<PolarisGrantRecord> grantRecords = new ArrayList<>();
    final long catalogId;

    GrantRecordsCollector(long catalogId) {
      this.catalogId = catalogId;
    }

    @Override
    public void handle(SecurableAndGrantee securableAndGrantee, PrivilegeSet granted) {
      for (var privilege : granted) {
        var privilegeCode = PolarisPrivilege.valueOf(privilege.name()).getCode();
        var record = securableAndGrantee.grantRecordForPrivilege(privilegeCode);
        LOGGER.trace(
            "   Yielding grant record: securable: catalog:{} id:{} - grantee: catalog:{} id:{} - privilege: {}",
            record.getSecurableCatalogId(),
            record.getSecurableId(),
            record.getGranteeCatalogId(),
            record.getGranteeId(),
            record.getPrivilegeCode());
        grantRecords.add(record);
      }
    }
  }

  private static String grantsRefName(long catalogStableId) {
    /*
    TODO better move catalog-related ACLs to the catalog-grants (needs extensive testing!)

    return catalogStableId != 0L
        ? perCatalogReferenceName(CATALOG_GRANTS_REF_NAME_PATTERN, catalogStableId)
        : REALM_GRANTS_REF_NAME;
    */
    return REALM_GRANTS_REF_NAME;
  }

  void persistGrantsOrRevokes(long catalogStableId, boolean grant, Grant... grants) {
    /*
    TODO better move catalog-related ACLs to the catalog-grants (needs extensive testing!)

    if (catalogStableId != 0L) {
      doPersistGrantsOrRevokes(
          perCatalogReferenceName(
              CatalogGrantsObj.CATALOG_GRANTS_REF_NAME_PATTERN, catalogStableId),
          CatalogGrantsObj::builder,
          grant,
          grants);
      return;
    }
    */

    doPersistGrantsOrRevokes(REALM_GRANTS_REF_NAME, RealmGrantsObj::builder, grant, grants);
  }

  private <O extends GrantsObj, B extends GrantsObj.Builder<O, B>> void doPersistGrantsOrRevokes(
      String refName, Supplier<B> builder, boolean doGrant, Grant... grants) {
    LOGGER.debug(
        "Persisting {} on '{}' for '{}'",
        doGrant ? "grants" : "revokes",
        refName,
        Arrays.asList(grants));

    try {
      persistence
          .createCommitter(refName, GrantsObj.class, String.class)
          .synchronizingLocally()
          .commitRuntimeException(
              new CommitRetryable<>() {
                @Nonnull
                @Override
                public Optional<GrantsObj> attempt(
                    @Nonnull CommitterState<GrantsObj, String> state,
                    @Nonnull Supplier<Optional<GrantsObj>> refObjSupplier)
                    throws CommitException {
                  var persistence = state.persistence();
                  var refObj = refObjSupplier.get();

                  var ref = builder.get();
                  refObj.ifPresent(ref::from);

                  var securablesIndex =
                      refObj
                          .map(GrantsObj::acls)
                          .map(c -> c.asUpdatableIndex(persistence, OBJ_REF_SERIALIZER))
                          .orElseGet(() -> newUpdatableIndex(persistence, OBJ_REF_SERIALIZER));

                  for (var g : grants) {
                    var securable = GrantTriplet.forEntity(g.securable());
                    var grantee = GrantTriplet.forEntity(g.grantee());
                    var forSec = grantee.asDirected();
                    var privilege = privileges.byName(g.privilege().name());
                    processGrant(state, securable, forSec, securablesIndex, privilege, doGrant);
                    processGrant(state, grantee, securable, securablesIndex, privilege, doGrant);
                  }

                  ref.acls(securablesIndex.toIndexed("idx-sec-", state::writeOrReplace));

                  return commitResult(state, ref, refObj);
                }

                // Some fun with Java generics...
                @SuppressWarnings({"unchecked", "rawtypes"})
                private static <REF_BUILDER> Optional<GrantsObj> commitResult(
                    CommitterState<GrantsObj, String> state,
                    REF_BUILDER ref,
                    Optional<GrantsObj> refObj) {
                  var cs = (CommitterState) state;
                  var refBuilder = (BaseCommitObj.Builder) ref;
                  return cs.commitResult("", refBuilder, refObj);
                }

                private void processGrant(
                    CommitterState<? extends GrantsObj, String> state,
                    GrantTriplet aclTriplet,
                    GrantTriplet grantee,
                    UpdatableIndex<ObjRef> securablesIndex,
                    Privilege privilege,
                    boolean doGrant) {

                  var aclName = aclTriplet.toRoleName();
                  var granteeRoleName = grantee.toRoleName();

                  LOGGER.trace(
                      "{} {} {} '{}' ({}) on '{}' in ACL '{}' ({})",
                      doGrant ? "Granting" : "Revoking",
                      privilege.name(),
                      doGrant ? "on" : "from",
                      granteeRoleName,
                      PolarisEntityType.fromCode(grantee.typeCode()),
                      refName,
                      aclName,
                      PolarisEntityType.fromCode(aclTriplet.typeCode()));

                  var aclKey = IndexKey.key(aclName);

                  var aclRef = securablesIndex.get(aclKey);
                  var aclObjOptional =
                      Optional.ofNullable(aclRef)
                          .map(r -> state.persistence().fetch(r, AclObj.class));
                  var aclObjBuilder =
                      aclObjOptional
                          .map(AclObj.builder()::from)
                          .orElseGet(AclObj::builder)
                          .id(persistence.generateId())
                          .securableId(aclTriplet.id())
                          .securableTypeCode(aclTriplet.typeCode());

                  var aclBuilder =
                      aclObjOptional
                          .map(o -> privileges.newAclBuilder().from(o.acl()))
                          .orElseGet(privileges::newAclBuilder);

                  aclBuilder.modify(
                      granteeRoleName,
                      aclEntryBuilder -> {
                        if (doGrant) {
                          aclEntryBuilder.grant(privilege);
                        } else {
                          aclEntryBuilder.revoke(privilege);
                        }
                      });

                  aclObjBuilder.acl(aclBuilder.build());

                  var aclObj = aclObjBuilder.build();

                  state.writeOrReplace("acl-" + aclTriplet.id(), aclObj);

                  securablesIndex.put(aclKey, objRef(aclObj));
                }
              });
    } finally {
      memoizedIndexedAccess.invalidateReferenceHead(refName);
    }
  }

  @Nullable
  @Override
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegration(
          @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    var storageConfig = BaseMetaStoreManager.extractStorageConfiguration(callCtx, entity);
    return storageIntegrationProvider.getStorageIntegrationForConfig(storageConfig);
  }

  @Override
  public <T extends PolarisStorageConfigurationInfo> void persistStorageIntegrationIfNeeded(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      @Nullable PolarisStorageIntegration<T> storageIntegration) {
    // Noop - no clue what this shall do!?
  }

  @Nullable
  @Override
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> createStorageIntegration(
          @Nonnull PolarisCallContext callCtx,
          long catalogId,
          long entityId,
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    return storageIntegrationProvider.getStorageIntegrationForConfig(
        polarisStorageConfigurationInfo);
  }

  @Override
  public void deletePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId, long principalId) {
    throw unimplemented();
  }

  CreatePrincipalResult createPrincipal(
      PolarisBaseEntity principal, RootCredentialsSet rootCredentialsSet) {
    LOGGER.atDebug().addArgument(() -> logEntityInfo(principal)).log("createPrincipal {}");

    return performPrincipalChange(
        CreatePrincipalResult.class,
        (state, ref, byName, byId, byClientId) -> {
          var principalName = principal.getName();
          var principalId = principal.getId();
          var nameKey = IndexKey.key(principalName);
          var persistence = state.persistence();

          var existingPrincipal =
              Optional.ofNullable(byName.get(nameKey))
                  .map(objRef -> persistence.fetch(objRef, PrincipalObj.class));
          if (existingPrincipal.isPresent()) {
            var existing = existingPrincipal.get();
            var secrets = principalObjToPolarisPrincipalSecrets(existing);
            var forComparison = objForChangeComparison(principal, Optional.of(secrets), existing);
            return new ChangeResult.NoChange<>(
                existing.equals(forComparison)
                    ? new CreatePrincipalResult(principal, secrets)
                    : new CreatePrincipalResult(
                        BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, null));
          }

          LOGGER.debug("Creating principal '{}' ...", principalName);

          PolarisPrincipalSecrets newPrincipalSecrets;
          while (true) {
            newPrincipalSecrets =
                secretsGenerator(rootCredentialsSet).produceSecrets(principalName, principalId);
            var newClientId = newPrincipalSecrets.getPrincipalClientId();
            if (byClientId.get(IndexKey.key(newClientId)) == null) {
              LOGGER.debug("Generated secrets for principal '{}' ...", principalName);
              break;
            }
          }

          var now = persistence.currentInstant();
          // Map from the given entity to retain properties + internal-properties (e.g.
          // PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)
          var updatedPrincipalBuilder =
              mapToObj(principal, Optional.of(newPrincipalSecrets))
                  .name(principalName)
                  .stableId(principalId)
                  .entityVersion(1)
                  .createTimestamp(now)
                  .updateTimestamp(now)
                  .id(persistence.generateId());
          var updatedPrincipal = updatedPrincipalBuilder.build();

          var updatedPrincipalObjRef = objRef(updatedPrincipal);
          byClientId.put(
              IndexKey.key(newPrincipalSecrets.getPrincipalClientId()), updatedPrincipalObjRef);
          byName.put(nameKey, updatedPrincipalObjRef);
          byId.put(IndexKey.key(principalId), nameKey);

          state.writeOrReplace("principal", updatedPrincipal);

          // return those
          return new ChangeResult.CommitChange<>(
              new CreatePrincipalResult(
                  mapToEntity(updatedPrincipal, 0L),
                  principalObjToPolarisPrincipalSecrets(
                      (PrincipalObj) updatedPrincipal, newPrincipalSecrets)));
        });
  }

  PrincipalSecretsResult rotatePrincipalSecrets(
      String clientId, long principalId, boolean reset, String oldSecretHash) {
    LOGGER.debug(
        "rotatePrincipalSecrets '{}', principalId: {}, reset: {}", clientId, principalId, reset);

    return performPrincipalChange(
        PrincipalSecretsResult.class,
        (state, ref, byName, byId, byClientId) -> {
          var clientIdKey = IndexKey.key(clientId);
          var principalObjRef = byClientId.get(clientIdKey);
          var persistence = state.persistence();
          if (principalObjRef == null) {
            return new ChangeResult.NoChange<>(
                new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null));
          }

          var principal = persistence.fetch(principalObjRef, PrincipalObj.class);
          checkState(principal != null, "Principal not found");
          checkState(principalId == principal.stableId(), "Principal id mismatch");

          // load the existing secrets
          var principalSecrets = principalObjToPolarisPrincipalSecrets(principal);

          // rotate the secrets
          principalSecrets.rotateSecrets(oldSecretHash);
          if (reset) {
            principalSecrets.rotateSecrets(principalSecrets.getMainSecretHash());
          }

          var updatedPrincipalBuilder =
              PrincipalObj.builder()
                  .from(principal)
                  .id(persistence.generateId())
                  .updateTimestamp(persistence.currentInstant())
                  .entityVersion(principal.entityVersion() + 1)
                  .credentialRotationRequired(reset && !principal.credentialRotationRequired());

          polarisPrincipalSecretsToPrincipal(principalSecrets, updatedPrincipalBuilder);

          var updatedPrincipal = updatedPrincipalBuilder.build();

          byName.put(IndexKey.key(updatedPrincipal.name()), objRef(updatedPrincipal));
          byClientId.put(clientIdKey, objRef(updatedPrincipal));

          state.writeOrReplace("principal", updatedPrincipal);

          return new ChangeResult.CommitChange<>(new PrincipalSecretsResult(principalSecrets));
        });
  }

  @Nullable
  @Override
  public PolarisPrincipalSecrets rotatePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {
    throw useMetaStoreManager("rotatePrincipalSecrets");
  }

  @Nonnull
  @Override
  public PolarisPrincipalSecrets generateNewPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String principalName, long principalId) {
    LOGGER.debug(
        "generateNewPrincipalSecrets principalName: {}, principalId: {}",
        principalName,
        principalId);

    return performPrincipalChange(
        PolarisPrincipalSecrets.class,
        (state, ref, byName, byId, byClientId) -> {
          var nameKey = IndexKey.key(principalName);
          var principalObjRef = byName.get(nameKey);

          var pers = state.persistence();
          var existingPrincipal =
              Optional.ofNullable(principalObjRef)
                  .map(objRef -> pers.fetch(objRef, PrincipalObj.class));

          checkState(
              existingPrincipal.isEmpty() || principalId == existingPrincipal.get().stableId(),
              "Principal id mismatch");

          // generate new secrets
          PolarisPrincipalSecrets newPrincipalSecrets;
          while (true) {
            newPrincipalSecrets = secretsGenerator(null).produceSecrets(principalName, principalId);
            var newClientId = newPrincipalSecrets.getPrincipalClientId();
            if (byClientId.get(IndexKey.key(newClientId)) == null) {
              break;
            }
          }

          var updatedPrincipalBuilder = PrincipalObj.builder();
          existingPrincipal.ifPresent(updatedPrincipalBuilder::from);
          var now = persistence.currentInstant();
          if (existingPrincipal.isEmpty()) {
            updatedPrincipalBuilder
                .name(principalName)
                .stableId(principalId)
                .entityVersion(1)
                .createTimestamp(now)
                .clientId(newPrincipalSecrets.getPrincipalClientId());
          }
          updatedPrincipalBuilder.id(persistence.generateId()).updateTimestamp(now);
          polarisPrincipalSecretsToPrincipal(newPrincipalSecrets, updatedPrincipalBuilder);
          var updatedPrincipal = updatedPrincipalBuilder.build();

          existingPrincipal
              .map(PrincipalObj::clientId)
              .map(IndexKey::key)
              .ifPresent(byClientId::remove);
          var updatedPrincipalObjRef = objRef(updatedPrincipal);
          byClientId.put(IndexKey.key(updatedPrincipal.clientId()), updatedPrincipalObjRef);
          byName.put(nameKey, updatedPrincipalObjRef);
          byId.put(IndexKey.key(principalId), nameKey);

          state.writeOrReplace("principal", updatedPrincipal);

          // return those
          return new ChangeResult.CommitChange<>(newPrincipalSecrets);
        });
  }

  protected PrincipalSecretsGenerator secretsGenerator(
      @Nullable RootCredentialsSet rootCredentialsSet) {
    if (rootCredentialsSet != null) {
      var realmId = this.persistence.realmId();
      return PrincipalSecretsGenerator.bootstrap(realmId, rootCredentialsSet);
    } else {
      return PrincipalSecretsGenerator.RANDOM_SECRETS;
    }
  }

  @Nullable
  @Override
  public PolarisPrincipalSecrets loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    LOGGER.debug("loadPrincipalSecrets clientId: {}", clientId);

    var key = IndexKey.key(clientId);

    return memoizedIndexedAccess
        .indexedAccess(0L, PolarisEntityType.PRINCIPAL.getCode())
        .refObj()
        .map(PrincipalsObj.class::cast)
        .map(PrincipalsObj::byClientId)
        .map(c -> c.indexForRead(persistence, OBJ_REF_SERIALIZER))
        .map(i -> i.get(key))
        .map(objRef -> persistence.fetch(objRef, PrincipalObj.class))
        .map(TypeMapping::principalObjToPolarisPrincipalSecrets)
        .orElse(null);
  }

  @Override
  public void deleteAll(@Nonnull PolarisCallContext callCtx) {
    throw unimplemented();
  }

  @Override
  public BasePersistence detach() {
    return new PersistenceMetaStore(persistence, privileges, storageIntegrationProvider);
  }

  private static UnsupportedOperationException unimplemented() {
    var ex = new UnsupportedOperationException("IMPLEMENT ME");
    LOGGER.error("Unsupported function call", ex);
    return ex;
  }

  private static UnsupportedOperationException useMetaStoreManager(String function) {
    var ex =
        new UnsupportedOperationException(
            "Operation not supported - use PolarisMetaStoreManager." + function + "()");
    LOGGER.error("Unsupported function call", ex);
    return ex;
  }
}
