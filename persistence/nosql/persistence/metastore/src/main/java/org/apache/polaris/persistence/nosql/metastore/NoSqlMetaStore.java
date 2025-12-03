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
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.core.entity.PolarisEntityConstants.ENTITY_BASE_LOCATION;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.ENTITY_NOT_FOUND;
import static org.apache.polaris.persistence.nosql.api.index.IndexContainer.newUpdatableIndex;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.INDEX_KEY_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.EntityIdSet.ENTITY_ID_SET_SERIALIZER;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.containerTypeForEntityType;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.filterIsEntityType;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.isCatalogContent;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.mapToEntity;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.objTypeForPolarisTypeForFiltering;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.referenceName;
import static org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMapping.POLICY_MAPPING_SERIALIZER;
import static org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMappingsObj.POLICY_MAPPINGS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMappingsObj.PolicyMappingKey.fromIndexKey;
import static org.apache.polaris.persistence.nosql.coretypes.realm.RootObj.ROOT_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.refs.References.catalogReferenceNames;
import static org.apache.polaris.persistence.nosql.metastore.ContentIdentifier.identifierFromLocationString;
import static org.apache.polaris.persistence.nosql.metastore.ContentIdentifier.indexKeyToIdentifier;
import static org.apache.polaris.persistence.nosql.metastore.indexaccess.MemoizedIndexedAccess.newMemoizedIndexedAccess;
import static org.apache.polaris.persistence.nosql.metastore.mutation.EntityUpdate.Operation.CREATE;
import static org.apache.polaris.persistence.nosql.metastore.mutation.EntityUpdate.Operation.UPDATE;
import static org.apache.polaris.persistence.nosql.metastore.mutation.UpdateKeyForCatalogAndEntityType.updateKeyForCatalogAndEntityType;

import com.google.common.collect.Streams;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisObjectMapperUtil;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.apache.polaris.core.persistence.dao.entity.LoadPolicyMappingsResult;
import org.apache.polaris.core.persistence.dao.entity.PolicyAttachmentResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PolicyMappingUtil;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjTypes;
import org.apache.polaris.persistence.nosql.authz.api.Privilege;
import org.apache.polaris.persistence.nosql.authz.api.PrivilegeSet;
import org.apache.polaris.persistence.nosql.authz.api.Privileges;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.acl.AclObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogRoleObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogRolesObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogsObj;
import org.apache.polaris.persistence.nosql.coretypes.content.ContentObj;
import org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMapping;
import org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMappingsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.RootObj;
import org.apache.polaris.persistence.nosql.metastore.committers.CatalogChangeCommitterWrapper;
import org.apache.polaris.persistence.nosql.metastore.committers.ChangeCommitter;
import org.apache.polaris.persistence.nosql.metastore.committers.ChangeCommitterWrapper;
import org.apache.polaris.persistence.nosql.metastore.committers.ChangeResult;
import org.apache.polaris.persistence.nosql.metastore.indexaccess.IndexedContainerAccess;
import org.apache.polaris.persistence.nosql.metastore.indexaccess.MemoizedIndexedAccess;
import org.apache.polaris.persistence.nosql.metastore.mutation.EntityUpdate;
import org.apache.polaris.persistence.nosql.metastore.mutation.GrantsMutation;
import org.apache.polaris.persistence.nosql.metastore.mutation.MutationAttempt;
import org.apache.polaris.persistence.nosql.metastore.mutation.MutationAttemptRoot;
import org.apache.polaris.persistence.nosql.metastore.mutation.MutationResults;
import org.apache.polaris.persistence.nosql.metastore.mutation.PolicyMutation;
import org.apache.polaris.persistence.nosql.metastore.mutation.PrincipalMutations;
import org.apache.polaris.persistence.nosql.metastore.mutation.PrincipalMutations.UpdateSecrets.SecretsUpdater;
import org.apache.polaris.persistence.nosql.metastore.mutation.UpdateKeyForCatalogAndEntityType;
import org.apache.polaris.persistence.nosql.metastore.privs.GrantTriplet;
import org.apache.polaris.persistence.nosql.metastore.privs.SecurableAndGrantee;
import org.apache.polaris.persistence.nosql.metastore.privs.SecurableGranteePrivilegeTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NoSqlMetaStore extends NonFunctionalBasePersistence {
  private static final Logger LOGGER = LoggerFactory.getLogger(NoSqlMetaStore.class);

  private final Persistence persistence;
  private final Privileges privileges;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;
  private final MemoizedIndexedAccess memoizedIndexedAccess;
  private final PolarisDiagnostics diagnostics;

  NoSqlMetaStore(
      Persistence persistence,
      Privileges privileges,
      PolarisStorageIntegrationProvider storageIntegrationProvider,
      PolarisDiagnostics diagnostics) {
    this.persistence = persistence;
    this.privileges = privileges;
    this.storageIntegrationProvider = storageIntegrationProvider;
    this.memoizedIndexedAccess = newMemoizedIndexedAccess(persistence);
    this.diagnostics = diagnostics;
  }

  <REF_OBJ extends ContainerObj, RESULT> RESULT performChange(
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
      var commitRetryable = new ChangeCommitterWrapper<>(changeCommitter, entityType);
      return committer.commitRuntimeException(commitRetryable).orElseThrow();
    } finally {
      memoizedIndexedAccess.invalidateIndexedAccess(catalogStableId, entityType.getCode());
    }
  }

  long generateNewId() {
    return persistence.generateId();
  }

  void initializeCatalogsIfNecessary() {
    memoizedIndexedAccess
        .indexedAccess(0, PolarisEntityType.CATALOG.getCode())
        .nameIndex()
        .ifPresent(
            names ->
                persistence
                    .bucketizedBulkFetches(
                        Streams.stream(names).filter(Objects::nonNull).map(Map.Entry::getValue),
                        CatalogObj.class)
                    .filter(Objects::nonNull)
                    .forEach(
                        catalogObj -> {
                          LOGGER.debug("Initializing catalog {} if necessary", catalogObj.name());
                          initializeCatalogIfNecessary(persistence, catalogObj);
                        }));
  }

  CreateCatalogResult createCatalog(
      PolarisBaseEntity catalog, List<PolarisBaseEntity> principalRoles) {
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
            // A catalog with the same name already exists (different ID)
            return new ChangeResult.NoChange<>(
                new CreateCatalogResult(ENTITY_ALREADY_EXISTS, null));
          }
          if (catalogObj == null) {
            catalogObj =
                EntityObjMappings.<CatalogObj, CatalogObj.Builder>mapToObj(
                        catalog, Optional.empty())
                    .id(persistence.generateId())
                    .build();
            state.writeOrReplace("catalog", catalogObj);
          }

          initializeCatalogIfNecessary(persistence, catalogObj);

          checkState(!byId.contains(idKey), "Catalog ID %s already used", catalog.getId());

          // 'persistStorageIntegrationIfNeeded' is a no-op in all implementations ?!?!?
          // persistStorageIntegrationIfNeeded(callCtx, catalog, integration);

          var catalogAdminRoleObj =
              createCatalogRoleIdempotent(
                  catalogObj,
                  persistence.generateId(),
                  PolarisEntityConstants.getNameOfCatalogAdminRole());

          var catalogAdminRole = mapToEntity(catalogAdminRoleObj, catalogObj.stableId());

          var grants = new ArrayList<SecurableGranteePrivilegeTuple>();

          // grant the catalog admin role access-management on the catalog
          grants.add(
              new SecurableGranteePrivilegeTuple(
                  catalog, catalogAdminRole, PolarisPrivilege.CATALOG_MANAGE_ACCESS));
          // grant the catalog admin role metadata-management on the catalog; this one is revocable
          grants.add(
              new SecurableGranteePrivilegeTuple(
                  catalog, catalogAdminRole, PolarisPrivilege.CATALOG_MANAGE_METADATA));

          var effRoles =
              principalRoles.isEmpty()
                  ? List.of(
                      requireNonNull(
                          lookupEntityByName(
                              0L,
                              0L,
                              PolarisEntityType.PRINCIPAL_ROLE.getCode(),
                              PolarisEntityConstants.getNameOfPrincipalServiceAdminRole())))
                  : principalRoles;

          for (PolarisBaseEntity effRole : effRoles) {
            grants.add(
                new SecurableGranteePrivilegeTuple(
                    catalogAdminRole, effRole, PolarisPrivilege.CATALOG_ROLE_USAGE));
          }

          persistGrantsOrRevokes(true, grants.toArray(SecurableGranteePrivilegeTuple[]::new));

          byName.put(nameKey, objRef(catalogObj));
          byId.put(idKey, nameKey);

          if (existing == null) {
            // created
            return new ChangeResult.CommitChange<>(
                new CreateCatalogResult(catalog, catalogAdminRole));
          }
          // retry
          return new ChangeResult.NoChange<>(new CreateCatalogResult(ENTITY_ALREADY_EXISTS, null));
        }));
  }

  private static void initializeCatalogIfNecessary(Persistence persistence, CatalogObj catalog) {
    persistence.createReferencesSilent(catalogReferenceNames(catalog.stableId()));
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

    return createOrUpdateEntity(CREATE, entity);
  }

  EntitiesResult createEntities(List<PolarisBaseEntity> entities) {
    LOGGER.atDebug().addArgument(() -> logEntitiesInfo(entities)).log("create entities: {}");

    return createOrUpdateEntities(entities.stream(), CREATE);
  }

  EntityResult updateEntity(PolarisBaseEntity entity) {
    LOGGER.atDebug().addArgument(() -> logEntityInfo(entity)).log("update entity: {}");
    return createOrUpdateEntity(UPDATE, entity);
  }

  EntitiesResult updateEntities(List<EntityWithPath> entities) {
    LOGGER
        .atDebug()
        .addArgument(
            () -> logEntitiesInfo(entities.stream().map(EntityWithPath::getEntity).toList()))
        .log("update entities: {}");

    return createOrUpdateEntities(entities.stream().map(EntityWithPath::getEntity), UPDATE);
  }

  private EntityResult createOrUpdateEntity(EntityUpdate.Operation op, PolarisBaseEntity entity) {
    var mutationResults =
        performEntityMutations(
            updateKeyForCatalogAndEntityType(entity), List.of(new EntityUpdate(op, entity)));
    return (EntityResult) mutationResults.results().getFirst();
  }

  private EntitiesResult createOrUpdateEntities(
      Stream<PolarisBaseEntity> entitiesStream, EntityUpdate.Operation op) {
    var byCatalogAndEntityType =
        entitiesStream
            .map(e -> new EntityUpdate(op, e))
            .collect(Collectors.groupingBy(u -> updateKeyForCatalogAndEntityType(u.entity())));

    checkArgument(
        byCatalogAndEntityType.size() <= 1,
        "Cannot atomically create entities against multiple targets: %s",
        byCatalogAndEntityType.keySet());

    for (var concernChanges : byCatalogAndEntityType.entrySet()) {
      var results = performEntityMutations(concernChanges.getKey(), concernChanges.getValue());
      var firstFailure = results.firstFailure();
      if (firstFailure.isPresent()) {
        var failure = firstFailure.get();
        return new EntitiesResult(failure.getReturnStatus(), failure.getExtraInformation());
      }

      return new EntitiesResult(
          Page.fromItems(
              results.results().stream()
                  .map(EntityResult.class::cast)
                  .map(EntityResult::getEntity)
                  .collect(Collectors.toList())));
    }

    return new EntitiesResult(Page.fromItems(List.of()));
  }

  DropEntityResult dropEntity(
      PolarisBaseEntity entityToDrop, Map<String, String> cleanupProperties, boolean cleanup) {
    requireNonNull(entityToDrop);

    LOGGER.atDebug().addArgument(() -> logEntityInfo(entityToDrop)).log("drop entity: {}");

    var results =
        performEntityMutations(
            updateKeyForCatalogAndEntityType(entityToDrop),
            List.of(new EntityUpdate(EntityUpdate.Operation.DELETE, entityToDrop, cleanup)));

    if (cleanup && PolarisEntityType.POLICY == entityToDrop.getType()) {
      cleanup = false;
    }

    var result = results.results().getFirst();
    if (result.isSuccess() && cleanup) {
      // If cleanup, schedule a cleanup task for the entity.
      // Do this here so that the drop operation and scheduling the cleanup task are
      // transactional.
      // Otherwise, we'll be unable to schedule the cleanup task
      var dropped = results.droppedEntities().getFirst();

      PolarisEntity.Builder taskEntityBuilder =
          new PolarisEntity.Builder()
              .setId(generateNewId())
              .setCatalogId(0L)
              .setName("entityCleanup_" + entityToDrop.getId())
              .setType(PolarisEntityType.TASK)
              .setSubType(PolarisEntitySubType.NULL_SUBTYPE)
              .setCreateTimestamp(persistence.currentTimeMillis());

      Map<String, String> properties = new HashMap<>();
      properties.put(
          PolarisTaskConstants.TASK_TYPE,
          String.valueOf(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER.typeCode()));
      properties.put("data", PolarisObjectMapperUtil.serialize(dropped));
      taskEntityBuilder.setProperties(properties);
      if (cleanupProperties != null) {
        taskEntityBuilder.setInternalProperties(cleanupProperties);
      }
      var taskEntity = taskEntityBuilder.build();

      try {
        performEntityMutations(
            new UpdateKeyForCatalogAndEntityType(PolarisEntityType.TASK, 0L, false),
            List.of(new EntityUpdate(CREATE, taskEntity)));

        if (entityToDrop.getType() == PolarisEntityType.POLICY) {
          detachAllPolicyMappings(true, entityToDrop.getCatalogId(), entityToDrop.getId());
        } else if (PolicyMappingUtil.isValidTargetEntityType(
            entityToDrop.getType(), entityToDrop.getSubType())) {
          detachAllPolicyMappings(false, entityToDrop.getCatalogId(), entityToDrop.getId());
        }

        return new DropEntityResult(taskEntity.getId());
      } catch (Exception e) {
        LOGGER.warn("Failed to write cleanup task entity for dropped entity", e);
      }
    }

    return (DropEntityResult) result;
  }

  private void detachAllPolicyMappings(boolean policyNotEntity, long catalogId, long id) {
    try {
      var committer =
          persistence
              .createCommitter(POLICY_MAPPINGS_REF_NAME, PolicyMappingsObj.class, String.class)
              .synchronizingLocally();
      var ignore =
          committer
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
                                () -> newUpdatableIndex(persistence, POLICY_MAPPING_SERIALIZER));
                    var builder = PolicyMappingsObj.builder();
                    refObj.ifPresent(builder::from);

                    var keyBy =
                        policyNotEntity
                            ? new PolicyMappingsObj.KeyByPolicy(catalogId, id, 0, 0L, 0L)
                                .toPolicyPartialIndexKey()
                            : new PolicyMappingsObj.KeyByEntity(catalogId, id, 0, 0L, 0L)
                                .toEntityPartialIndexKey();

                    var keys = new ArrayList<IndexKey>();
                    for (var iter = index.iterator(keyBy, keyBy, true); iter.hasNext(); ) {
                      var elem = iter.next();
                      keys.add(elem.getKey());
                    }

                    if (keys.isEmpty()) {
                      return state.noCommit("");
                    }

                    for (var key : keys) {
                      index.remove(key);
                      index.remove(fromIndexKey(key).reverse().toIndexKey());
                    }

                    builder.policyMappings(index.toIndexed("mappings", state::writeOrReplace));
                    return state.commitResult("", builder, refObj);
                  })
              .orElseThrow();
    } finally {
      memoizedIndexedAccess.invalidateReferenceHead(POLICY_MAPPINGS_REF_NAME);
    }
  }

  private MutationResults performEntityMutations(
      UpdateKeyForCatalogAndEntityType updateKeyForCatalogAndEntityType,
      List<EntityUpdate> updates) {
    LOGGER
        .atDebug()
        .addArgument(updates.size())
        .addArgument(updateKeyForCatalogAndEntityType.entityType())
        .addArgument(updateKeyForCatalogAndEntityType.catalogId())
        .addArgument(
            updateKeyForCatalogAndEntityType.catalogContent()
                ? "catalog-content"
                : "non-catalog-content")
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

    if (updateKeyForCatalogAndEntityType.entityType() == PolarisEntityType.ROOT) {
      checkArgument(updates.size() == 1, "Cannot write multiple root entities");
      try {
        var update = updates.getFirst();
        checkArgument(update.operation() == CREATE, "Cannot update or delete the root entity");
        return persistence
            .createCommitter(ROOT_REF_NAME, RootObj.class, MutationResults.class)
            .synchronizingLocally()
            .commitRuntimeException(
                (state, refObjSupplier) ->
                    new MutationAttemptRoot(state, refObjSupplier, update).apply())
            .orElseThrow();
      } finally {
        memoizedIndexedAccess.invalidateIndexedAccess(0L, PolarisEntityType.ROOT.getCode());
      }
    }

    var mutationResults = (MutationResults) null;

    if (updateKeyForCatalogAndEntityType.catalogContent()) {
      try {
        var committer =
            persistence
                .createCommitter(
                    format(
                        CATALOG_STATE_REF_NAME_PATTERN,
                        updateKeyForCatalogAndEntityType.catalogId()),
                    CatalogStateObj.class,
                    MutationResults.class)
                .synchronizingLocally();
        var commitRetryable =
            new CatalogChangeCommitterWrapper<MutationResults>(
                ((state, ref, byName, byId, changes, locations) ->
                    new MutationAttempt(
                            updateKeyForCatalogAndEntityType,
                            updates,
                            state,
                            byName,
                            byId,
                            changes,
                            locations,
                            memoizedIndexedAccess)
                        .apply()));
        mutationResults = committer.commitRuntimeException(commitRetryable).orElseThrow();
      } finally {
        memoizedIndexedAccess.invalidateCatalogContent(
            updateKeyForCatalogAndEntityType.catalogId());
      }
    } else {
      mutationResults =
          performChange(
              updateKeyForCatalogAndEntityType.entityType(),
              containerTypeForEntityType(updateKeyForCatalogAndEntityType.entityType()),
              MutationResults.class,
              updateKeyForCatalogAndEntityType.catalogId(),
              ((state, ref, byName, byId) ->
                  new MutationAttempt(
                          updateKeyForCatalogAndEntityType,
                          updates,
                          state,
                          byName,
                          byId,
                          null,
                          null,
                          memoizedIndexedAccess)
                      .apply()));
    }

    // TODO populate MutationResults.aclsToRemove and handle those, also need a maintenance
    //  operation to garbage-collect ACL entries for no longer existing entities.

    // TODO handle MutationResults.policyIndexKeysToRemove(), also need a maintenance
    //  operation to garbage-collect stale policy entries.

    return mutationResults;
  }

  <T extends PolarisEntity & LocationBasedEntity> Optional<String> hasOverlappingSiblings(
      T entity) {
    var baseLocation = entity.getBaseLocation();
    if (baseLocation == null) {
      return Optional.empty();
    }

    var checkLocation = StorageLocation.of(baseLocation).withoutScheme();

    return hasOverlappingSiblings(entity.getCatalogId(), checkLocation);
  }

  Optional<String> hasOverlappingSiblings(long catalogId, String checkLocation) {
    return memoizedIndexedAccess
        .catalogContent(catalogId)
        .refObj()
        .flatMap(
            catalogStateObj -> {
              var locationsIndex =
                  catalogStateObj
                      .locations()
                      .map(i -> i.indexForRead(persistence, ENTITY_ID_SET_SERIALIZER))
                      .orElseGet(Index::empty);
              var byId =
                  catalogStateObj.stableIdToName().indexForRead(persistence, INDEX_KEY_SERIALIZER);
              var byName =
                  catalogStateObj.nameToObjRef().indexForRead(persistence, OBJ_REF_SERIALIZER);

              var locationIdentifier = identifierFromLocationString(checkLocation);
              var locationIndexKey = locationIdentifier.toIndexKey();
              // TODO VALIDATE THE CHECKS HERE !
              var iter = locationsIndex.iterator(locationIndexKey, null, false);
              if (!iter.hasNext()) {
                return Optional.empty();
              }

              var elem = iter.next();
              var elemKey = elem.getKey();
              var elemIdentifier = indexKeyToIdentifier(elemKey);
              if (!elemIdentifier.startsWith(locationIdentifier)) {
                return Optional.empty();
              }

              return elem.getValue().entityIds().stream()
                  .map(IndexKey::key)
                  .map(byId::get)
                  .filter(Objects::nonNull)
                  .map(byName::get)
                  .filter(Objects::nonNull)
                  .map(objRef -> persistence.fetch(objRef, ContentObj.class))
                  .filter(Objects::nonNull)
                  .map(
                      contentObj -> {
                        // Check if conflict is the parent namespace - TODO recurse??
                        var conflictingBaseLocation =
                            contentObj.properties().get(ENTITY_BASE_LOCATION);
                        return conflictingBaseLocation != null
                            ? conflictingBaseLocation
                            : String.join("/", elemIdentifier.elements());
                      })
                  .findFirst();
            });
  }

  @Nullable
  PolarisBaseEntity lookupEntity(long catalogId, long entityId, int entityTypeCode) {
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

    return resolved
        .flatMap(objBase -> filterIsEntityType(objBase, entityTypeCode))
        .map(objBase -> mapToEntity(objBase, access.catalogStableId()))
        .orElse(null);
  }

  PolarisBaseEntity lookupEntityByName(
      long catalogId, long parentId, int entityTypeCode, String name) {
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

    return resolved
        .flatMap(objBase -> filterIsEntityType(objBase, entityTypeCode))
        .map(objBase -> mapToEntity(objBase, access.catalogStableId()))
        .orElse(null);
  }

  Optional<PolarisBaseEntity> lookupRoot() {
    return memoizedIndexedAccess
        .indexedAccess(0L, PolarisEntityType.ROOT.getCode())
        .byId(0L)
        .map(root -> EntityObjMappings.mapToEntity(root, 0L));
  }

  <I, T> Page<T> fetchEntitiesAsPage(
      long catalogStableId,
      long parentId,
      PolarisEntityType entityType,
      PolarisEntitySubType entitySubType,
      PageToken pageToken,
      Function<ObjBase, I> mapper,
      Predicate<I> filter,
      Function<I, T> transformer) {

    LOGGER.debug(
        "fetchEntitiesAsPage, catalogId: {}, parentId: {}, entityType: {}, pageToken: {}",
        catalogStableId,
        parentId,
        entityType,
        pageToken);

    if (entityType == PolarisEntityType.NULL_TYPE) {
      return Page.fromItems(List.of());
    }
    if (entityType == PolarisEntityType.CATALOG) {
      catalogStableId = 0L;
    }

    var paginationToken = pageToken.valueAs(NoSqlPaginationToken.class);
    var pageTokenOffset = paginationToken.map(NoSqlPaginationToken::key);

    var catalogContent = isCatalogContent(entityType);
    var access =
        paginationToken.isPresent()
            ? memoizedIndexedAccess.indexedAccessDirect(
                paginationToken.orElseThrow().containerObjRef())
            : catalogContent
                ? memoizedIndexedAccess.catalogContent(catalogStableId)
                : memoizedIndexedAccess.indexedAccess(catalogStableId, entityType.getCode());
    var nameIndex = access.nameIndex().orElse(null);

    if (nameIndex == null) {
      return Page.fromItems(List.of());
    }

    var objRefs = Stream.<Map.Entry<IndexKey, ObjRef>>empty();
    if (catalogStableId != 0L) {
      if (parentId == 0L || parentId == catalogStableId) {
        // list on catalog root
        var lower = pageTokenOffset.orElse(null);
        objRefs = catalogRootEntriesStream(nameIndex, lower);
      } else {
        // list on namespace
        var prefixKeyOptional = access.nameKeyById(parentId);
        if (prefixKeyOptional.isPresent()) {
          var prefixKey = prefixKeyOptional.get();
          var offsetKey =
              pageTokenOffset.filter(pto -> pto.compareTo(prefixKey) >= 0).orElse(prefixKey);
          var prefix = indexKeyToIdentifier(prefixKey);
          objRefs = catalogNamespaceEntriesStream(nameIndex, offsetKey, prefix);
        }
      }
    } else {
      objRefs = Streams.stream(nameIndex.iterator(pageTokenOffset.orElse(null), null, false));
    }

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

    return listEntitiesBuildPage(access, pageToken, mapper, filter, transformer, objRefs);
  }

  Stream<? extends ContentObj> listChildren(Index<ObjRef> nameIndex, ContentIdentifier parent) {
    var objRefs =
        (parent.isEmpty()
                ? catalogRootEntriesStream(nameIndex, null)
                : catalogNamespaceEntriesStream(nameIndex, parent.toIndexKey(), parent))
            .map(Map.Entry::getValue);

    return persistence.bucketizedBulkFetches(objRefs, ContentObj.class);
  }

  private static Stream<Map.Entry<IndexKey, ObjRef>> catalogNamespaceEntriesStream(
      Index<ObjRef> nameIndex, IndexKey offsetKey, ContentIdentifier prefix) {
    var prefixElems = prefix.elements();
    var directChildLevel = prefixElems.size() + 1;
    return Streams.stream(nameIndex.iterator(offsetKey, null, false))
        .takeWhile(
            e -> {
              var ident = indexKeyToIdentifier(requireNonNull(e).getKey());
              var identElems = ident.elements();
              if (identElems.size() < prefixElems.size() + 1) {
                return ident.equals(prefix);
              }
              return identElems.subList(0, prefixElems.size()).equals(prefixElems);
            })
        .filter(
            e -> {
              var ident = indexKeyToIdentifier(requireNonNull(e).getKey());
              return ident.elements().size() == directChildLevel;
            });
  }

  private static Stream<Map.Entry<IndexKey, ObjRef>> catalogRootEntriesStream(
      Index<ObjRef> nameIndex, IndexKey lower) {
    return Streams.stream(nameIndex.iterator(lower, null, false))
        .filter(Objects::nonNull)
        .filter(
            e -> {
              var ident = indexKeyToIdentifier(e.getKey());
              return ident.elements().size() == 1;
            });
  }

  /**
   * Number of {@link ObjBase objects} to {@link Persistence#fetchMany(Class, ObjRef...)
   * bulk-fetch}.
   */
  public static final int FETCH_PAGE_SIZE = 25;

  private <I, T> Page<T> listEntitiesBuildPage(
      IndexedContainerAccess<?> access,
      PageToken pageToken,
      Function<ObjBase, I> mapper,
      Predicate<I> filter,
      Function<I, T> transformer,
      Stream<Map.Entry<IndexKey, ObjRef>> objRefs) {
    var limit = pageToken.pageSize().orElse(Integer.MAX_VALUE);
    var nextToken = (NoSqlPaginationToken) null;
    var result = new ArrayList<T>();

    var fetchBuffer = new ArrayList<Map.Entry<IndexKey, ObjRef>>();

    for (var objRefIter = objRefs.iterator(); objRefIter.hasNext(); ) {
      var keyAndRef = objRefIter.next();
      fetchBuffer.add(keyAndRef);
      if (fetchBuffer.size() == FETCH_PAGE_SIZE) {
        nextToken =
            listEntitiesBuildPagePart(
                access, fetchBuffer, mapper, filter, transformer, result, limit);
        fetchBuffer.clear();
        if (nextToken != null || result.size() == limit) {
          break;
        }
      }
    }
    if (!fetchBuffer.isEmpty()) {
      nextToken =
          listEntitiesBuildPagePart(
              access, fetchBuffer, mapper, filter, transformer, result, limit);
    }

    return Page.page(pageToken, result, nextToken);
  }

  @Nullable
  private <I, T> NoSqlPaginationToken listEntitiesBuildPagePart(
      IndexedContainerAccess<?> access,
      List<Map.Entry<IndexKey, ObjRef>> fetchBuffer,
      Function<ObjBase, I> mapper,
      Predicate<I> filter,
      Function<I, T> transformer,
      List<T> result,
      int limit) {
    var objs =
        persistence.fetchMany(
            ObjBase.class, fetchBuffer.stream().map(Map.Entry::getValue).toArray(ObjRef[]::new));
    for (int i = 0; i < fetchBuffer.size(); i++) {
      var obj = objs[i];

      if (obj == null) {
        continue;
      }
      var intermediate = mapper.apply(obj);
      if (intermediate == null || !filter.test(intermediate)) {
        continue;
      }
      var transformed = transformer.apply(intermediate);
      if (transformed == null) {
        continue;
      }

      if (result.size() == limit) {
        return NoSqlPaginationToken.paginationToken(
            ObjRef.objRef(access.refObj().orElseThrow()), fetchBuffer.get(i).getKey());
      }

      result.add(transformed);
    }
    return null;
  }

  PolicyAttachmentResult attachDetachPolicyOnEntity(
      long policyCatalogId,
      long policyId,
      @Nonnull PolicyType policyType,
      long targetCatalogId,
      long targetId,
      boolean doAttach,
      @Nonnull Map<String, String> parameters) {
    return new PolicyMutation(
            persistence,
            memoizedIndexedAccess,
            policyCatalogId,
            policyId,
            requireNonNull(policyType),
            targetCatalogId,
            targetId,
            doAttach,
            parameters)
        .apply();
  }

  // TODO remove entirely?
  @SuppressWarnings("SameParameterValue")
  LoadPolicyMappingsResult loadEntitiesOnPolicy(
      @Nullable PolarisEntityType entityType,
      long policyCatalogId,
      long policyId,
      Optional<PolicyType> policyType) {
    if (entityType != null && !isCatalogContent(entityType)) {
      return new LoadPolicyMappingsResult(ENTITY_NOT_FOUND, null);
    }

    return memoizedIndexedAccess
        .referenceHead(POLICY_MAPPINGS_REF_NAME, PolicyMappingsObj.class)
        .map(
            policyMappingsObj -> {
              var index =
                  policyMappingsObj
                      .policyMappings()
                      .indexForRead(persistence, POLICY_MAPPING_SERIALIZER);

              // (Partial) index-key for the lookup
              var keyByPolicyTemplate =
                  new PolicyMappingsObj.KeyByPolicy(
                      policyCatalogId,
                      policyId,
                      policyType.map(PolicyType::getCode).orElse(0),
                      0L,
                      0L);

              // Construct the prefix-key, depending on whether to look for all attached policies or
              // attached policies having the given policy-type
              var prefixKey =
                  policyType.isPresent()
                      ? keyByPolicyTemplate.toPolicyWithTypePartialIndexKey()
                      : keyByPolicyTemplate.toPolicyPartialIndexKey();

              Class<? extends PolicyMappingsObj.PolicyMappingKey> expectedKeyType =
                  PolicyMappingsObj.KeyByPolicy.class;

              return loadPolicyMappings(index, prefixKey, expectedKeyType);
            })
        .orElse(new LoadPolicyMappingsResult(List.of(), List.of()));
  }

  LoadPolicyMappingsResult loadPoliciesOnEntity(
      @Nullable PolarisEntityType entityType,
      long catalogId,
      long id,
      Optional<PolicyType> policyType) {
    if (entityType != null
        && entityType != PolarisEntityType.CATALOG
        && !isCatalogContent(entityType)) {
      return new LoadPolicyMappingsResult(ENTITY_NOT_FOUND, null);
    }

    return memoizedIndexedAccess
        .referenceHead(POLICY_MAPPINGS_REF_NAME, PolicyMappingsObj.class)
        .map(
            policyMappingsObj -> {
              var index =
                  policyMappingsObj
                      .policyMappings()
                      .indexForRead(persistence, POLICY_MAPPING_SERIALIZER);

              // (Partial) index-key for the lookup
              var keyByEntityTemplate =
                  new PolicyMappingsObj.KeyByEntity(
                      catalogId, id, policyType.map(PolicyType::getCode).orElse(0), 0L, 0L);

              // Construct the prefix-key, depending on whether to look for all attached policies or
              // attached policies having the given policy-type
              var prefixKey =
                  policyType.isPresent()
                      ? keyByEntityTemplate.toPolicyTypePartialIndexKey()
                      : keyByEntityTemplate.toEntityPartialIndexKey();

              Class<? extends PolicyMappingsObj.PolicyMappingKey> expectedKeyType =
                  PolicyMappingsObj.KeyByEntity.class;

              return loadPolicyMappings(index, prefixKey, expectedKeyType);
            })
        .orElse(new LoadPolicyMappingsResult(List.of(), List.of()));
  }

  private LoadPolicyMappingsResult loadPolicyMappings(
      Index<PolicyMapping> index,
      IndexKey prefixKey,
      Class<? extends PolicyMappingsObj.PolicyMappingKey> expectedKeyType) {
    var mappingRecords = new ArrayList<PolarisPolicyMappingRecord>();
    var policyEntities = new ArrayList<PolarisBaseEntity>();
    var seenPolicies = new HashSet<Long>();
    for (var iter = index.iterator(prefixKey, prefixKey, false); iter.hasNext(); ) {
      var elem = iter.next();
      var key = fromIndexKey(elem.getKey());
      if (expectedKeyType.isInstance(key)) {
        if (seenPolicies.add(key.policyId())) {
          memoizedIndexedAccess
              .catalogContent(key.policyCatalogId())
              .byId(key.policyId())
              .flatMap(objBase -> filterIsEntityType(objBase, PolarisEntityType.POLICY))
              .map(obj -> mapToEntity(obj, key.policyCatalogId()))
              .ifPresent(policyEntities::add);
        }
        mappingRecords.add(key.toMappingRecord(elem.getValue()));
      } else {
        // `key` is not what we're looking for.
        // This should actually never happen due to the prefix-key.
        break;
      }
    }

    return new LoadPolicyMappingsResult(mappingRecords, policyEntities);
  }

  // grants

  @FunctionalInterface
  interface AclEntryHandler {
    void handle(SecurableAndGrantee securableAndGrantee, PrivilegeSet granted);
  }

  List<PolarisGrantRecord> allGrantRecords(PolarisBaseEntity entity) {
    var catalogId = entity.getCatalogId();
    var aclName = GrantTriplet.forEntity(entity).toRoleName();

    LOGGER.debug("allGrantRecords for {}", aclName);

    var grantRecords =
        memoizedIndexedAccess
            .grantsIndex()
            .map(entries -> collectGrantRecords(catalogId, aclName, entries))
            .orElseGet(List::of);

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

    var collector = new GrantRecordsCollector();
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

  private List<PolarisGrantRecord> collectGrantRecords(
      long catalogId, String aclName, Index<ObjRef> securablesIndex) {

    var collector = new GrantRecordsCollector();
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
        },
        securablesIndex);
    return collector.grantRecords;
  }

  private void collectGrantRecords(
      long catalogStableId, String aclName, AclEntryHandler aclEntryConsumer) {
    LOGGER.debug("Checking ACL '{}'", aclName);

    var securablesIndex = memoizedIndexedAccess.grantsIndex();
    if (securablesIndex.isPresent()) {
      collectGrantRecords(catalogStableId, aclName, aclEntryConsumer, securablesIndex.get());
    } else {
      LOGGER.trace("ACL {} does not exist", aclName);
    }
  }

  private void collectGrantRecords(
      long catalogStableId,
      String aclName,
      AclEntryHandler aclEntryConsumer,
      Index<ObjRef> securablesIndex) {
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
  }

  static class GrantRecordsCollector implements AclEntryHandler {
    final List<PolarisGrantRecord> grantRecords = new ArrayList<>();

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

  boolean persistGrantsOrRevokes(boolean doGrant, SecurableGranteePrivilegeTuple... grants) {
    LOGGER.debug("Persisting {} for '{}'", doGrant ? "grants" : "revokes", Arrays.asList(grants));

    return new GrantsMutation(persistence, memoizedIndexedAccess, privileges, doGrant, grants)
        .apply();
  }

  <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegration(
          @Nonnull PolarisBaseEntity entity) {
    var storageConfig = BaseMetaStoreManager.extractStorageConfiguration(diagnostics, entity);
    return storageIntegrationProvider.getStorageIntegrationForConfig(storageConfig);
  }

  CreatePrincipalResult createPrincipal(
      PolarisBaseEntity principal, RootCredentialsSet rootCredentialsSet) {
    LOGGER.atDebug().addArgument(() -> logEntityInfo(principal)).log("createPrincipal {}");

    return new PrincipalMutations.CreatePrincipal(
            persistence, memoizedIndexedAccess, principal, rootCredentialsSet)
        .apply();
  }

  static class PrincipalNotFoundException extends RuntimeException {}

  <R> R updatePrincipalSecrets(
      Class<R> resultType, String logInfo, long principalId, SecretsUpdater<R> updater) {
    LOGGER.debug("updatePrincipalSecrets ({}), principalId: {}", logInfo, principalId);

    return new PrincipalMutations.UpdateSecrets<>(
            persistence, memoizedIndexedAccess, resultType, principalId, updater)
        .apply();
  }

  PolarisPrincipalSecrets loadPrincipalSecrets(@Nonnull String clientId) {
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
        .map(EntityObjMappings::principalObjToPolarisPrincipalSecrets)
        .orElse(null);
  }
}
