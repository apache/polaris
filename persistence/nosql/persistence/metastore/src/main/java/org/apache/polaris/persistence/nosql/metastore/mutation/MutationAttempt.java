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

package org.apache.polaris.persistence.nosql.metastore.mutation;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.core.entity.PolarisEntityConstants.ENTITY_BASE_LOCATION;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.CATALOG_NOT_EMPTY;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RENAMED;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.ENTITY_NOT_FOUND;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.ENTITY_UNDROPPABLE;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.NAMESPACE_NOT_EMPTY;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.POLICY_HAS_MAPPINGS;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.EntityIdSet.entityIdSet;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.entitySubTypeCodeFromObjType;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.mapToEntity;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.mapToObj;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.maybeObjToPolarisPrincipalSecrets;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.objTypeForPolarisType;
import static org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMapping.POLICY_MAPPING_SERIALIZER;
import static org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMappingsObj.POLICY_MAPPINGS_REF_NAME;
import static org.apache.polaris.persistence.nosql.metastore.ContentIdentifier.identifierFromLocationString;
import static org.apache.polaris.persistence.nosql.metastore.ContentIdentifier.indexKeyToIdentifierBuilder;
import static org.apache.polaris.persistence.nosql.metastore.indexaccess.IndexUtils.hasChildren;
import static org.apache.polaris.persistence.nosql.metastore.mutation.MutationResults.newMutableMutationResults;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.persistence.nosql.api.commit.CommitterState;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.UpdatableIndex;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.catalog.EntityIdSet;
import org.apache.polaris.persistence.nosql.coretypes.changes.Change;
import org.apache.polaris.persistence.nosql.coretypes.changes.ChangeAdd;
import org.apache.polaris.persistence.nosql.coretypes.changes.ChangeRemove;
import org.apache.polaris.persistence.nosql.coretypes.changes.ChangeRename;
import org.apache.polaris.persistence.nosql.coretypes.changes.ChangeUpdate;
import org.apache.polaris.persistence.nosql.coretypes.content.PolicyObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMappingsObj;
import org.apache.polaris.persistence.nosql.metastore.ContentIdentifier;
import org.apache.polaris.persistence.nosql.metastore.committers.ChangeResult;
import org.apache.polaris.persistence.nosql.metastore.indexaccess.MemoizedIndexedAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record MutationAttempt(
    UpdateKeyForCatalogAndEntityType updateKeyForCatalogAndEntityType,
    List<EntityUpdate> updates,
    CommitterState<? extends ContainerObj, MutationResults> state,
    UpdatableIndex<ObjRef> byName,
    UpdatableIndex<IndexKey> byId,
    UpdatableIndex<Change> changes,
    UpdatableIndex<EntityIdSet> locations,
    MemoizedIndexedAccess memoizedIndexedAccess) {

  private static final Logger LOGGER = LoggerFactory.getLogger(MutationAttempt.class);

  public static ObjBase objForChangeComparison(
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

  public ChangeResult<MutationResults> apply() {
    var mutationResults = newMutableMutationResults();
    for (var update : updates) {
      LOGGER.debug("Processing update {}", update);

      switch (update.operation()) {
        case CREATE ->
            applyEntityCreateMutation(
                updateKeyForCatalogAndEntityType,
                state,
                byName,
                byId,
                changes,
                locations,
                update,
                mutationResults);
        case UPDATE ->
            applyEntityUpdateMutation(
                updateKeyForCatalogAndEntityType,
                state,
                byName,
                byId,
                changes,
                locations,
                update,
                mutationResults);
        case DELETE ->
            applyEntityDeleteMutation(
                updateKeyForCatalogAndEntityType,
                state,
                byName,
                byId,
                changes,
                locations,
                update,
                mutationResults,
                memoizedIndexedAccess);
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

  private static void applyEntityDeleteMutation(
      UpdateKeyForCatalogAndEntityType updateKeyForCatalogAndEntityType,
      CommitterState<? extends ContainerObj, MutationResults> state,
      UpdatableIndex<ObjRef> byName,
      UpdatableIndex<IndexKey> byId,
      UpdatableIndex<Change> changes,
      UpdatableIndex<EntityIdSet> locations,
      EntityUpdate update,
      MutationResults mutationResults,
      MemoizedIndexedAccess memoizedIndexedAccess) {
    var entity = update.entity();
    var entityType = entity.getType();
    var persistence = state.persistence();

    var entityIdKey = IndexKey.key(entity.getId());
    var originalNameKey = byId.get(entityIdKey);

    if (originalNameKey == null) {
      mutationResults.dropResult(ENTITY_NOT_FOUND);
      return;
    }
    var originalRef = byName.get(originalNameKey);
    if (originalRef == null) {
      mutationResults.dropResult(ENTITY_NOT_FOUND);
      return;
    }
    var originalObj =
        (ObjBase)
            state
                .persistence()
                .fetch(
                    originalRef,
                    objTypeForPolarisType(entityType, entity.getSubType()).targetClass());
    if (originalObj == null) {
      mutationResults.dropResult(ENTITY_NOT_FOUND);
      return;
    }
    if (entity.getEntityVersion() != originalObj.entityVersion()) {
      mutationResults.dropResult(TARGET_ENTITY_CONCURRENTLY_MODIFIED);
      return;
    }
    if (entity.cannotBeDroppedOrRenamed()) {
      mutationResults.dropResult(ENTITY_UNDROPPABLE);
      return;
    }

    updateLocationsIndex(locations, originalObj, null);

    var ok =
        switch (entityType) {
          case NAMESPACE -> {
            if (hasChildren(
                updateKeyForCatalogAndEntityType.catalogId(), byName, byId, entity.getId())) {
              mutationResults.dropResult(NAMESPACE_NOT_EMPTY);
              yield false;
            }
            yield true;
          }
          case CATALOG -> {
            var catalogState = memoizedIndexedAccess.catalogContent(entity.getId());

            if (catalogState.nameIndex().map(idx -> idx.iterator().hasNext()).orElse(false)) {
              mutationResults.dropResult(NAMESPACE_NOT_EMPTY);
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

            // If we have 2, we cannot drop the catalog.
            // If only one left, better be the admin role
            if (numCatalogRoles > 1) {
              mutationResults.dropResult(CATALOG_NOT_EMPTY);
              yield false;
            }
            // If 1, drop the last catalog role.
            // Should be the catalog admin role, but don't validate this.
            // (No need to drop the catalog role here, it'll be eventually done by
            // persistence-maintenance!)

            yield true;
          }
          case POLICY ->
              memoizedIndexedAccess
                  .referenceHead(POLICY_MAPPINGS_REF_NAME, PolicyMappingsObj.class)
                  .map(
                      policyMappingsObj -> {
                        var index =
                            policyMappingsObj
                                .policyMappings()
                                .indexForRead(persistence, POLICY_MAPPING_SERIALIZER);

                        var prefixKey = policyIndexPrefixKey((PolicyObj) originalObj, entity);

                        var iter = index.iterator(prefixKey, prefixKey, false);

                        if (iter.hasNext() && !update.cleanup()) {
                          mutationResults.dropResult(POLICY_HAS_MAPPINGS);
                          return false;
                        }

                        while (iter.hasNext()) {
                          var elem = iter.next();
                          var key = PolicyMappingsObj.PolicyMappingKey.fromIndexKey(elem.getKey());
                          var reversed = key.reverse();

                          mutationResults.addPolicyIndexKeyToRemove(elem.getKey());
                          mutationResults.addPolicyIndexKeyToRemove(reversed.toIndexKey());
                        }

                        return true;
                      })
                  .orElse(true);
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

  private static void applyEntityUpdateMutation(
      UpdateKeyForCatalogAndEntityType updateKeyForCatalogAndEntityType,
      CommitterState<? extends ContainerObj, MutationResults> state,
      UpdatableIndex<ObjRef> byName,
      UpdatableIndex<IndexKey> byId,
      UpdatableIndex<Change> changes,
      UpdatableIndex<EntityIdSet> locations,
      EntityUpdate update,
      MutationResults mutationResults) {
    var entity = update.entity();
    var entityType = entity.getType();
    var persistence = state.persistence();
    var now = persistence.currentInstant();

    var entityIdKey = IndexKey.key(entity.getId());
    var originalNameKey = byId.get(entityIdKey);

    var entityParentId = entity.getParentId();

    if (originalNameKey == null) {
      mutationResults.entityResult(ENTITY_NOT_FOUND);
      return;
    }
    var originalRef = byName.get(originalNameKey);
    if (originalRef == null) {
      mutationResults.entityResult(ENTITY_NOT_FOUND);
      return;
    }
    var originalObj =
        (ObjBase)
            state
                .persistence()
                .fetch(
                    originalRef,
                    objTypeForPolarisType(entityType, entity.getSubType()).targetClass());
    if (originalObj == null) {
      mutationResults.entityResult(ENTITY_NOT_FOUND);
      return;
    }
    if (entity.getEntityVersion() != originalObj.entityVersion()) {
      mutationResults.entityResult(TARGET_ENTITY_CONCURRENTLY_MODIFIED);
      return;
    }

    var currentSecrets = maybeObjToPolarisPrincipalSecrets(originalObj);

    var renameOrMove =
        entityParentId != originalObj.parentStableId()
            || !entity.getName().equals(originalObj.name());

    if (renameOrMove) {
      if (entity.cannotBeDroppedOrRenamed()) {
        mutationResults.entityResult(ENTITY_CANNOT_BE_RENAMED);
        return;
      }
      if (!byName.remove(originalNameKey)) {
        mutationResults.entityResult(ENTITY_NOT_FOUND);
        return;
      }

      var newNameKey = nameKeyForEntity(entity, byId, mutationResults::entityResult);
      if (newNameKey == null) {
        return;
      }

      var existingRef = byName.get(newNameKey);
      if (existingRef != null) {
        mutationResults.entityResult(
            ENTITY_ALREADY_EXISTS, entitySubTypeCodeFromObjType(existingRef));
        return;
      }

      var entityObj =
          mapToObj(entity, currentSecrets)
              .id(persistence.generateId())
              .updateTimestamp(now)
              .entityVersion(originalObj.entityVersion() + 1)
              .build();

      updateLocationsIndex(locations, originalObj, entityObj);

      state.writeOrReplace("entity-" + entityObj.stableId(), entityObj);

      byName.put(newNameKey, objRef(entityObj));
      byId.put(entityIdKey, newNameKey);
      if (changes != null) {
        checkState(
            changes.put(newNameKey, ChangeRename.builder().renameFrom(originalNameKey).build()),
            "Entity '%s' updated more than once",
            newNameKey);
      }
      mutationResults.entityResult(
          mapToEntity(entityObj, updateKeyForCatalogAndEntityType.catalogId()));

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

        updateLocationsIndex(locations, originalObj, entityObj);

        state.writeOrReplace("entity-" + entityObj.stableId(), entityObj);
        byName.put(originalNameKey, objRef(entityObj));
        if (changes != null) {
          checkState(
              changes.put(originalNameKey, ChangeUpdate.builder().build()),
              "Entity '%s' updated more than once",
              originalNameKey);
        }
        mutationResults.entityResult(
            mapToEntity(entityObj, updateKeyForCatalogAndEntityType.catalogId()));

        LOGGER.debug("Updated {} '{}' with ID {}...", entityType, originalNameKey, entity.getId());
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

  private static void applyEntityCreateMutation(
      UpdateKeyForCatalogAndEntityType updateKeyForCatalogAndEntityType,
      CommitterState<? extends ContainerObj, MutationResults> state,
      UpdatableIndex<ObjRef> byName,
      UpdatableIndex<IndexKey> byId,
      UpdatableIndex<Change> changes,
      UpdatableIndex<EntityIdSet> locations,
      EntityUpdate update,
      MutationResults mutationResults) {
    var entity = update.entity();
    var entityType = entity.getType();
    var persistence = state.persistence();
    var now = persistence.currentInstant();

    var entityIdKey = IndexKey.key(entity.getId());
    var originalNameKey = byId.get(entityIdKey);

    if (entityType == PolarisEntityType.PRINCIPAL) {
      throw new IllegalArgumentException("Use createPrincipal function instead of writeEntity");
    }

    var entityObjBuilder =
        mapToObj(entity, Optional.empty())
            .id(persistence.generateId())
            .createTimestamp(now)
            .updateTimestamp(now)
            .entityVersion(1);

    var nameKey = nameKeyForEntity(entity, byId, mutationResults::entityResult);
    if (nameKey == null) {
      return;
    }

    var entityObj = entityObjBuilder.build();
    var existingRef = byName.get(nameKey);
    if (existingRef != null || originalNameKey != null) {
      // PolarisMetaStoreManager.createEntityIfNotExists: if the entity already exists,
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
          var unchangedCompareObj = objForChangeComparison(entity, Optional.empty(), originalObj);
          if (unchangedCompareObj.equals(originalObj)) {
            mutationResults.unchangedEntityResult(entity);
            return;
          }
        }
      }

      mutationResults.entityResult(
          ENTITY_ALREADY_EXISTS, entitySubTypeCodeFromObjType(existingRef));
      return;
    }

    updateLocationsIndex(locations, null, entityObj);

    mutationResults.entityResult(
        mapToEntity(entityObj, updateKeyForCatalogAndEntityType.catalogId()));
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
        "Added {} '{}' with ID {}...", entityObj.type().name(), nameKey, entityObj.stableId());
  }

  private static IndexKey policyIndexPrefixKey(PolicyObj policyObj, PolarisBaseEntity entity) {
    // (Partial) index-key for the lookup
    var keyByPolicyTemplate =
        new PolicyMappingsObj.KeyByPolicy(
            entity.getCatalogId(), entity.getId(), policyObj.policyType().getCode(), 0L, 0L);

    // Construct the prefix-key
    return keyByPolicyTemplate.toPolicyWithTypePartialIndexKey();
  }

  public static void updateLocationsIndex(
      UpdatableIndex<EntityIdSet> locations, ObjBase originalObj, ObjBase entityObj) {
    var previousBaseLocation =
        originalObj != null ? originalObj.properties().get(ENTITY_BASE_LOCATION) : null;
    var entityBaseLocation =
        entityObj != null ? entityObj.properties().get(ENTITY_BASE_LOCATION) : null;

    if (Objects.equals(previousBaseLocation, entityBaseLocation)) {
      return;
    }

    if (previousBaseLocation != null) {
      var locationIdentifier =
          identifierFromLocationString(StorageLocation.of(previousBaseLocation).withoutScheme());
      var locationKey = locationIdentifier.toIndexKey();
      var currentEntityIds = locations.get(locationKey);
      var newIds = new HashSet<Long>();
      if (currentEntityIds != null) {
        newIds.addAll(currentEntityIds.entityIds());
      }
      newIds.remove(originalObj.stableId());
      if (newIds.isEmpty()) {
        locations.remove(locationKey);
      } else {
        locations.put(locationKey, entityIdSet(newIds));
      }
    }
    if (entityBaseLocation != null) {
      var locationWithoutScheme = StorageLocation.of(entityBaseLocation).withoutScheme();
      var locationIdentifier = identifierFromLocationString(locationWithoutScheme);
      var locationKey = locationIdentifier.toIndexKey();
      var currentEntityIds = locations.get(locationKey);
      var newIds = new HashSet<Long>();
      if (currentEntityIds != null) {
        newIds.addAll(currentEntityIds.entityIds());
      }
      newIds.add(entityObj.stableId());
      locations.put(locationKey, entityIdSet(newIds));
    }
  }

  private static IndexKey nameKeyForEntity(
      PolarisEntityCore entity,
      UpdatableIndex<IndexKey> byId,
      Consumer<BaseResult.ReturnStatus> errorHandler) {
    var identifierBuilder = ContentIdentifier.builder();
    var entityParentId = entity.getParentId();
    if (entityParentId != 0L && entityParentId != entity.getCatalogId()) {
      var parentNameKey = byId.get(IndexKey.key(entityParentId));
      if (parentNameKey == null) {
        errorHandler.accept(ENTITY_NOT_FOUND);
        return null;
      }
      indexKeyToIdentifierBuilder(parentNameKey, identifierBuilder);
    }
    identifierBuilder.addElements(entity.getName());
    var identifier = identifierBuilder.build();
    return identifier.toIndexKey();
  }
}
