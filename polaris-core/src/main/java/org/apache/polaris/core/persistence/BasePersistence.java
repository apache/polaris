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
package org.apache.polaris.core.persistence;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntityActiveRecord;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;

/**
 * Interface to the Polaris metadata store, allows to persist and retrieve all Polaris metadata like
 * metadata for Polaris entities and metadata about grants between these entities which is the
 * foundation of our role base access control model.
 *
 * <p>Note that APIs to the actual persistence store are very basic, often point read or write to
 * the underlying data store. The goal is to make it really easy to back this using databases like
 * Postgres or simpler KV store.
 */
public interface BasePersistence {
  /**
   * @param callCtx call context
   * @return new unique entity identifier
   */
  long generateNewId(@Nonnull PolarisCallContext callCtx);

  /**
   * Write this entity to the meta store.
   *
   * @param callCtx call context
   * @param entity entity to persist
   * @param nameOrParentChanged if true, also write it to by-name lookups if applicable
   * @param originalEntity original state of the entity to use for compare-and-swap purposes, or
   *     null if this is expected to be a brand-new entity
   */
  void writeEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity);

  /**
   * Write the specified grantRecord to the grant_records table. If there is a conflict (existing
   * record with the same PK), all attributes of the new record will replace the existing one.
   *
   * @param callCtx call context
   * @param grantRec entity record to write, potentially replacing an existing entity record with
   *     the same key
   */
  void writeToGrantRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec);

  /**
   * Delete this entity from the meta store.
   *
   * @param callCtx call context
   * @param entity entity to delete
   */
  void deleteEntity(@Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity);

  /**
   * Delete the specified grantRecord to the grant_records table.
   *
   * @param callCtx call context
   * @param grantRec entity record to delete.
   */
  void deleteFromGrantRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec);

  /**
   * Delete the all grant records in the grant_records table for the specified entity. This method
   * will delete all grant records on that securable entity and also all grants to that grantee
   * entity assuming that the entity is a grantee (catalog role, principal role or principal).
   *
   * @param callCtx call context
   * @param entity entity whose grant records to and from should be deleted
   * @param grantsOnGrantee all grants to that grantee entity. Empty list if that entity is not a
   *     grantee
   * @param grantsOnSecurable all grants on that securable entity
   */
  void deleteAllEntityGrantRecords(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore entity,
      @Nonnull List<PolarisGrantRecord> grantsOnGrantee,
      @Nonnull List<PolarisGrantRecord> grantsOnSecurable);

  /**
   * Delete Polaris entity and grant record metadata from all tables. This is used during metadata
   * bootstrap to reset all tables to their original state
   *
   * @param callCtx call context
   */
  void deleteAll(@Nonnull PolarisCallContext callCtx);

  /**
   * Lookup an entity given its catalog id (which can be NULL_ID for top-level entities) and its
   * unique id.
   *
   * @param callCtx call context
   * @param catalogId catalog id or NULL_ID
   * @param entityId unique entity id
   * @return NULL if the entity was not found, else the base entity.
   */
  @Nullable
  PolarisBaseEntity lookupEntity(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId);

  /**
   * Lookup an entity given its catalogId, parentId, typeCode, and name.
   *
   * @param callCtx call context
   * @param catalogId catalog id or NULL_ID
   * @param parentId id of the parent, either a namespace or a catalog
   * @param typeCode the PolarisEntityType code of the entity to lookup
   * @param name the name of the entity
   * @return null if the specified entity does not exist
   */
  @Nullable
  PolarisBaseEntity lookupEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name);

  /**
   * Looks up just the entity's subType and id given it catalogId, parentId, typeCode, and name.
   *
   * @param callCtx call context
   * @param catalogId catalog id or NULL_ID
   * @param parentId id of the parent, either a namespace or a catalog
   * @param typeCode the PolarisEntityType code of the entity to lookup
   * @param name the name of the entity
   * @return null if the specified entity does not exist
   */
  default PolarisEntityActiveRecord lookupEntityIdAndSubTypeByName(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name) {
    PolarisBaseEntity baseEntity = lookupEntityByName(callCtx, catalogId, parentId, typeCode, name);
    if (baseEntity == null) {
      return null;
    }
    return new PolarisEntityActiveRecord(baseEntity);
  }

  /**
   * Lookup a set of entities given their catalog id/entity id unique identifier
   *
   * @param callCtx call context
   * @param entityIds list of entity ids
   * @return list of polaris base entities, parallel to the input list of ids. An entity in the list
   *     will be null if the corresponding entity could not be found.
   */
  @Nonnull
  List<PolarisBaseEntity> lookupEntities(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds);

  /**
   * Lookup the current entityVersion of an entity given its catalog id (which can be NULL_ID for
   * top-level entities) and its unique id. Will return 0 if the entity does not exist.
   *
   * @param callCtx call context
   * @param catalogId catalog id or NULL_ID
   * @param entityId unique entity id
   * @return current version for that entity or 0 if entity was not found.
   */
  int lookupEntityVersion(@Nonnull PolarisCallContext callCtx, long catalogId, long entityId);

  /**
   * Get change tracking versions for all specified entity ids.
   *
   * @param callCtx call context
   * @param entityIds list of entity id
   * @return list parallel to the input list of entity versions. If an entity cannot be found, the
   *     corresponding element in the list will be null
   */
  @Nonnull
  List<PolarisChangeTrackingVersions> lookupEntityVersions(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds);

  /**
   * List all active entities of the specified type which are child entities of the specified parent
   *
   * @param callCtx call context
   * @param catalogId catalog id for that entity, NULL_ID if the entity is top-level
   * @param parentId id of the parent, can be the special 0 value representing the root entity
   * @param entityType type of entities to list
   * @return the list of entities_active records for the specified list operation
   */
  @Nonnull
  List<PolarisEntityActiveRecord> listActiveEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType);

  /**
   * List active entities where some predicate returns true
   *
   * @param callCtx call context
   * @param catalogId catalog id for that entity, NULL_ID if the entity is top-level
   * @param parentId id of the parent, can be the special 0 value representing the root entity
   * @param entityType type of entities to list
   * @param entityFilter the filter to be applied to each entity. Only entities where the predicate
   *     returns true are returned in the list
   * @return the list of entities for which the predicate returns true
   */
  @Nonnull
  List<PolarisEntityActiveRecord> listActiveEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter);

  /**
   * List active entities where some predicate returns true and transform the entities with a
   * function
   *
   * @param callCtx call context
   * @param catalogId catalog id for that entity, NULL_ID if the entity is top-level
   * @param parentId id of the parent, can be the special 0 value representing the root entity
   * @param entityType type of entities to list
   * @param limit the max number of items to return
   * @param entityFilter the filter to be applied to each entity. Only entities where the predicate
   *     returns true are returned in the list
   * @param transformer the transformation function applied to the {@link PolarisBaseEntity} before
   *     returning
   * @return the list of entities for which the predicate returns true
   */
  @Nonnull
  <T> List<T> listActiveEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      int limit,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull Function<PolarisBaseEntity, T> transformer);

  /**
   * Lookup the current entityGrantRecordsVersion for the specified entity. That version is changed
   * everytime a grant record is added or removed on a base securable or added to a grantee.
   *
   * @param callCtx call context
   * @param catalogId catalog id or NULL_ID
   * @param entityId unique entity id
   * @return current grant records version for that entity.
   */
  int lookupEntityGrantRecordsVersion(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId);

  /**
   * Lookup the specified grant record from the grant_records table. Return NULL if not found
   *
   * @param callCtx call context
   * @param securableCatalogId catalog id of the securable entity, NULL_ID if the entity is
   *     top-level
   * @param securableId id of the securable entity
   * @param granteeCatalogId catalog id of the grantee entity, NULL_ID if the entity is top-level
   * @param granteeId id of the grantee entity
   * @param privilegeCode code for the privilege we are looking up
   * @return the grant record if found, NULL if not found
   */
  @Nullable
  PolarisGrantRecord lookupGrantRecord(
      @Nonnull PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode);

  /**
   * Get all grant records on the specified securable entity.
   *
   * @param callCtx call context
   * @param securableCatalogId catalog id of the securable entity, NULL_ID if the entity is
   *     top-level
   * @param securableId id of the securable entity
   * @return the list of grant records for the specified securable
   */
  @Nonnull
  List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      @Nonnull PolarisCallContext callCtx, long securableCatalogId, long securableId);

  /**
   * Get all grant records granted to the specified grantee entity.
   *
   * @param callCtx call context
   * @param granteeCatalogId catalog id of the grantee entity, NULL_ID if the entity is top-level
   * @param granteeId id of the grantee entity
   * @return the list of grant records for the specified grantee
   */
  @Nonnull
  List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      @Nonnull PolarisCallContext callCtx, long granteeCatalogId, long granteeId);

  /**
   * Check if the specified parent entity has children.
   *
   * @param callContext the polaris call context
   * @param optionalEntityType if not null, only check for the specified type, else check for all
   *     types of children entities
   * @param catalogId id of the catalog
   * @param parentId id of the parent, either a namespace or a catalog
   * @return true if the parent entity has children
   */
  boolean hasChildren(
      @Nonnull PolarisCallContext callContext,
      @Nullable PolarisEntityType optionalEntityType,
      long catalogId,
      long parentId);
}
