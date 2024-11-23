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

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntityActiveRecord;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Interface to the Polaris metadata store, allows to persist and retrieve all Polaris metadata like
 * metadata for Polaris entities and metadata about grants between these entities which is the
 * foundation of our role base access control model.
 *
 * <p>Note that APIs to the actual persistence store are very basic, often point read or write to
 * the underlying data store. The goal is to make it really easy to back this using databases like
 * Postgres or simpler KV store.
 */
public interface PolarisMetaStoreSession {

  /**
   * Run the specified transaction code (a Supplier lambda type) in a database read/write
   * transaction. If the code of the transaction does not throw any exception and returns normally,
   * the transaction will be committed, else the transaction will be automatically rolled-back on
   * error. The result of the supplier lambda is returned if success, else the error will be
   * re-thrown.
   *
   * @param callCtx call context
   * @param transactionCode code of the transaction being executed, a supplier lambda
   */
  <T> T runInTransaction(@NotNull PolarisCallContext callCtx, @NotNull Supplier<T> transactionCode);

  /**
   * Run the specified transaction code (a runnable lambda type) in a database read/write
   * transaction. If the code of the transaction does not throw any exception and returns normally,
   * the transaction will be committed, else the transaction will be automatically rolled-back on
   * error.
   *
   * @param callCtx call context
   * @param transactionCode code of the transaction being executed, a runnable lambda
   */
  void runActionInTransaction(
      @NotNull PolarisCallContext callCtx, @NotNull Runnable transactionCode);

  /**
   * Run the specified transaction code (a Supplier lambda type) in a database read transaction. If
   * the code of the transaction does not throw any exception and returns normally, the transaction
   * will be committed, else the transaction will be automatically rolled-back on error. The result
   * of the supplier lambda is returned if success, else the error will be re-thrown.
   *
   * @param callCtx call context
   * @param transactionCode code of the transaction being executed, a supplier lambda
   */
  <T> T runInReadTransaction(
      @NotNull PolarisCallContext callCtx, @NotNull Supplier<T> transactionCode);

  /**
   * Run the specified transaction code (a runnable lambda type) in a database read transaction. If
   * the code of the transaction does not throw any exception and returns normally, the transaction
   * will be committed, else the transaction will be automatically rolled-back on error.
   *
   * @param callCtx call context
   * @param transactionCode code of the transaction being executed, a runnable lambda
   */
  void runActionInReadTransaction(
      @NotNull PolarisCallContext callCtx, @NotNull Runnable transactionCode);

  /**
   * @param callCtx call context
   * @return new unique entity identifier
   */
  long generateNewId(@NotNull PolarisCallContext callCtx);

  /**
   * Write the base entity to the entities table. If there is a conflict (existing record with the
   * same id), all attributes of the new record will replace the existing one.
   *
   * @param callCtx call context
   * @param entity entity record to write, potentially replacing an existing entity record with the
   *     same key
   */
  void writeToEntities(@NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity entity);

  /**
   * Write the base entity to the entities_active table. If there is a conflict (existing record
   * with the same PK), all attributes of the new record will replace the existing one.
   *
   * @param callCtx call context
   * @param entity entity record to write, potentially replacing an existing entity record with the
   *     same key
   */
  void writeToEntitiesActive(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity entity);

  /**
   * Write the base entity to the entities_dropped table. If there is a conflict (existing record
   * with the same PK), all attributes of the new record will replace the existing one.
   *
   * @param callCtx call context
   * @param entity entity record to write, potentially replacing an existing entity record with the
   *     same key
   */
  void writeToEntitiesDropped(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity entity);

  /**
   * Write the base entity to the entities change tracking table. If there is a conflict (existing
   * record with the same id), all attributes of the new record will replace the existing one.
   *
   * @param callCtx call context
   * @param entity entity record to write, potentially replacing an existing entity record with the
   *     same key
   */
  void writeToEntitiesChangeTracking(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity entity);

  /**
   * Write the specified grantRecord to the grant_records table. If there is a conflict (existing
   * record with the same PK), all attributes of the new record will replace the existing one.
   *
   * @param callCtx call context
   * @param grantRec entity record to write, potentially replacing an existing entity record with
   *     the same key
   */
  void writeToGrantRecords(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisGrantRecord grantRec);

  /**
   * Delete the base entity from the entities table.
   *
   * @param callCtx call context
   * @param entity entity record to delete
   */
  void deleteFromEntities(@NotNull PolarisCallContext callCtx, @NotNull PolarisEntityCore entity);

  /**
   * Delete the base entity from the entities_active table.
   *
   * @param callCtx call context
   * @param entity entity record to delete
   */
  void deleteFromEntitiesActive(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisEntityCore entity);

  /**
   * Delete the base entity to the entities_dropped table
   *
   * @param callCtx call context
   * @param entity entity record to delete
   */
  void deleteFromEntitiesDropped(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity entity);

  /**
   * Delete the base entity from the entities change tracking table
   *
   * @param callCtx call context
   * @param entity entity record to delete
   */
  void deleteFromEntitiesChangeTracking(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisEntityCore entity);

  /**
   * Delete the specified grantRecord to the grant_records table.
   *
   * @param callCtx call context
   * @param grantRec entity record to delete.
   */
  void deleteFromGrantRecords(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisGrantRecord grantRec);

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
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisEntityCore entity,
      @NotNull List<PolarisGrantRecord> grantsOnGrantee,
      @NotNull List<PolarisGrantRecord> grantsOnSecurable);

  /**
   * Delete Polaris entity and grant record metadata from all tables. This is used during metadata
   * bootstrap to reset all tables to their original state
   *
   * @param callCtx call context
   */
  void deleteAll(@NotNull PolarisCallContext callCtx);

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
      @NotNull PolarisCallContext callCtx, long catalogId, long entityId);

  /**
   * Lookup a set of entities given their catalog id/entity id unique identifier
   *
   * @param callCtx call context
   * @param entityIds list of entity ids
   * @return list of polaris base entities, parallel to the input list of ids. An entity in the list
   *     will be null if the corresponding entity could not be found.
   */
  @NotNull
  List<PolarisBaseEntity> lookupEntities(
      @NotNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds);

  /**
   * Lookup in the entities_change_tracking table the current version of an entity given its catalog
   * id (which can be NULL_ID for top-level entities) and its unique id. Will return 0 if the entity
   * does not exist.
   *
   * @param callCtx call context
   * @param catalogId catalog id or NULL_ID
   * @param entityId unique entity id
   * @return current version for that entity or 0 if entity was not found.
   */
  int lookupEntityVersion(@NotNull PolarisCallContext callCtx, long catalogId, long entityId);

  /**
   * Get change tracking versions for all specified entity ids.
   *
   * @param callCtx call context
   * @param entityIds list of entity id
   * @return list parallel to the input list of entity versions. If an entity cannot be found, the
   *     corresponding element in the list will be null
   */
  @NotNull
  List<PolarisChangeTrackingVersions> lookupEntityVersions(
      @NotNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds);

  /**
   * Lookup in the entities_active table to determine if the specified entity exists. Return the
   * result of that lookup
   *
   * @param callCtx call context
   * @param entityActiveKey key in the ENTITIES_ACTIVE table
   * @return null if the specified entity does not exist or has been dropped.
   */
  @Nullable
  PolarisEntityActiveRecord lookupEntityActive(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisEntitiesActiveKey entityActiveKey);

  /**
   * Lookup in the entities_active table to determine if the specified set of entities exist. Return
   * the result, a parallel list of active records. A record in that list will be null if its
   * associated lookup failed
   *
   * @return the list of entities_active records for the specified lookup operation
   */
  @NotNull
  List<PolarisEntityActiveRecord> lookupEntityActiveBatch(
      @NotNull PolarisCallContext callCtx, List<PolarisEntitiesActiveKey> entityActiveKeys);

  /**
   * List all active entities of the specified type which are child entities of the specified parent
   *
   * @param callCtx call context
   * @param catalogId catalog id for that entity, NULL_ID if the entity is top-level
   * @param parentId id of the parent, can be the special 0 value representing the root entity
   * @param entityType type of entities to list
   * @return the list of entities_active records for the specified list operation
   */
  @NotNull
  List<PolarisEntityActiveRecord> listActiveEntities(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @NotNull PolarisEntityType entityType);

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
  @NotNull
  List<PolarisEntityActiveRecord> listActiveEntities(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @NotNull PolarisEntityType entityType,
      @NotNull Predicate<PolarisBaseEntity> entityFilter);

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
  @NotNull
  <T> List<T> listActiveEntities(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @NotNull PolarisEntityType entityType,
      int limit,
      @NotNull Predicate<PolarisBaseEntity> entityFilter,
      @NotNull Function<PolarisBaseEntity, T> transformer);

  /**
   * Lookup in the entities_change_tracking table the current version of the grant records for this
   * entity. That version is changed everytime a grant record is added or removed on a base
   * securable or added to a grantee.
   *
   * @param callCtx call context
   * @param catalogId catalog id or NULL_ID
   * @param entityId unique entity id
   * @return current grant records version for that entity.
   */
  int lookupEntityGrantRecordsVersion(
      @NotNull PolarisCallContext callCtx, long catalogId, long entityId);

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
      @NotNull PolarisCallContext callCtx,
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
  @NotNull
  List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      @NotNull PolarisCallContext callCtx, long securableCatalogId, long securableId);

  /**
   * Get all grant records granted to the specified grantee entity.
   *
   * @param callCtx call context
   * @param granteeCatalogId catalog id of the grantee entity, NULL_ID if the entity is top-level
   * @param granteeId id of the grantee entity
   * @return the list of grant records for the specified grantee
   */
  @NotNull
  List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      @NotNull PolarisCallContext callCtx, long granteeCatalogId, long granteeId);

  /**
   * Allows to retrieve to the secrets of a principal given its unique client id
   *
   * @param callCtx call context
   * @param clientId principal client id
   * @return the secrets
   */
  @Nullable
  PolarisPrincipalSecrets loadPrincipalSecrets(
      @NotNull PolarisCallContext callCtx, @NotNull String clientId);

  /**
   * generate and store a client id and associated secrets for a newly created principal entity
   *
   * @param callCtx call context
   * @param principalName name of the principal
   * @param principalId principal id
   */
  @NotNull
  PolarisPrincipalSecrets generateNewPrincipalSecrets(
      @NotNull PolarisCallContext callCtx, @NotNull String principalName, long principalId);

  /**
   * Rotate the secrets of a principal entity, i.e. make the specified main secrets the secondary
   * and generate a new main secret
   *
   * @param callCtx call context
   * @param clientId principal client id
   * @param principalId principal id
   * @param reset true if the principal secrets should be disabled and replaced with a one-time
   *     password
   * @param oldSecretHash the principal secret's old main secret hash
   */
  @Nullable
  PolarisPrincipalSecrets rotatePrincipalSecrets(
      @NotNull PolarisCallContext callCtx,
      @NotNull String clientId,
      long principalId,
      boolean reset,
      @NotNull String oldSecretHash);

  /**
   * When dropping a principal, we also need to drop the secrets of that principal
   *
   * @param callCtx the call context
   * @param clientId principal client id
   * @param principalId the id of the principal whose secrets are dropped
   */
  void deletePrincipalSecrets(
      @NotNull PolarisCallContext callCtx, @NotNull String clientId, long principalId);

  /**
   * Create an in-memory storage integration
   *
   * @param callCtx the polaris calllctx
   * @param catalogId the catalog id
   * @param entityId the entity id
   * @param polarisStorageConfigurationInfo the storage configuration information
   * @return a storage integration object
   */
  @Nullable
  <T extends PolarisStorageConfigurationInfo> PolarisStorageIntegration<T> createStorageIntegration(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      PolarisStorageConfigurationInfo polarisStorageConfigurationInfo);

  /**
   * Persist a storage integration in the metastore
   *
   * @param callContext the polaris call context
   * @param entity the entity of the object
   * @param storageIntegration the storage integration to persist
   */
  <T extends PolarisStorageConfigurationInfo> void persistStorageIntegrationIfNeeded(
      @NotNull PolarisCallContext callContext,
      @NotNull PolarisBaseEntity entity,
      @Nullable PolarisStorageIntegration<T> storageIntegration);

  /**
   * Load the polaris storage integration for a polaris entity (Catalog,Namespace,Table,View)
   *
   * @param callContext the polaris call context
   * @param entity the polaris entity
   * @return a polaris storage integration
   */
  @Nullable
  <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegration(
          @NotNull PolarisCallContext callContext, @NotNull PolarisBaseEntity entity);

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
      @NotNull PolarisCallContext callContext,
      @Nullable PolarisEntityType optionalEntityType,
      long catalogId,
      long parentId);

  /** Rollback the current transaction */
  void rollback();
}
