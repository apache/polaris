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
package org.apache.polaris.core.persistence.transactional;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;

/**
 * Extends BasePersistence to express a more "transaction-oriented" control flow for backing stores
 * which can support a runInTransaction semantic, while providing default implementations of some of
 * the BasePersistence methods in terms of lower-level methods that subclasses must implement.
 */
public abstract class AbstractTransactionalPersistence implements TransactionalPersistence {
  //
  // New abstract methods specific to this slice-based transactional persistence that subclasses
  // must implement to inherit implementations of lookup/write/delete
  //

  /**
   * Lookup an entity by entityActiveKey
   *
   * @param callCtx call context
   * @param entityActiveKey key by name
   * @return null if the specified entity does not exist or has been dropped.
   */
  @Nullable
  protected abstract EntityNameLookupRecord lookupEntityActiveInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntitiesActiveKey entityActiveKey);

  /**
   * Write the base entity to the entities table. If there is a conflict (existing record with the
   * same id), all attributes of the new record will replace the existing one.
   *
   * @param callCtx call context
   * @param entity entity record to write, potentially replacing an existing entity record with the
   *     same key
   */
  protected abstract void writeToEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity);

  /**
   * Write the base entity to the entities_active table. If there is a conflict (existing record
   * with the same PK), all attributes of the new record will replace the existing one.
   *
   * @param callCtx call context
   * @param entity entity record to write, potentially replacing an existing entity record with the
   *     same key
   */
  protected abstract void writeToEntitiesActiveInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity);

  /**
   * Write the base entity to the entities change tracking table. If there is a conflict (existing
   * record with the same id), all attributes of the new record will replace the existing one.
   *
   * @param callCtx call context
   * @param entity entity record to write, potentially replacing an existing entity record with the
   *     same key
   */
  protected abstract void writeToEntitiesChangeTrackingInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity);

  /**
   * Delete the base entity from the entities table.
   *
   * @param callCtx call context
   * @param entity entity record to delete
   */
  protected abstract void deleteFromEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore entity);

  /**
   * Delete the base entity from the entities_active table.
   *
   * @param callCtx call context
   * @param entity entity record to delete
   */
  protected abstract void deleteFromEntitiesActiveInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore entity);

  /**
   * Delete the base entity from the entities change tracking table
   *
   * @param callCtx call context
   * @param entity entity record to delete
   */
  protected abstract void deleteFromEntitiesChangeTrackingInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore entity);

  //
  // Implementations of the one-shot atomic BasePersistence methods which explicitly run
  // the *InCurrentTxn variants of methods in a new transaction.
  //

  /** {@inheritDoc} */
  @Override
  public long generateNewId(@Nonnull PolarisCallContext callCtx) {
    return runInTransaction(callCtx, () -> this.generateNewIdInCurrentTxn(callCtx));
  }

  /** {@inheritDoc} */
  @Override
  public void writeEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity) {
    runActionInTransaction(
        callCtx,
        () -> this.writeEntityInCurrentTxn(callCtx, entity, nameOrParentChanged, originalEntity));
  }

  /** {@inheritDoc} */
  @Override
  public void writeEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisBaseEntity> entities,
      @Nullable List<PolarisBaseEntity> originalEntities) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /** {@inheritDoc} */
  @Override
  public void writeToGrantRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {
    runActionInTransaction(callCtx, () -> this.writeToGrantRecordsInCurrentTxn(callCtx, grantRec));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteEntity(@Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    runActionInTransaction(callCtx, () -> this.deleteEntityInCurrentTxn(callCtx, entity));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromGrantRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {
    runActionInTransaction(
        callCtx, () -> this.deleteFromGrantRecordsInCurrentTxn(callCtx, grantRec));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAllEntityGrantRecords(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore entity,
      @Nonnull List<PolarisGrantRecord> grantsOnGrantee,
      @Nonnull List<PolarisGrantRecord> grantsOnSecurable) {
    runActionInTransaction(
        callCtx,
        () ->
            this.deleteAllEntityGrantRecordsInCurrentTxn(
                callCtx, entity, grantsOnGrantee, grantsOnSecurable));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAll(@Nonnull PolarisCallContext callCtx) {
    runActionInTransaction(callCtx, () -> this.deleteAllInCurrentTxn(callCtx));
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public PolarisBaseEntity lookupEntity(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId, int typeCode) {
    return runInReadTransaction(
        callCtx, () -> this.lookupEntityInCurrentTxn(callCtx, catalogId, entityId, typeCode));
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public PolarisBaseEntity lookupEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name) {
    return runInReadTransaction(
        callCtx,
        () -> this.lookupEntityByNameInCurrentTxn(callCtx, catalogId, parentId, typeCode, name));
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public EntityNameLookupRecord lookupEntityIdAndSubTypeByName(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name) {
    return runInReadTransaction(
        callCtx,
        () ->
            this.lookupEntityIdAndSubTypeByNameInCurrentTxn(
                callCtx, catalogId, parentId, typeCode, name));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<PolarisBaseEntity> lookupEntities(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    return runInReadTransaction(callCtx, () -> this.lookupEntitiesInCurrentTxn(callCtx, entityIds));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<PolarisChangeTrackingVersions> lookupEntityVersions(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    return runInReadTransaction(
        callCtx, () -> this.lookupEntityVersionsInCurrentTxn(callCtx, entityIds));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<EntityNameLookupRecord> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType) {
    return runInReadTransaction(
        callCtx, () -> this.listEntitiesInCurrentTxn(callCtx, catalogId, parentId, entityType));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<EntityNameLookupRecord> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter) {
    return runInReadTransaction(
        callCtx,
        () ->
            this.listEntitiesInCurrentTxn(callCtx, catalogId, parentId, entityType, entityFilter));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public <T> List<T> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      int limit,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull Function<PolarisBaseEntity, T> transformer) {
    return runInReadTransaction(
        callCtx,
        () ->
            this.listEntitiesInCurrentTxn(
                callCtx, catalogId, parentId, entityType, limit, entityFilter, transformer));
  }

  /** {@inheritDoc} */
  @Override
  public int lookupEntityGrantRecordsVersion(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId) {
    return runInReadTransaction(
        callCtx,
        () -> this.lookupEntityGrantRecordsVersionInCurrentTxn(callCtx, catalogId, entityId));
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public PolarisGrantRecord lookupGrantRecord(
      @Nonnull PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    return runInReadTransaction(
        callCtx,
        () ->
            this.lookupGrantRecordInCurrentTxn(
                callCtx,
                securableCatalogId,
                securableId,
                granteeCatalogId,
                granteeId,
                privilegeCode));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      @Nonnull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    return runInReadTransaction(
        callCtx,
        () ->
            this.loadAllGrantRecordsOnSecurableInCurrentTxn(
                callCtx, securableCatalogId, securableId));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      @Nonnull PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    return runInReadTransaction(
        callCtx,
        () -> this.loadAllGrantRecordsOnGranteeInCurrentTxn(callCtx, granteeCatalogId, granteeId));
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasChildren(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityType optionalEntityType,
      long catalogId,
      long parentId) {
    return runInReadTransaction(
        callCtx,
        () -> this.hasChildrenInCurrentTxn(callCtx, optionalEntityType, catalogId, parentId));
  }

  //
  // Implementations of the one-shot atomic IntegrationPersistence methods which explicitly run
  // the *InCurrentTxn variants of methods in a new transaction.
  //

  /** {@inheritDoc} */
  @Override
  @Nullable
  public PolarisPrincipalSecrets loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    return runInReadTransaction(
        callCtx, () -> this.loadPrincipalSecretsInCurrentTxn(callCtx, clientId));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public PolarisPrincipalSecrets generateNewPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String principalName, long principalId) {
    return runInTransaction(
        callCtx,
        () -> this.generateNewPrincipalSecretsInCurrentTxn(callCtx, principalName, principalId));
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public PolarisPrincipalSecrets rotatePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {
    return runInTransaction(
        callCtx,
        () ->
            this.rotatePrincipalSecretsInCurrentTxn(
                callCtx, clientId, principalId, reset, oldSecretHash));
  }

  /** {@inheritDoc} */
  @Override
  public void deletePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId, long principalId) {
    runActionInTransaction(
        callCtx, () -> this.deletePrincipalSecretsInCurrentTxn(callCtx, clientId, principalId));
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> createStorageIntegration(
          @Nonnull PolarisCallContext callCtx,
          long catalogId,
          long entityId,
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    return runInTransaction(
        callCtx,
        () ->
            this.createStorageIntegrationInCurrentTxn(
                callCtx, catalogId, entityId, polarisStorageConfigurationInfo));
  }

  /** {@inheritDoc} */
  @Override
  public <T extends PolarisStorageConfigurationInfo> void persistStorageIntegrationIfNeeded(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      @Nullable PolarisStorageIntegration<T> storageIntegration) {
    runActionInTransaction(
        callCtx,
        () ->
            this.persistStorageIntegrationIfNeededInCurrentTxn(
                callCtx, entity, storageIntegration));
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegration(
          @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    return runInReadTransaction(
        callCtx, () -> this.loadPolarisStorageIntegrationInCurrentTxn(callCtx, entity));
  }

  //
  // Implementations of the *InCurrentTxn versions for basic write/delete/lookup using the
  // slice-based model supported by this class.
  //

  /** {@inheritDoc} */
  @Override
  public void writeEntityInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity) {
    // TODO: Pull down relevant compare-and-swap semantics from PolarisMetaStoreManagerImpl
    // into this layer.
    writeToEntitiesInCurrentTxn(callCtx, entity);
    writeToEntitiesChangeTrackingInCurrentTxn(callCtx, entity);

    if (nameOrParentChanged) {
      if (originalEntity != null) {
        // In our case, rename isn't automatically handled when the main "entities" slice
        // is updated; instead we must explicitly remove from the old entitiesActive
        // key as well.
        deleteFromEntitiesActiveInCurrentTxn(callCtx, originalEntity);
      }
      writeToEntitiesActiveInCurrentTxn(callCtx, entity);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void writeEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisBaseEntity> entities,
      @Nullable List<PolarisBaseEntity> originalEntities) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /** {@inheritDoc} */
  @Override
  public void deleteEntityInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    deleteFromEntitiesActiveInCurrentTxn(callCtx, entity);
    deleteFromEntitiesInCurrentTxn(callCtx, entity);
    deleteFromEntitiesChangeTrackingInCurrentTxn(callCtx, entity);
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public PolarisBaseEntity lookupEntityByNameInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name) {
    PolarisEntitiesActiveKey entityActiveKey =
        new PolarisEntitiesActiveKey(catalogId, parentId, typeCode, name);

    // ensure that the entity exists
    EntityNameLookupRecord entityActiveRecord =
        lookupEntityActiveInCurrentTxn(callCtx, entityActiveKey);

    // if not found, return null
    if (entityActiveRecord == null) {
      return null;
    }

    // lookup the entity, should be there
    PolarisBaseEntity entity =
        lookupEntityInCurrentTxn(
            callCtx,
            entityActiveRecord.getCatalogId(),
            entityActiveRecord.getId(),
            entityActiveRecord.getTypeCode());
    callCtx
        .getDiagServices()
        .checkNotNull(
            entity, "unexpected_not_found_entity", "entityActiveRecord={}", entityActiveRecord);

    // return it now
    return entity;
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public EntityNameLookupRecord lookupEntityIdAndSubTypeByNameInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name) {
    PolarisEntitiesActiveKey entityActiveKey =
        new PolarisEntitiesActiveKey(catalogId, parentId, typeCode, name);
    return lookupEntityActiveInCurrentTxn(callCtx, entityActiveKey);
  }
}
