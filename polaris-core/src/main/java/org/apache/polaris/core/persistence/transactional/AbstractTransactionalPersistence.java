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

import com.google.common.base.Preconditions;
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
import org.apache.polaris.core.persistence.EntityAlreadyExistsException;
import org.apache.polaris.core.persistence.PolicyMappingAlreadyExistsException;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PolicyType;
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
  // the in-transaction variants of methods in a new transaction.
  //

  /** {@inheritDoc} */
  @Override
  public long generateNewId(@Nonnull PolarisCallContext callCtx) {
    return runInTransaction(callCtx, () -> this.generateNewIdInCurrentTxn(callCtx));
  }

  /** Helper to perform the compare-and-swap semantics of a single writeEntity call. */
  private void checkConditionsForWriteEntityInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      @Nullable PolarisBaseEntity originalEntity) {
    PolarisBaseEntity refreshedEntity =
        this.lookupEntityInCurrentTxn(
            callCtx, entity.getCatalogId(), entity.getId(), entity.getTypeCode());

    if (originalEntity == null) {
      if (refreshedEntity != null) {
        // If this is a "create", and we manage to look up an existing entity with already
        // the same id, where ids are uniquely reserved when generated, it means it's a
        // low-level retry possibly in the face of a transient connectivity failure to
        // the backend database.
        throw new EntityAlreadyExistsException(refreshedEntity);
      } else {
        // Successfully verified the entity doesn't already exist by-id, but for a "create"
        // we must also check for name-collection now.
        refreshedEntity =
            this.lookupEntityByNameInCurrentTxn(
                callCtx,
                entity.getCatalogId(),
                entity.getParentId(),
                entity.getType().getCode(),
                entity.getName());
        if (refreshedEntity != null) {
          // Name-collision conflict.
          throw new EntityAlreadyExistsException(refreshedEntity);
        }
      }
    } else {
      // This is an "update".
      if (refreshedEntity == null
          || refreshedEntity.getEntityVersion() != originalEntity.getEntityVersion()
          || refreshedEntity.getGrantRecordsVersion() != originalEntity.getGrantRecordsVersion()) {
        // TODO: Better standardization of exception types, possibly make the ones that are
        // really part of the persistence contract be CheckedExceptions.
        throw new RetryOnConcurrencyException(
            "Entity '%s' id '%s' concurrently modified; expected version %s/%s got %s/%s",
            entity.getName(),
            entity.getId(),
            originalEntity.getEntityVersion(),
            originalEntity.getGrantRecordsVersion(),
            refreshedEntity != null ? refreshedEntity.getEntityVersion() : -1,
            refreshedEntity != null ? refreshedEntity.getGrantRecordsVersion() : -1);
      }
    }
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
        () -> {
          this.checkConditionsForWriteEntityInCurrentTxn(callCtx, entity, originalEntity);
          this.writeEntityInCurrentTxn(callCtx, entity, nameOrParentChanged, originalEntity);
        });
  }

  /** {@inheritDoc} */
  @Override
  public void writeEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisBaseEntity> entities,
      @Nullable List<PolarisBaseEntity> originalEntities) {
    if (originalEntities != null) {
      callCtx
          .getDiagServices()
          .check(
              entities.size() == originalEntities.size(),
              "mismatched_entities_and_original_entities_size",
              "entities.size()={}, originalEntities.size()={}",
              entities.size(),
              originalEntities.size());
    }
    runActionInTransaction(
        callCtx,
        () -> {
          // Validate and write each one independently so that we can also detect conflicting
          // writes to the same entity id within a given batch (so that previously written
          // ones will be seen during validation of the later item).
          for (int i = 0; i < entities.size(); ++i) {
            PolarisBaseEntity entity = entities.get(i);
            PolarisBaseEntity originalEntity =
                originalEntities != null ? originalEntities.get(i) : null;
            // TODO: This isn't quite correct right now, because originalEntity is only actually
            // safe to use for entityVersion and grantRecordsVersion right now. Once we refactor
            // the writeEntities[] methods to take something like PolarisEntityCore
            // for originalEntity and force the callsites such as BasePolarisCatalog to actually
            // provide the original values, this will be correct. For now, the API does't support
            // bulk renames anyways.
            boolean nameOrParentChanged =
                originalEntity == null
                    || !entity.getName().equals(originalEntity.getName())
                    || entity.getParentId() != originalEntity.getParentId();
            try {
              this.checkConditionsForWriteEntityInCurrentTxn(callCtx, entity, originalEntity);
            } catch (EntityAlreadyExistsException e) {
              // If the ids are equal then it is an idempotent-create-retry error, which counts
              // as a "success" for multi-entity commit purposes; name-collisions on different
              // ids counts as a true error that we rethrow.
              if (e.getExistingEntity().getId() != entity.getId()) {
                throw e;
              }
              // Else silently swallow the apparent create-retry
            }
            this.writeEntityInCurrentTxn(callCtx, entity, nameOrParentChanged, originalEntity);
          }
        });
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
  public Page<EntityNameLookupRecord> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PageToken pageToken) {
    return runInReadTransaction(
        callCtx,
        () -> this.listEntitiesInCurrentTxn(callCtx, catalogId, parentId, entityType, pageToken));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public Page<EntityNameLookupRecord> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull PageToken pageToken) {
    return runInReadTransaction(
        callCtx,
        () ->
            this.listEntitiesInCurrentTxn(
                callCtx, catalogId, parentId, entityType, entityFilter, pageToken));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public <T> Page<T> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull Function<PolarisBaseEntity, T> transformer,
      @Nonnull PageToken pageToken) {
    return runInReadTransaction(
        callCtx,
        () ->
            this.listEntitiesInCurrentTxn(
                callCtx, catalogId, parentId, entityType, entityFilter, transformer, pageToken));
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
  // the * variants of methods in a new transaction.
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

  @Override
  public PolarisPrincipalSecrets resetPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash,
      String customClientId,
      String customClientSecret) {
    return runInTransaction(
        callCtx,
        () ->
            this.rotatePrincipalSecretsInCurrentTxn(
                callCtx, clientId, principalId, reset, oldSecretHash));
  }
  ;

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
  // Implementations of the in-transaction versions for basic write/delete/lookup using the
  // slice-based model supported by this class.
  //

  /** {@inheritDoc} */
  @Override
  public void writeEntityInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity) {
    this.writeToEntitiesInCurrentTxn(callCtx, entity);
    this.writeToEntitiesChangeTrackingInCurrentTxn(callCtx, entity);

    if (nameOrParentChanged) {
      if (originalEntity != null) {
        // In our case, rename isn't automatically handled when the main "entities" slice
        // is updated; instead we must explicitly remove from the old entitiesActive
        // key as well.
        this.deleteFromEntitiesActiveInCurrentTxn(callCtx, originalEntity);
      }
      this.writeToEntitiesActiveInCurrentTxn(callCtx, entity);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void writeEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisBaseEntity> entities,
      @Nullable List<PolarisBaseEntity> originalEntities) {
    if (originalEntities != null) {
      callCtx
          .getDiagServices()
          .check(
              entities.size() == originalEntities.size(),
              "mismatched_entities_and_original_entities_size",
              "entities.size()={}, originalEntities.size()={}",
              entities.size(),
              originalEntities.size());
    }
    for (int i = 0; i < entities.size(); ++i) {
      PolarisBaseEntity entity = entities.get(i);
      PolarisBaseEntity originalEntity = originalEntities != null ? originalEntities.get(i) : null;
      // TODO: This isn't quite correct right now, because originalEntity is only actually
      // safe to use for entityVersion and grantRecordsVersion right now. Once we refactor
      // the writeEntities[] methods to take something like PolarisEntityCore
      // for originalEntity and force the callsites such as BasePolarisCatalog to actually
      // provide the original values, this will be correct. For now, the API does't support
      // bulk renames anyways.
      boolean nameOrParentChanged =
          originalEntity == null
              || !entity.getName().equals(originalEntity.getName())
              || entity.getParentId() != originalEntity.getParentId();
      this.writeEntityInCurrentTxn(callCtx, entity, nameOrParentChanged, originalEntity);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteEntityInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    this.deleteFromEntitiesActiveInCurrentTxn(callCtx, entity);
    this.deleteFromEntitiesInCurrentTxn(callCtx, entity);
    this.deleteFromEntitiesChangeTrackingInCurrentTxn(callCtx, entity);
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
        this.lookupEntityActiveInCurrentTxn(callCtx, entityActiveKey);

    // if not found, return null
    if (entityActiveRecord == null) {
      return null;
    }

    // lookup the entity, should be there
    PolarisBaseEntity entity =
        this.lookupEntityInCurrentTxn(
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
    return this.lookupEntityActiveInCurrentTxn(callCtx, entityActiveKey);
  }

  /** {@inheritDoc} */
  @Override
  public void writeToPolicyMappingRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisPolicyMappingRecord record) {
    this.runActionInTransaction(
        callCtx,
        () -> {
          this.checkConditionsForWriteToPolicyMappingRecordsInCurrentTxn(callCtx, record);
          this.writeToPolicyMappingRecordsInCurrentTxn(callCtx, record);
        });
  }

  /** {@inheritDoc} */
  @Override
  public void checkConditionsForWriteToPolicyMappingRecordsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisPolicyMappingRecord record) {

    PolicyType policyType = PolicyType.fromCode(record.getPolicyTypeCode());
    Preconditions.checkArgument(
        policyType != null, "Invalid policy type code: %s", record.getPolicyTypeCode());

    if (!policyType.isInheritable()) {
      return;
    }

    List<PolarisPolicyMappingRecord> existingRecords =
        this.loadPoliciesOnTargetByTypeInCurrentTxn(
            callCtx, record.getTargetCatalogId(), record.getTargetId(), record.getPolicyTypeCode());
    if (existingRecords.size() > 1) {
      throw new PolicyMappingAlreadyExistsException(existingRecords.get(0));
    } else if (existingRecords.size() == 1) {
      PolarisPolicyMappingRecord existingRecord = existingRecords.get(0);
      if (existingRecord.getPolicyCatalogId() != record.getPolicyCatalogId()
          || existingRecord.getPolicyId() != record.getPolicyId()) {
        throw new PolicyMappingAlreadyExistsException(existingRecord);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromPolicyMappingRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisPolicyMappingRecord record) {
    this.runActionInTransaction(
        callCtx, () -> this.deleteFromPolicyMappingRecordsInCurrentTxn(callCtx, record));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAllEntityPolicyMappingRecords(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      @Nonnull List<PolarisPolicyMappingRecord> mappingOnTarget,
      @Nonnull List<PolarisPolicyMappingRecord> mappingOnPolicy) {
    this.runActionInTransaction(
        callCtx,
        () ->
            this.deleteAllEntityPolicyMappingRecordsInCurrentTxn(
                callCtx, entity, mappingOnTarget, mappingOnPolicy));
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public PolarisPolicyMappingRecord lookupPolicyMappingRecord(
      @Nonnull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int policyTypeCode,
      long policyCatalogId,
      long policyId) {
    return this.runInReadTransaction(
        callCtx,
        () ->
            this.lookupPolicyMappingRecordInCurrentTxn(
                callCtx, targetCatalogId, targetId, policyTypeCode, policyCatalogId, policyId));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<PolarisPolicyMappingRecord> loadPoliciesOnTargetByType(
      @Nonnull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int policyTypeCode) {
    return this.runInReadTransaction(
        callCtx,
        () ->
            this.loadPoliciesOnTargetByTypeInCurrentTxn(
                callCtx, targetCatalogId, targetId, policyTypeCode));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<PolarisPolicyMappingRecord> loadAllPoliciesOnTarget(
      @Nonnull PolarisCallContext callCtx, long targetCatalogId, long targetId) {
    return this.runInReadTransaction(
        callCtx,
        () -> this.loadAllPoliciesOnTargetInCurrentTxn(callCtx, targetCatalogId, targetId));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<PolarisPolicyMappingRecord> loadAllTargetsOnPolicy(
      @Nonnull PolarisCallContext callCtx,
      long policyCatalogId,
      long policyId,
      int policyTypeCode) {
    return this.runInReadTransaction(
        callCtx,
        () ->
            this.loadAllTargetsOnPolicyInCurrentTxn(
                callCtx, policyCatalogId, policyId, policyTypeCode));
  }
}
