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

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.IntegrationPersistence;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.TransactionalPolicyMappingPersistence;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Extends BasePersistence to express a more "transaction-oriented" control flow for backing stores
 * which can support a runInTransaction semantic, while providing default implementations of some of
 * the BasePersistence methods in terms of lower-level methods that subclasses must implement.
 */
public interface TransactionalPersistence
    extends BasePersistence, IntegrationPersistence, TransactionalPolicyMappingPersistence {

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
  <T> T runInTransaction(@NonNull PolarisCallContext callCtx, @NonNull Supplier<T> transactionCode);

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
      @NonNull PolarisCallContext callCtx, @NonNull Runnable transactionCode);

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
      @NonNull PolarisCallContext callCtx, @NonNull Supplier<T> transactionCode);

  /**
   * Run the specified transaction code (a runnable lambda type) in a database read transaction. If
   * the code of the transaction does not throw any exception and returns normally, the transaction
   * will be committed, else the transaction will be automatically rolled-back on error.
   *
   * @param callCtx call context
   * @param transactionCode code of the transaction being executed, a runnable lambda
   */
  void runActionInReadTransaction(
      @NonNull PolarisCallContext callCtx, @NonNull Runnable transactionCode);

  /**
   * Lookup the specified set of entities by entityActiveKeys Return the result, a parallel list of
   * active records. A record in that list will be null if its associated lookup failed
   *
   * @return the list of entityActiveKeys for the specified lookup operation
   */
  @NonNull List<EntityNameLookupRecord> lookupEntityActiveBatchInCurrentTxn(
      @NonNull PolarisCallContext callCtx, List<PolarisEntitiesActiveKey> entityActiveKeys);

  /** Rollback the current transaction */
  void rollback();

  //
  // Every method of BasePersistence will have a related * method here; the semantics
  // being that transactional implementations of a PolarisMetaStoreManager may choose to
  // self-manage outer transactions to perform all the persistence calls within that provided
  // transaction, while the basic implementation will only use the "durable in a single-shot"
  // methods from BasePersistence. Condition-checks for atomic compare-and-swap behaviors are *not*
  // expected to occur within these *InCurrentTxn methods.
  //

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#generateNewId} */
  long generateNewIdInCurrentTxn(@NonNull PolarisCallContext callCtx);

  /**
   * See {@link org.apache.polaris.core.persistence.BasePersistence#writeEntity}
   *
   * <p>NOTE: By virtue of the way callers of these *InCurrentTxn methods organize entity-state
   * checks interspersed between different persistence actions, the basic compare-and-swap
   * conditions are *not* expected to be enforced within these methods, in contrast to the analogous
   * methods in BasePersistence. For example, BasePersistence::writeEntity is expected to use the
   * entityVersion of originalEntity as part of an atomic conditional check before writing the new
   * entity, but TransactionalPersistence::writeEntityInCurrentTxn is *not* expected to do the same.
   */
  void writeEntityInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#writeEntities} */
  void writeEntitiesInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisBaseEntity> entities,
      @Nullable List<PolarisBaseEntity> originalEntities);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#writeToGrantRecords} */
  void writeToGrantRecordsInCurrentTxn(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisGrantRecord grantRec);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#deleteEntity} */
  void deleteEntityInCurrentTxn(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisBaseEntity entity);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#deleteFromGrantRecords} */
  void deleteFromGrantRecordsInCurrentTxn(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisGrantRecord grantRec);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#deleteAllEntityGrantRecords} */
  void deleteAllEntityGrantRecordsInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore entity,
      @NonNull List<PolarisGrantRecord> grantsOnGrantee,
      @NonNull List<PolarisGrantRecord> grantsOnSecurable);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#deleteAll} */
  void deleteAllInCurrentTxn(@NonNull PolarisCallContext callCtx);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#lookupEntity} */
  @Nullable PolarisBaseEntity lookupEntityInCurrentTxn(
      @NonNull PolarisCallContext callCtx, long catalogId, long entityId, int typeCode);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#lookupEntityByName} */
  @Nullable PolarisBaseEntity lookupEntityByNameInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @NonNull String name);

  /**
   * See {@link org.apache.polaris.core.persistence.BasePersistence#lookupEntityIdAndSubTypeByName}
   */
  @Nullable EntityNameLookupRecord lookupEntityIdAndSubTypeByNameInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @NonNull String name);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#lookupEntities} */
  @NonNull List<PolarisBaseEntity> lookupEntitiesInCurrentTxn(
      @NonNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#lookupEntityVersions} */
  @NonNull List<PolarisChangeTrackingVersions> lookupEntityVersionsInCurrentTxn(
      @NonNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds);

  /**
   * See {@link org.apache.polaris.core.persistence.BasePersistence#listEntities}. Implementations
   * may choose to override this method for performance reasons (to only load the required subset of
   * the entity properties to build the EntityNameLookupRecord).
   */
  default @NonNull Page<EntityNameLookupRecord> listEntitiesInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
    return loadEntitiesInCurrentTxn(
        callCtx,
        catalogId,
        parentId,
        entityType,
        entitySubType,
        e -> true,
        EntityNameLookupRecord::new,
        pageToken);
  }

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#listFullEntities} */
  @NonNull <T> Page<T> loadEntitiesInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull Predicate<PolarisBaseEntity> entityFilter,
      @NonNull Function<PolarisBaseEntity, T> transformer,
      @NonNull PageToken pageToken);

  /**
   * See {@link org.apache.polaris.core.persistence.BasePersistence#lookupEntityGrantRecordsVersion}
   */
  int lookupEntityGrantRecordsVersionInCurrentTxn(
      @NonNull PolarisCallContext callCtx, long catalogId, long entityId);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#lookupGrantRecord} */
  @Nullable PolarisGrantRecord lookupGrantRecordInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode);

  /**
   * See {@link org.apache.polaris.core.persistence.BasePersistence#loadAllGrantRecordsOnSecurable}
   */
  @NonNull List<PolarisGrantRecord> loadAllGrantRecordsOnSecurableInCurrentTxn(
      @NonNull PolarisCallContext callCtx, long securableCatalogId, long securableId);

  /**
   * See {@link org.apache.polaris.core.persistence.BasePersistence#loadAllGrantRecordsOnGrantee}
   */
  @NonNull List<PolarisGrantRecord> loadAllGrantRecordsOnGranteeInCurrentTxn(
      @NonNull PolarisCallContext callCtx, long granteeCatalogId, long granteeId);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#hasChildren} */
  boolean hasChildrenInCurrentTxn(
      @NonNull PolarisCallContext callContext,
      @Nullable PolarisEntityType optionalEntityType,
      long catalogId,
      long parentId);

  /** See {@link org.apache.polaris.core.persistence.IntegrationPersistence#loadPrincipalSecrets} */
  @Nullable PolarisPrincipalSecrets loadPrincipalSecretsInCurrentTxn(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId);

  /**
   * See {@link
   * org.apache.polaris.core.persistence.IntegrationPersistence#generateNewPrincipalSecrets}
   */
  @NonNull PolarisPrincipalSecrets generateNewPrincipalSecretsInCurrentTxn(
      @NonNull PolarisCallContext callCtx, @NonNull String principalName, long principalId);

  /**
   * See {@link org.apache.polaris.core.persistence.IntegrationPersistence#rotatePrincipalSecrets}
   */
  @Nullable PolarisPrincipalSecrets rotatePrincipalSecretsInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      @NonNull String clientId,
      long principalId,
      boolean reset,
      @NonNull String oldSecretHash);

  /**
   * See {@link org.apache.polaris.core.persistence.IntegrationPersistence#deletePrincipalSecrets}
   */
  void deletePrincipalSecretsInCurrentTxn(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId, long principalId);

  /**
   * See {@link org.apache.polaris.core.persistence.IntegrationPersistence#createStorageIntegration}
   */
  @Nullable <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> createStorageIntegrationInCurrentTxn(
          @NonNull PolarisCallContext callCtx,
          long catalogId,
          long entityId,
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo);

  /**
   * See {@link
   * org.apache.polaris.core.persistence.IntegrationPersistence#persistStorageIntegrationIfNeeded}
   */
  <T extends PolarisStorageConfigurationInfo> void persistStorageIntegrationIfNeededInCurrentTxn(
      @NonNull PolarisCallContext callContext,
      @NonNull PolarisBaseEntity entity,
      @Nullable PolarisStorageIntegration<T> storageIntegration);

  /**
   * See {@link
   * org.apache.polaris.core.persistence.IntegrationPersistence#loadPolarisStorageIntegration}
   */
  @Nullable <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegrationInCurrentTxn(
          @NonNull PolarisCallContext callContext, @NonNull PolarisBaseEntity entity);
}
