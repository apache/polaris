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
import java.util.function.Supplier;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
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
   * @param transactionCode code of the transaction being executed, a supplier lambda
   */
  <T> T runInTransaction(@Nonnull Supplier<T> transactionCode);

  /**
   * Run the specified transaction code (a runnable lambda type) in a database read/write
   * transaction. If the code of the transaction does not throw any exception and returns normally,
   * the transaction will be committed, else the transaction will be automatically rolled-back on
   * error.
   *
   * @param transactionCode code of the transaction being executed, a runnable lambda
   */
  void runActionInTransaction(@Nonnull Runnable transactionCode);

  /**
   * Run the specified transaction code (a Supplier lambda type) in a database read transaction. If
   * the code of the transaction does not throw any exception and returns normally, the transaction
   * will be committed, else the transaction will be automatically rolled-back on error. The result
   * of the supplier lambda is returned if success, else the error will be re-thrown.
   *
   * @param transactionCode code of the transaction being executed, a supplier lambda
   */
  <T> T runInReadTransaction(@Nonnull Supplier<T> transactionCode);

  /**
   * Run the specified transaction code (a runnable lambda type) in a database read transaction. If
   * the code of the transaction does not throw any exception and returns normally, the transaction
   * will be committed, else the transaction will be automatically rolled-back on error.
   *
   * @param transactionCode code of the transaction being executed, a runnable lambda
   */
  void runActionInReadTransaction(@Nonnull Runnable transactionCode);

  /**
   * Lookup the specified set of entities by entityActiveKeys Return the result, a parallel list of
   * active records. A record in that list will be null if its associated lookup failed
   *
   * @return the list of entityActiveKeys for the specified lookup operation
   */
  @Nonnull
  List<EntityNameLookupRecord> lookupEntityActiveBatchInCurrentTxn(
      List<PolarisEntitiesActiveKey> entityActiveKeys);

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
  long generateNewIdInCurrentTxn();

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
      @Nonnull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#writeEntities} */
  void writeEntitiesInCurrentTxn(
      @Nonnull List<PolarisBaseEntity> entities,
      @Nullable List<PolarisBaseEntity> originalEntities);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#writeToGrantRecords} */
  void writeToGrantRecordsInCurrentTxn(@Nonnull PolarisGrantRecord grantRec);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#deleteEntity} */
  void deleteEntityInCurrentTxn(@Nonnull PolarisBaseEntity entity);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#deleteFromGrantRecords} */
  void deleteFromGrantRecordsInCurrentTxn(@Nonnull PolarisGrantRecord grantRec);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#deleteAllEntityGrantRecords} */
  void deleteAllEntityGrantRecordsInCurrentTxn(
      @Nonnull PolarisEntityCore entity,
      @Nonnull List<PolarisGrantRecord> grantsOnGrantee,
      @Nonnull List<PolarisGrantRecord> grantsOnSecurable);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#deleteAll} */
  void deleteAllInCurrentTxn();

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#lookupEntity} */
  @Nullable
  PolarisBaseEntity lookupEntityInCurrentTxn(long catalogId, long entityId, int typeCode);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#lookupEntityByName} */
  @Nullable
  PolarisBaseEntity lookupEntityByNameInCurrentTxn(
      long catalogId, long parentId, int typeCode, @Nonnull String name);

  /**
   * See {@link org.apache.polaris.core.persistence.BasePersistence#lookupEntityIdAndSubTypeByName}
   */
  @Nullable
  EntityNameLookupRecord lookupEntityIdAndSubTypeByNameInCurrentTxn(
      long catalogId, long parentId, int typeCode, @Nonnull String name);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#lookupEntities} */
  @Nonnull
  List<PolarisBaseEntity> lookupEntitiesInCurrentTxn(List<PolarisEntityId> entityIds);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#lookupEntityVersions} */
  @Nonnull
  List<PolarisChangeTrackingVersions> lookupEntityVersionsInCurrentTxn(
      List<PolarisEntityId> entityIds);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#listEntities} */
  @Nonnull
  Page<EntityNameLookupRecord> listEntitiesInCurrentTxn(
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PageToken pageToken);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#listEntities} */
  @Nonnull
  Page<EntityNameLookupRecord> listEntitiesInCurrentTxn(
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull PageToken pageToken);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#listEntities} */
  @Nonnull
  <T> Page<T> listEntitiesInCurrentTxn(
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull Function<PolarisBaseEntity, T> transformer,
      @Nonnull PageToken pageToken);

  /**
   * See {@link org.apache.polaris.core.persistence.BasePersistence#lookupEntityGrantRecordsVersion}
   */
  int lookupEntityGrantRecordsVersionInCurrentTxn(long catalogId, long entityId);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#lookupGrantRecord} */
  @Nullable
  PolarisGrantRecord lookupGrantRecordInCurrentTxn(
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode);

  /**
   * See {@link org.apache.polaris.core.persistence.BasePersistence#loadAllGrantRecordsOnSecurable}
   */
  @Nonnull
  List<PolarisGrantRecord> loadAllGrantRecordsOnSecurableInCurrentTxn(
      long securableCatalogId, long securableId);

  /**
   * See {@link org.apache.polaris.core.persistence.BasePersistence#loadAllGrantRecordsOnGrantee}
   */
  @Nonnull
  List<PolarisGrantRecord> loadAllGrantRecordsOnGranteeInCurrentTxn(
      long granteeCatalogId, long granteeId);

  /** See {@link org.apache.polaris.core.persistence.BasePersistence#hasChildren} */
  boolean hasChildrenInCurrentTxn(
      @Nullable PolarisEntityType optionalEntityType, long catalogId, long parentId);

  /** See {@link org.apache.polaris.core.persistence.IntegrationPersistence#loadPrincipalSecrets} */
  @Nullable
  PolarisPrincipalSecrets loadPrincipalSecretsInCurrentTxn(@Nonnull String clientId);

  /**
   * See {@link
   * org.apache.polaris.core.persistence.IntegrationPersistence#generateNewPrincipalSecrets}
   */
  @Nonnull
  PolarisPrincipalSecrets generateNewPrincipalSecretsInCurrentTxn(
      @Nonnull String principalName, long principalId);

  /**
   * See {@link org.apache.polaris.core.persistence.IntegrationPersistence#rotatePrincipalSecrets}
   */
  @Nullable
  PolarisPrincipalSecrets rotatePrincipalSecretsInCurrentTxn(
      @Nonnull String clientId, long principalId, boolean reset, @Nonnull String oldSecretHash);

  /**
   * See {@link org.apache.polaris.core.persistence.IntegrationPersistence#deletePrincipalSecrets}
   */
  void deletePrincipalSecretsInCurrentTxn(@Nonnull String clientId, long principalId);

  /**
   * See {@link org.apache.polaris.core.persistence.IntegrationPersistence#createStorageIntegration}
   */
  @Nullable
  <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> createStorageIntegrationInCurrentTxn(
          long catalogId,
          long entityId,
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo);

  /**
   * See {@link
   * org.apache.polaris.core.persistence.IntegrationPersistence#persistStorageIntegrationIfNeeded}
   */
  <T extends PolarisStorageConfigurationInfo> void persistStorageIntegrationIfNeededInCurrentTxn(
      @Nonnull PolarisBaseEntity entity, @Nullable PolarisStorageIntegration<T> storageIntegration);

  /**
   * See {@link
   * org.apache.polaris.core.persistence.IntegrationPersistence#loadPolarisStorageIntegration}
   */
  @Nullable
  <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegrationInCurrentTxn(
          @Nonnull PolarisBaseEntity entity);
}
