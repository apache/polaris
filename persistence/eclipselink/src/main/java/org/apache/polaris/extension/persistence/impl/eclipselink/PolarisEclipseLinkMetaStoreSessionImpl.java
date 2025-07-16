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
package org.apache.polaris.extension.persistence.impl.eclipselink;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import jakarta.persistence.OptimisticLockException;
import jakarta.persistence.PersistenceException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.persistence.pagination.EntityIdToken;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.transactional.AbstractTransactionalPersistence;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelEntity;
import org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelEntityActive;
import org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelEntityChangeTracking;
import org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelGrantRecord;
import org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelPolicyMappingRecord;
import org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelPrincipalSecrets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EclipseLink implementation of a Polaris metadata store supporting persisting and retrieving all
 * Polaris metadata from/to the configured database systems.
 */
public class PolarisEclipseLinkMetaStoreSessionImpl extends AbstractTransactionalPersistence {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisEclipseLinkMetaStoreSessionImpl.class);

  // Cache to hold the EntityManagerFactory for each realm. Each realm needs a separate
  // EntityManagerFactory since it connects to different databases
  private static final ConcurrentHashMap<String, EntityManagerFactory> realmFactories =
      new ConcurrentHashMap<>();
  private final EntityManagerFactory emf;

  // TODO this has to be refactored, see https://github.com/apache/polaris/issues/463 and
  //  https://errorprone.info/bugpattern/ThreadLocalUsage
  @SuppressWarnings("ThreadLocalUsage")
  private final ThreadLocal<EntityManager> localSession = new ThreadLocal<>();

  private final PolarisEclipseLinkStore store;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;
  private final PrincipalSecretsGenerator secretsGenerator;

  /**
   * Create a meta store session against provided realm. Each realm has its own database.
   *
   * @param store Backing store of EclipseLink implementation
   * @param storageIntegrationProvider Storage integration provider
   * @param realmContext Realm context used to communicate with different database.
   * @param confFile Optional EclipseLink configuration file. Default to 'META-INF/persistence.xml'.
   * @param persistenceUnitName Optional persistence-unit name in confFile. Default to 'polaris'.
   */
  public PolarisEclipseLinkMetaStoreSessionImpl(
      @Nonnull PolarisEclipseLinkStore store,
      @Nonnull PolarisStorageIntegrationProvider storageIntegrationProvider,
      @Nonnull RealmContext realmContext,
      @Nullable String confFile,
      @Nullable String persistenceUnitName,
      @Nonnull PrincipalSecretsGenerator secretsGenerator) {
    LOGGER.debug(
        "Creating EclipseLink Meta Store Session for realm {}", realmContext.getRealmIdentifier());
    emf = createEntityManagerFactory(realmContext, confFile, persistenceUnitName);

    // init store
    this.store = store;
    try (EntityManager session = emf.createEntityManager()) {
      this.store.initialize(session);
    }
    this.storageIntegrationProvider = storageIntegrationProvider;
    this.secretsGenerator = secretsGenerator;
  }

  /**
   * Create EntityManagerFactory.
   *
   * <p>The EntityManagerFactory creation is expensive, so we are caching and reusing it for each
   * realm.
   */
  private EntityManagerFactory createEntityManagerFactory(
      @Nonnull RealmContext realmContext,
      @Nullable String confFile,
      @Nullable String persistenceUnitName) {
    String realm = realmContext.getRealmIdentifier();
    return realmFactories.computeIfAbsent(
        realm,
        key -> {
          try {
            PolarisEclipseLinkPersistenceUnit persistenceUnit =
                PolarisEclipseLinkPersistenceUnit.locatePersistenceUnit(
                    confFile, persistenceUnitName);
            return persistenceUnit.createEntityManagerFactory(realmContext);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  @VisibleForTesting
  static void clearEntityManagerFactories() {
    realmFactories.clear();
  }

  /** {@inheritDoc} */
  @Override
  public <T> T runInTransaction(
      @Nonnull PolarisCallContext callCtx, @Nonnull Supplier<T> transactionCode) {
    callCtx.getDiagServices().check(localSession.get() == null, "cannot nest transaction");

    try (EntityManager session = emf.createEntityManager()) {
      localSession.set(session);
      EntityTransaction tr = session.getTransaction();
      try {
        tr.begin();

        T result = transactionCode.get();

        // Commit when it's not rolled back by the client
        if (session.getTransaction().isActive()) {
          tr.commit();
          LOGGER.debug("transaction committed");
        }

        return result;
      } catch (Exception e) {
        // For some transaction conflict errors, the transaction will already no longer be active;
        // if it's still active, explicitly rollback.
        if (tr.isActive()) {
          tr.rollback();
        }
        LOGGER.debug("transaction rolled back", e);

        if (e instanceof OptimisticLockException
            || e.getCause() instanceof OptimisticLockException) {
          throw new RetryOnConcurrencyException(e);
        }

        throw e;
      } finally {
        localSession.remove();
      }
    } catch (PersistenceException e) {
      if (e.toString().toLowerCase(Locale.ROOT).contains("duplicate key")) {
        throw new AlreadyExistsException("Duplicate key error when persisting entity", e);
      } else {
        throw new RuntimeException("Error persisting entity", e);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void runActionInTransaction(
      @Nonnull PolarisCallContext callCtx, @Nonnull Runnable transactionCode) {
    callCtx.getDiagServices().check(localSession.get() == null, "cannot nest transaction");

    try (EntityManager session = emf.createEntityManager()) {
      localSession.set(session);
      EntityTransaction tr = session.getTransaction();
      try {
        tr.begin();

        transactionCode.run();

        // Commit when it's not rolled back by the client
        if (session.getTransaction().isActive()) {
          tr.commit();
          LOGGER.debug("transaction committed");
        }
      } catch (Exception e) {
        LOGGER.debug("Rolling back transaction due to an error", e);
        // For some transaction conflict errors, the transaction will already no longer be active;
        // if it's still active, explicitly rollback.
        if (tr.isActive()) {
          tr.rollback();
        }

        if (e instanceof OptimisticLockException
            || e.getCause() instanceof OptimisticLockException) {
          throw new RetryOnConcurrencyException(e);
        }

        throw e;
      } finally {
        localSession.remove();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> T runInReadTransaction(
      @Nonnull PolarisCallContext callCtx, @Nonnull Supplier<T> transactionCode) {
    // EclipseLink doesn't support readOnly transaction
    return runInTransaction(callCtx, transactionCode);
  }

  /** {@inheritDoc} */
  @Override
  public void runActionInReadTransaction(
      @Nonnull PolarisCallContext callCtx, @Nonnull Runnable transactionCode) {
    // EclipseLink doesn't support readOnly transaction
    runActionInTransaction(callCtx, transactionCode);
  }

  /**
   * @return new unique entity identifier
   */
  @Override
  public long generateNewIdInCurrentTxn(@Nonnull PolarisCallContext callCtx) {
    // This function can be called within a transaction or out of transaction.
    // If called out of transaction, create a new transaction, otherwise run in current transaction
    return localSession.get() != null
        ? this.store.getNextSequence(localSession.get())
        : runInReadTransaction(callCtx, () -> generateNewIdInCurrentTxn(callCtx));
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    this.store.writeToEntities(localSession.get(), entity);
  }

  /** {@inheritDoc} */
  @Override
  public <T extends PolarisStorageConfigurationInfo>
      void persistStorageIntegrationIfNeededInCurrentTxn(
          @Nonnull PolarisCallContext callContext,
          @Nonnull PolarisBaseEntity entity,
          @Nullable PolarisStorageIntegration<T> storageIntegration) {
    // not implemented for eclipselink store
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntitiesActiveInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    // write it
    this.store.writeToEntitiesActive(localSession.get(), entity);
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntitiesChangeTrackingInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    // write it
    this.store.writeToEntitiesChangeTracking(localSession.get(), entity);
  }

  /** {@inheritDoc} */
  @Override
  public void writeToGrantRecordsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {
    // write it
    this.store.writeToGrantRecords(localSession.get(), grantRec);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore entity) {

    // delete it
    this.store.deleteFromEntities(
        localSession.get(), entity.getCatalogId(), entity.getId(), entity.getTypeCode());
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromEntitiesActiveInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore entity) {
    // delete it
    this.store.deleteFromEntitiesActive(localSession.get(), new PolarisEntitiesActiveKey(entity));
  }

  /**
   * {@inheritDoc}
   *
   * @param callCtx
   * @param entity entity record to delete
   */
  @Override
  public void deleteFromEntitiesChangeTrackingInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore entity) {
    // delete it
    this.store.deleteFromEntitiesChangeTracking(localSession.get(), entity);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromGrantRecordsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {
    this.store.deleteFromGrantRecords(localSession.get(), grantRec);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAllEntityGrantRecordsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore entity,
      @Nonnull List<PolarisGrantRecord> grantsOnGrantee,
      @Nonnull List<PolarisGrantRecord> grantsOnSecurable) {
    this.store.deleteAllEntityGrantRecords(localSession.get(), entity);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAllInCurrentTxn(@Nonnull PolarisCallContext callCtx) {
    this.store.deleteAll(localSession.get());
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisBaseEntity lookupEntityInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId, int typeCode) {
    return ModelEntity.toEntity(
        this.store.lookupEntity(localSession.get(), catalogId, entityId, typeCode));
  }

  @Override
  public @Nonnull List<PolarisBaseEntity> lookupEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    return this.store.lookupEntities(localSession.get(), entityIds).stream()
        .map(ModelEntity::toEntity)
        .toList();
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisChangeTrackingVersions> lookupEntityVersionsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    Map<PolarisEntityId, ModelEntity> idToEntityMap =
        this.store.lookupEntities(localSession.get(), entityIds).stream()
            .collect(
                Collectors.toMap(
                    entry -> new PolarisEntityId(entry.getCatalogId(), entry.getId()),
                    entry -> entry));
    return entityIds.stream()
        .map(
            entityId -> {
              ModelEntity entity = idToEntityMap.getOrDefault(entityId, null);
              return entity == null
                  ? null
                  : new PolarisChangeTrackingVersions(
                      entity.getEntityVersion(), entity.getGrantRecordsVersion());
            })
        .collect(Collectors.toList());
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public EntityNameLookupRecord lookupEntityActiveInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntitiesActiveKey entityActiveKey) {
    // lookup the active entity slice
    return ModelEntityActive.toEntityActive(
        this.store.lookupEntityActive(localSession.get(), entityActiveKey));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<EntityNameLookupRecord> lookupEntityActiveBatchInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisEntitiesActiveKey> entityActiveKeys) {
    // now build a list to quickly verify that nothing has changed
    return entityActiveKeys.stream()
        .map(entityActiveKey -> this.lookupEntityActiveInCurrentTxn(callCtx, entityActiveKey))
        .collect(Collectors.toList());
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull Page<EntityNameLookupRecord> listEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PageToken pageToken) {
    return this.listEntitiesInCurrentTxn(
        callCtx, catalogId, parentId, entityType, Predicates.alwaysTrue(), pageToken);
  }

  @Override
  public @Nonnull Page<EntityNameLookupRecord> listEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull PageToken pageToken) {
    // full range scan under the parent for that type
    return this.listEntitiesInCurrentTxn(
        callCtx,
        catalogId,
        parentId,
        entityType,
        entityFilter,
        entity ->
            new EntityNameLookupRecord(
                entity.getCatalogId(),
                entity.getId(),
                entity.getParentId(),
                entity.getName(),
                entity.getTypeCode(),
                entity.getSubTypeCode()),
        pageToken);
  }

  @Override
  public @Nonnull <T> Page<T> listEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull Function<PolarisBaseEntity, T> transformer,
      @Nonnull PageToken pageToken) {
    // full range scan under the parent for that type
    Stream<PolarisBaseEntity> data =
        this.store
            .lookupFullEntitiesActive(
                localSession.get(), catalogId, parentId, entityType, pageToken)
            .stream()
            .map(ModelEntity::toEntity)
            .filter(entityFilter);

    return Page.mapped(pageToken, data, transformer, EntityIdToken::fromEntity);
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasChildrenInCurrentTxn(
      @Nonnull PolarisCallContext callContext,
      @Nullable PolarisEntityType entityType,
      long catalogId,
      long parentId) {
    // check if it has children
    return this.store.countActiveChildEntities(localSession.get(), catalogId, parentId, entityType)
        > 0;
  }

  /** {@inheritDoc} */
  @Override
  public int lookupEntityGrantRecordsVersionInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId) {
    ModelEntityChangeTracking entity =
        this.store.lookupEntityChangeTracking(localSession.get(), catalogId, entityId);

    // does not exist, 0
    return entity == null ? 0 : entity.getGrantRecordsVersion();
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisGrantRecord lookupGrantRecordInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    // lookup the grants records slice to find the usage role
    return ModelGrantRecord.toGrantRecord(
        this.store.lookupGrantRecord(
            localSession.get(),
            securableCatalogId,
            securableId,
            granteeCatalogId,
            granteeId,
            privilegeCode));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisGrantRecord> loadAllGrantRecordsOnSecurableInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    // now fetch all grants for this securable
    return this.store
        .lookupAllGrantRecordsOnSecurable(localSession.get(), securableCatalogId, securableId)
        .stream()
        .map(ModelGrantRecord::toGrantRecord)
        .toList();
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisGrantRecord> loadAllGrantRecordsOnGranteeInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    // now fetch all grants assigned to this grantee
    return this.store
        .lookupGrantRecordsOnGrantee(localSession.get(), granteeCatalogId, granteeId)
        .stream()
        .map(ModelGrantRecord::toGrantRecord)
        .toList();
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisPrincipalSecrets loadPrincipalSecretsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    return ModelPrincipalSecrets.toPrincipalSecrets(
        this.store.lookupPrincipalSecrets(localSession.get(), clientId));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PolarisPrincipalSecrets generateNewPrincipalSecretsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull String principalName, long principalId) {
    // ensure principal client id is unique
    PolarisPrincipalSecrets principalSecrets;
    ModelPrincipalSecrets lookupPrincipalSecrets;
    do {
      // generate new random client id and secrets
      principalSecrets = secretsGenerator.produceSecrets(principalName, principalId);

      // load the existing secrets
      lookupPrincipalSecrets =
          this.store.lookupPrincipalSecrets(
              localSession.get(), principalSecrets.getPrincipalClientId());
    } while (lookupPrincipalSecrets != null);

    // write new principal secrets
    this.store.writePrincipalSecrets(localSession.get(), principalSecrets);

    // if not found, return null
    return principalSecrets;
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PolarisPrincipalSecrets rotatePrincipalSecretsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {

    // load the existing secrets
    PolarisPrincipalSecrets principalSecrets =
        ModelPrincipalSecrets.toPrincipalSecrets(
            this.store.lookupPrincipalSecrets(localSession.get(), clientId));

    // should be found
    callCtx
        .getDiagServices()
        .checkNotNull(
            principalSecrets,
            "cannot_find_secrets",
            "client_id={} principalId={}",
            clientId,
            principalId);

    // ensure principal id is matching
    callCtx
        .getDiagServices()
        .check(
            principalId == principalSecrets.getPrincipalId(),
            "principal_id_mismatch",
            "expectedId={} id={}",
            principalId,
            principalSecrets.getPrincipalId());

    // rotate the secrets
    principalSecrets.rotateSecrets(oldSecretHash);
    if (reset) {
      principalSecrets.rotateSecrets(principalSecrets.getMainSecretHash());
    }

    // write back new secrets
    this.store.writePrincipalSecrets(localSession.get(), principalSecrets);

    // return those
    return principalSecrets;
  }

  /** {@inheritDoc} */
  @Override
  public void deletePrincipalSecretsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId, long principalId) {
    // load the existing secrets
    ModelPrincipalSecrets principalSecrets =
        this.store.lookupPrincipalSecrets(localSession.get(), clientId);

    // should be found
    callCtx
        .getDiagServices()
        .checkNotNull(
            principalSecrets,
            "cannot_find_secrets",
            "client_id={} principalId={}",
            clientId,
            principalId);

    // ensure principal id is matching
    callCtx
        .getDiagServices()
        .check(
            principalId == principalSecrets.getPrincipalId(),
            "principal_id_mismatch",
            "expectedId={} id={}",
            principalId,
            principalSecrets.getPrincipalId());

    // delete these secrets
    this.store.deletePrincipalSecrets(localSession.get(), clientId);
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> createStorageIntegrationInCurrentTxn(
          @Nonnull PolarisCallContext callCtx,
          long catalogId,
          long entityId,
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    return storageIntegrationProvider.getStorageIntegrationForConfig(
        polarisStorageConfigurationInfo);
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegrationInCurrentTxn(
          @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    PolarisStorageConfigurationInfo storageConfig =
        BaseMetaStoreManager.extractStorageConfiguration(callCtx, entity);
    return storageIntegrationProvider.getStorageIntegrationForConfig(storageConfig);
  }

  /** {@inheritDoc} */
  @Override
  public void writeToPolicyMappingRecordsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisPolicyMappingRecord record) {

    this.store.writeToPolicyMappingRecords(localSession.get(), record);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromPolicyMappingRecordsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisPolicyMappingRecord record) {
    this.store.deleteFromPolicyMappingRecords(localSession.get(), record);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAllEntityPolicyMappingRecordsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      @Nonnull List<PolarisPolicyMappingRecord> mappingOnTarget,
      @Nonnull List<PolarisPolicyMappingRecord> mappingOnPolicy) {
    this.store.deleteAllEntityPolicyMappingRecords(localSession.get(), entity);
  }

  /** {@inheritDoc} */
  @Nullable
  @Override
  public PolarisPolicyMappingRecord lookupPolicyMappingRecordInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int policyTypeCode,
      long policyCatalogId,
      long policyId) {
    return ModelPolicyMappingRecord.toPolicyMappingRecord(
        this.store.lookupPolicyMappingRecord(
            localSession.get(),
            targetCatalogId,
            targetId,
            policyTypeCode,
            policyCatalogId,
            policyId));
  }

  /** {@inheritDoc} */
  @Nonnull
  @Override
  public List<PolarisPolicyMappingRecord> loadPoliciesOnTargetByTypeInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int policyTypeCode) {
    return this.store
        .loadPoliciesOnTargetByType(localSession.get(), targetCatalogId, targetId, policyTypeCode)
        .stream()
        .map(ModelPolicyMappingRecord::toPolicyMappingRecord)
        .toList();
  }

  /** {@inheritDoc} */
  @Nonnull
  @Override
  public List<PolarisPolicyMappingRecord> loadAllPoliciesOnTargetInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, long targetCatalogId, long targetId) {
    return this.store
        .loadAllPoliciesOnTarget(localSession.get(), targetCatalogId, targetId)
        .stream()
        .map(ModelPolicyMappingRecord::toPolicyMappingRecord)
        .toList();
  }

  /** {@inheritDoc} */
  @Nonnull
  @Override
  public List<PolarisPolicyMappingRecord> loadAllTargetsOnPolicyInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      long policyCatalogId,
      long policyId,
      int policyTypeCode) {
    return this.store
        .loadAllTargetsOnPolicy(localSession.get(), policyCatalogId, policyId, policyTypeCode)
        .stream()
        .map(ModelPolicyMappingRecord::toPolicyMappingRecord)
        .toList();
  }

  @Override
  public void rollback() {
    EntityManager session = localSession.get();
    if (session != null) {
      session.getTransaction().rollback();
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T extends PolarisEntity & LocationBasedEntity>
      Optional<Optional<String>> hasOverlappingSiblings(
          @Nonnull PolarisCallContext callContext, T entity) {
    return Optional.empty();
  }
}
