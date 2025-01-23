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

import com.google.common.base.Predicates;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisDiagnostics;
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
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;

public class PolarisTreeMapMetaStoreSessionImpl implements PolarisMetaStoreSession {

  // the TreeMap store to use
  private final PolarisTreeMapStore store;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;
  private final PrincipalSecretsGenerator secretsGenerator;
  private final PolarisDiagnostics diagnostics;

  public PolarisTreeMapMetaStoreSessionImpl(
      @Nonnull PolarisTreeMapStore store,
      @Nonnull PolarisStorageIntegrationProvider storageIntegrationProvider,
      @Nonnull PrincipalSecretsGenerator secretsGenerator,
      @Nonnull PolarisDiagnostics diagnostics) {

    // init store
    this.store = store;
    this.storageIntegrationProvider = storageIntegrationProvider;
    this.secretsGenerator = secretsGenerator;
    this.diagnostics = diagnostics;
  }

  /** {@inheritDoc} */
  @Override
  public <T> T runInTransaction(@Nonnull Supplier<T> transactionCode) {

    // run transaction on our underlying store
    return store.runInTransaction(transactionCode);
  }

  /** {@inheritDoc} */
  @Override
  public void runActionInTransaction(@Nonnull Runnable transactionCode) {

    // run transaction on our underlying store
    store.runActionInTransaction(transactionCode);
  }

  /** {@inheritDoc} */
  @Override
  public <T> T runInReadTransaction(@Nonnull Supplier<T> transactionCode) {
    // run transaction on our underlying store
    return store.runInReadTransaction(transactionCode);
  }

  /** {@inheritDoc} */
  @Override
  public void runActionInReadTransaction(@Nonnull Runnable transactionCode) {

    // run transaction on our underlying store
    store.runActionInReadTransaction(transactionCode);
  }

  /**
   * @return new unique entity identifier
   */
  @Override
  public long generateNewId() {
    return this.store.getNextSequence();
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntities(@Nonnull PolarisBaseEntity entity) {
    // write it
    this.store.getSliceEntities().write(entity);
  }

  /** {@inheritDoc} */
  @Override
  public <T extends PolarisStorageConfigurationInfo> void persistStorageIntegrationIfNeeded(
      @Nonnull PolarisBaseEntity entity,
      @Nullable PolarisStorageIntegration<T> storageIntegration) {
    // not implemented for in-memory store
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntitiesActive(@Nonnull PolarisBaseEntity entity) {
    // write it
    this.store.getSliceEntitiesActive().write(entity);
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntitiesDropped(@Nonnull PolarisBaseEntity entity) {
    // write it
    this.store.getSliceEntitiesDropped().write(entity);
    this.store.getSliceEntitiesDroppedToPurge().write(entity);
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntitiesChangeTracking(@Nonnull PolarisBaseEntity entity) {
    // write it
    this.store.getSliceEntitiesChangeTracking().write(entity);
  }

  /** {@inheritDoc} */
  @Override
  public void writeToGrantRecords(@Nonnull PolarisGrantRecord grantRec) {
    // write it
    this.store.getSliceGrantRecords().write(grantRec);
    this.store.getSliceGrantRecordsByGrantee().write(grantRec);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromEntities(@Nonnull PolarisEntityCore entity) {

    // delete it
    this.store.getSliceEntities().delete(this.store.buildEntitiesKey(entity));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromEntitiesActive(@Nonnull PolarisEntityCore entity) {
    // delete it
    this.store.getSliceEntitiesActive().delete(this.store.buildEntitiesActiveKey(entity));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromEntitiesDropped(@Nonnull PolarisBaseEntity entity) {
    // delete it
    this.store.getSliceEntitiesDropped().delete(entity);
    this.store.getSliceEntitiesDroppedToPurge().delete(entity);
  }

  /**
   * {@inheritDoc}
   *
   * @param entity entity record to delete
   */
  @Override
  public void deleteFromEntitiesChangeTracking(@Nonnull PolarisEntityCore entity) {
    // delete it
    this.store.getSliceEntitiesChangeTracking().delete(this.store.buildEntitiesKey(entity));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromGrantRecords(@Nonnull PolarisGrantRecord grantRec) {

    // delete it
    this.store.getSliceGrantRecords().delete(grantRec);
    this.store.getSliceGrantRecordsByGrantee().delete(grantRec);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAllEntityGrantRecords(
      @Nonnull PolarisEntityCore entity,
      @Nonnull List<PolarisGrantRecord> grantsOnGrantee,
      @Nonnull List<PolarisGrantRecord> grantsOnSecurable) {

    // build composite prefix key and delete grant records on the indexed side of each grant table
    String prefix = this.store.buildPrefixKeyComposite(entity.getCatalogId(), entity.getId());
    this.store.getSliceGrantRecords().deleteRange(prefix);
    this.store.getSliceGrantRecordsByGrantee().deleteRange(prefix);

    // also delete the other side. We need to delete these grants one at a time versus doing a
    // range delete
    grantsOnGrantee.forEach(gr -> this.store.getSliceGrantRecords().delete(gr));
    grantsOnSecurable.forEach(gr -> this.store.getSliceGrantRecordsByGrantee().delete(gr));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAll() {
    // clear all slices
    this.store.deleteAll();
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisBaseEntity lookupEntity(long catalogId, long entityId) {
    return this.store.getSliceEntities().read(this.store.buildKeyComposite(catalogId, entityId));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisBaseEntity> lookupEntities(List<PolarisEntityId> entityIds) {
    // allocate return list
    return entityIds.stream()
        .map(
            id ->
                this.store
                    .getSliceEntities()
                    .read(this.store.buildKeyComposite(id.getCatalogId(), id.getId())))
        .collect(Collectors.toList());
  }

  /** {@inheritDoc} */
  @Override
  public int lookupEntityVersion(long catalogId, long entityId) {
    PolarisBaseEntity baseEntity =
        this.store
            .getSliceEntitiesChangeTracking()
            .read(this.store.buildKeyComposite(catalogId, entityId));

    return baseEntity == null ? 0 : baseEntity.getEntityVersion();
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisChangeTrackingVersions> lookupEntityVersions(
      List<PolarisEntityId> entityIds) {
    // allocate return list
    return entityIds.stream()
        .map(
            id ->
                this.store
                    .getSliceEntitiesChangeTracking()
                    .read(this.store.buildKeyComposite(id.getCatalogId(), id.getId())))
        .map(
            entity ->
                (entity != null)
                    ? new PolarisChangeTrackingVersions(
                        entity.getEntityVersion(), entity.getGrantRecordsVersion())
                    : null)
        .collect(Collectors.toList());
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public PolarisEntityActiveRecord lookupEntityActive(
      @Nonnull PolarisEntitiesActiveKey entityActiveKey) {
    // lookup the active entity slice
    PolarisBaseEntity entity =
        this.store
            .getSliceEntitiesActive()
            .read(
                this.store.buildKeyComposite(
                    entityActiveKey.getCatalogId(),
                    entityActiveKey.getParentId(),
                    entityActiveKey.getTypeCode(),
                    entityActiveKey.getName()));

    // return record
    return (entity == null)
        ? null
        : new PolarisEntityActiveRecord(
            entity.getCatalogId(),
            entity.getId(),
            entity.getParentId(),
            entity.getName(),
            entity.getTypeCode(),
            entity.getSubTypeCode());
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<PolarisEntityActiveRecord> lookupEntityActiveBatch(
      @Nonnull List<PolarisEntitiesActiveKey> entityActiveKeys) {
    // now build a list to quickly verify that nothing has changed
    return entityActiveKeys.stream()
        .map(entityActiveKey -> this.lookupEntityActive(entityActiveKey))
        .collect(Collectors.toList());
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisEntityActiveRecord> listActiveEntities(
      long catalogId, long parentId, @Nonnull PolarisEntityType entityType) {
    return listActiveEntities(catalogId, parentId, entityType, Predicates.alwaysTrue());
  }

  @Override
  public @Nonnull List<PolarisEntityActiveRecord> listActiveEntities(
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter) {
    // full range scan under the parent for that type
    return listActiveEntities(
        catalogId,
        parentId,
        entityType,
        Integer.MAX_VALUE,
        entityFilter,
        entity ->
            new PolarisEntityActiveRecord(
                entity.getCatalogId(),
                entity.getId(),
                entity.getParentId(),
                entity.getName(),
                entity.getTypeCode(),
                entity.getSubTypeCode()));
  }

  @Override
  public @Nonnull <T> List<T> listActiveEntities(
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      int limit,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull Function<PolarisBaseEntity, T> transformer) {
    // full range scan under the parent for that type
    return this.store
        .getSliceEntitiesActive()
        .readRange(this.store.buildPrefixKeyComposite(catalogId, parentId, entityType.getCode()))
        .stream()
        .filter(entityFilter)
        .limit(limit)
        .map(transformer)
        .collect(Collectors.toList());
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasChildren(
      @Nullable PolarisEntityType entityType, long catalogId, long parentId) {
    // determine key prefix, add type if one is passed-in
    String prefixKey =
        entityType == null
            ? this.store.buildPrefixKeyComposite(catalogId, parentId)
            : this.store.buildPrefixKeyComposite(catalogId, parentId, entityType.getCode());
    // check if it has children
    return !this.store.getSliceEntitiesActive().readRange(prefixKey).isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public int lookupEntityGrantRecordsVersion(long catalogId, long entityId) {
    PolarisBaseEntity entity =
        this.store
            .getSliceEntitiesChangeTracking()
            .read(this.store.buildKeyComposite(catalogId, entityId));

    // does not exist, 0
    return entity == null ? 0 : entity.getGrantRecordsVersion();
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisGrantRecord lookupGrantRecord(
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    // lookup the grants records slice to find the usage role
    return this.store
        .getSliceGrantRecords()
        .read(
            this.store.buildKeyComposite(
                securableCatalogId, securableId, granteeCatalogId, granteeId, privilegeCode));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      long securableCatalogId, long securableId) {
    // now fetch all grants for this securable
    return this.store
        .getSliceGrantRecords()
        .readRange(this.store.buildPrefixKeyComposite(securableCatalogId, securableId));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      long granteeCatalogId, long granteeId) {
    // now fetch all grants assigned to this grantee
    return this.store
        .getSliceGrantRecordsByGrantee()
        .readRange(this.store.buildPrefixKeyComposite(granteeCatalogId, granteeId));
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisPrincipalSecrets loadPrincipalSecrets(@Nonnull String clientId) {
    return this.store.getSlicePrincipalSecrets().read(clientId);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PolarisPrincipalSecrets generateNewPrincipalSecrets(
      @Nonnull String principalName, long principalId) {
    // ensure principal client id is unique
    PolarisPrincipalSecrets principalSecrets;
    PolarisPrincipalSecrets lookupPrincipalSecrets;
    do {
      // generate new random client id and secrets
      principalSecrets = secretsGenerator.produceSecrets(principalName, principalId);

      // load the existing secrets
      lookupPrincipalSecrets =
          this.store.getSlicePrincipalSecrets().read(principalSecrets.getPrincipalClientId());
    } while (lookupPrincipalSecrets != null);

    // write new principal secrets
    this.store.getSlicePrincipalSecrets().write(principalSecrets);

    // if not found, return null
    return principalSecrets;
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PolarisPrincipalSecrets rotatePrincipalSecrets(
      @Nonnull String clientId, long principalId, boolean reset, @Nonnull String oldSecretHash) {

    // load the existing secrets
    PolarisPrincipalSecrets principalSecrets = this.store.getSlicePrincipalSecrets().read(clientId);

    // should be found
    diagnostics.checkNotNull(
        principalSecrets,
        "cannot_find_secrets",
        "client_id={} principalId={}",
        clientId,
        principalId);

    // ensure principal id is matching
    diagnostics.check(
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
    this.store.getSlicePrincipalSecrets().write(principalSecrets);

    // return those
    return principalSecrets;
  }

  /** {@inheritDoc} */
  @Override
  public void deletePrincipalSecrets(@Nonnull String clientId, long principalId) {
    // load the existing secrets
    PolarisPrincipalSecrets principalSecrets = this.store.getSlicePrincipalSecrets().read(clientId);

    // should be found
    diagnostics.checkNotNull(
        principalSecrets,
        "cannot_find_secrets",
        "client_id={} principalId={}",
        clientId,
        principalId);

    // ensure principal id is matching
    diagnostics.check(
        principalId == principalSecrets.getPrincipalId(),
        "principal_id_mismatch",
        "expectedId={} id={}",
        principalId,
        principalSecrets.getPrincipalId());

    // delete these secrets
    this.store.getSlicePrincipalSecrets().delete(clientId);
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> createStorageIntegration(
          long catalogId,
          long entityId,
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    return storageIntegrationProvider.getStorageIntegrationForConfig(
        polarisStorageConfigurationInfo);
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegration(
          @Nonnull PolarisBaseEntity entity) {
    PolarisStorageConfigurationInfo storageConfig =
        PolarisMetaStoreManagerImpl.readStorageConfiguration(diagnostics, entity);
    return storageIntegrationProvider.getStorageIntegrationForConfig(storageConfig);
  }

  @Override
  public void rollback() {
    this.store.rollback();
  }
}
