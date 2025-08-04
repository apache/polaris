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

import com.google.common.base.Predicates;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.pagination.EntityIdToken;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.StorageLocation;

public class TreeMapTransactionalPersistenceImpl extends AbstractTransactionalPersistence {

  // the TreeMap store to use
  private final TreeMapMetaStore store;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;
  private final PrincipalSecretsGenerator secretsGenerator;

  public TreeMapTransactionalPersistenceImpl(
      @Nonnull PolarisDiagnostics diagnostics,
      @Nonnull TreeMapMetaStore store,
      @Nonnull PolarisStorageIntegrationProvider storageIntegrationProvider,
      @Nonnull PrincipalSecretsGenerator secretsGenerator) {
    super(diagnostics);

    // init store
    this.store = store;
    this.storageIntegrationProvider = storageIntegrationProvider;
    this.secretsGenerator = secretsGenerator;
  }

  /** {@inheritDoc} */
  @Override
  public <T> T runInTransaction(@Nonnull Supplier<T> transactionCode) {

    // run transaction on our underlying store
    return store.runInTransaction(diagnostics, transactionCode);
  }

  /** {@inheritDoc} */
  @Override
  public void runActionInTransaction(@Nonnull Runnable transactionCode) {

    // run transaction on our underlying store
    store.runActionInTransaction(diagnostics, transactionCode);
  }

  /** {@inheritDoc} */
  @Override
  public <T> T runInReadTransaction(@Nonnull Supplier<T> transactionCode) {
    // run transaction on our underlying store
    return store.runInReadTransaction(diagnostics, transactionCode);
  }

  /** {@inheritDoc} */
  @Override
  public void runActionInReadTransaction(@Nonnull Runnable transactionCode) {

    // run transaction on our underlying store
    store.runActionInReadTransaction(diagnostics, transactionCode);
  }

  /**
   * @return new unique entity identifier
   */
  @Override
  public long generateNewIdInCurrentTxn() {
    return this.store.getNextSequence();
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntitiesInCurrentTxn(@Nonnull PolarisBaseEntity entity) {
    // write it
    this.store.getSliceEntities().write(entity);
  }

  /** {@inheritDoc} */
  @Override
  public <T extends PolarisStorageConfigurationInfo>
      void persistStorageIntegrationIfNeededInCurrentTxn(
          @Nonnull PolarisBaseEntity entity,
          @Nullable PolarisStorageIntegration<T> storageIntegration) {
    // not implemented for in-memory store
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntitiesActiveInCurrentTxn(@Nonnull PolarisBaseEntity entity) {
    // write it
    this.store.getSliceEntitiesActive().write(entity);
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntitiesChangeTrackingInCurrentTxn(@Nonnull PolarisBaseEntity entity) {
    // write it
    this.store.getSliceEntitiesChangeTracking().write(entity);
  }

  /** {@inheritDoc} */
  @Override
  public void writeToGrantRecordsInCurrentTxn(@Nonnull PolarisGrantRecord grantRec) {
    // write it
    this.store.getSliceGrantRecords().write(grantRec);
    this.store.getSliceGrantRecordsByGrantee().write(grantRec);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromEntitiesInCurrentTxn(@Nonnull PolarisEntityCore entity) {

    // delete it
    this.store.getSliceEntities().delete(this.store.buildEntitiesKey(entity));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromEntitiesActiveInCurrentTxn(@Nonnull PolarisEntityCore entity) {
    // delete it
    this.store.getSliceEntitiesActive().delete(this.store.buildEntitiesActiveKey(entity));
  }

  /**
   * {@inheritDoc}
   *
   * @param entity entity record to delete
   */
  @Override
  public void deleteFromEntitiesChangeTrackingInCurrentTxn(@Nonnull PolarisEntityCore entity) {
    // delete it
    this.store.getSliceEntitiesChangeTracking().delete(this.store.buildEntitiesKey(entity));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromGrantRecordsInCurrentTxn(@Nonnull PolarisGrantRecord grantRec) {

    // delete it
    this.store.getSliceGrantRecords().delete(grantRec);
    this.store.getSliceGrantRecordsByGrantee().delete(grantRec);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAllEntityGrantRecordsInCurrentTxn(
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
  public void deleteAllInCurrentTxn() {
    // clear all slices
    this.store.deleteAll();
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisBaseEntity lookupEntityInCurrentTxn(
      long catalogId, long entityId, int typeCode) {
    PolarisBaseEntity entity =
        this.store.getSliceEntities().read(this.store.buildKeyComposite(catalogId, entityId));
    if (entity != null && entity.getTypeCode() != typeCode) {
      return null;
    }
    return entity;
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisBaseEntity> lookupEntitiesInCurrentTxn(
      List<PolarisEntityId> entityIds) {
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
  public @Nonnull List<PolarisChangeTrackingVersions> lookupEntityVersionsInCurrentTxn(
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
  public EntityNameLookupRecord lookupEntityActiveInCurrentTxn(
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
        : new EntityNameLookupRecord(
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
  public List<EntityNameLookupRecord> lookupEntityActiveBatchInCurrentTxn(
      @Nonnull List<PolarisEntitiesActiveKey> entityActiveKeys) {
    // now build a list to quickly verify that nothing has changed
    return entityActiveKeys.stream()
        .map(this::lookupEntityActiveInCurrentTxn)
        .collect(Collectors.toList());
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull Page<EntityNameLookupRecord> listEntitiesInCurrentTxn(
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PageToken pageToken) {
    return this.listEntitiesInCurrentTxn(
        catalogId, parentId, entityType, Predicates.alwaysTrue(), pageToken);
  }

  @Override
  public @Nonnull Page<EntityNameLookupRecord> listEntitiesInCurrentTxn(
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull PageToken pageToken) {
    // full range scan under the parent for that type
    return this.listEntitiesInCurrentTxn(
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
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull Function<PolarisBaseEntity, T> transformer,
      @Nonnull PageToken pageToken) {
    // full range scan under the parent for that type
    Stream<PolarisBaseEntity> data =
        this.store
            .getSliceEntitiesActive()
            .readRange(
                this.store.buildPrefixKeyComposite(catalogId, parentId, entityType.getCode()))
            .stream()
            .map(
                nameRecord ->
                    this.lookupEntityInCurrentTxn(
                        catalogId, nameRecord.getId(), entityType.getCode()));

    Predicate<PolarisBaseEntity> tokenFilter =
        pageToken
            .valueAs(EntityIdToken.class)
            .map(
                entityIdToken -> {
                  var nextId = entityIdToken.entityId();
                  return (Predicate<PolarisBaseEntity>) e -> e.getId() > nextId;
                })
            .orElse(e -> true);

    data = data.sorted(Comparator.comparingLong(PolarisEntityCore::getId)).filter(tokenFilter);

    data = data.filter(entityFilter);

    return Page.mapped(pageToken, data, transformer, EntityIdToken::fromEntity);
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasChildrenInCurrentTxn(
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
  public int lookupEntityGrantRecordsVersionInCurrentTxn(long catalogId, long entityId) {
    PolarisBaseEntity entity =
        this.store
            .getSliceEntitiesChangeTracking()
            .read(this.store.buildKeyComposite(catalogId, entityId));

    // does not exist, 0
    return entity == null ? 0 : entity.getGrantRecordsVersion();
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisGrantRecord lookupGrantRecordInCurrentTxn(
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
  public @Nonnull List<PolarisGrantRecord> loadAllGrantRecordsOnSecurableInCurrentTxn(
      long securableCatalogId, long securableId) {
    // now fetch all grants for this securable
    return this.store
        .getSliceGrantRecords()
        .readRange(this.store.buildPrefixKeyComposite(securableCatalogId, securableId));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisGrantRecord> loadAllGrantRecordsOnGranteeInCurrentTxn(
      long granteeCatalogId, long granteeId) {
    // now fetch all grants assigned to this grantee
    return this.store
        .getSliceGrantRecordsByGrantee()
        .readRange(this.store.buildPrefixKeyComposite(granteeCatalogId, granteeId));
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisPrincipalSecrets loadPrincipalSecretsInCurrentTxn(
      @Nonnull String clientId) {
    return this.store.getSlicePrincipalSecrets().read(clientId);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PolarisPrincipalSecrets generateNewPrincipalSecretsInCurrentTxn(
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
  public @Nonnull PolarisPrincipalSecrets rotatePrincipalSecretsInCurrentTxn(
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
  public void deletePrincipalSecretsInCurrentTxn(@Nonnull String clientId, long principalId) {
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
      PolarisStorageIntegration<T> createStorageIntegrationInCurrentTxn(
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
          @Nonnull PolarisBaseEntity entity) {
    PolarisStorageConfigurationInfo storageConfig =
        BaseMetaStoreManager.extractStorageConfiguration(diagnostics, entity);
    return storageIntegrationProvider.getStorageIntegrationForConfig(storageConfig);
  }

  @Override
  public void rollback() {
    this.store.rollback();
  }

  /** {@inheritDoc} */
  @Override
  public void writeToPolicyMappingRecordsInCurrentTxn(@Nonnull PolarisPolicyMappingRecord record) {
    this.store.getSlicePolicyMappingRecords().write(record);
    this.store.getSlicePolicyMappingRecordsByPolicy().write(record);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromPolicyMappingRecordsInCurrentTxn(
      @Nonnull PolarisPolicyMappingRecord record) {
    this.store.getSlicePolicyMappingRecords().delete(record);
    this.store.getSlicePolicyMappingRecordsByPolicy().delete(record);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAllEntityPolicyMappingRecordsInCurrentTxn(
      @Nonnull PolarisBaseEntity entity,
      @Nonnull List<PolarisPolicyMappingRecord> mappingOnTarget,
      @Nonnull List<PolarisPolicyMappingRecord> mappingOnPolicy) {
    if (entity.getType() == PolarisEntityType.POLICY) {
      PolicyEntity policyEntity = PolicyEntity.of(entity);
      this.store
          .getSlicePolicyMappingRecordsByPolicy()
          .deleteRange(
              this.store.buildPrefixKeyComposite(
                  policyEntity.getPolicyTypeCode(),
                  policyEntity.getCatalogId(),
                  policyEntity.getId()));
      // also delete the other side. We need to delete these mapping one at a time versus doing a
      // range delete
      mappingOnPolicy.forEach(record -> this.store.getSlicePolicyMappingRecords().delete(record));
    } else {
      this.store
          .getSlicePolicyMappingRecords()
          .deleteRange(this.store.buildPrefixKeyComposite(entity.getCatalogId(), entity.getId()));
      // also delete the other side. We need to delete these mapping one at a time versus doing a
      // range delete
      mappingOnTarget.forEach(
          record -> this.store.getSlicePolicyMappingRecordsByPolicy().delete(record));
    }
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisPolicyMappingRecord lookupPolicyMappingRecordInCurrentTxn(
      long targetCatalogId,
      long targetId,
      int policyTypeCode,
      long policyCatalogId,
      long policyId) {
    return this.store
        .getSlicePolicyMappingRecords()
        .read(
            this.store.buildKeyComposite(
                targetCatalogId, targetId, policyTypeCode, policyCatalogId, policyId));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisPolicyMappingRecord> loadPoliciesOnTargetByTypeInCurrentTxn(
      long targetCatalogId, long targetId, int policyTypeCode) {
    return this.store
        .getSlicePolicyMappingRecords()
        .readRange(this.store.buildPrefixKeyComposite(targetCatalogId, targetId, policyTypeCode));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisPolicyMappingRecord> loadAllPoliciesOnTargetInCurrentTxn(
      long targetCatalogId, long targetId) {
    return this.store
        .getSlicePolicyMappingRecords()
        .readRange(this.store.buildPrefixKeyComposite(targetCatalogId, targetId));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisPolicyMappingRecord> loadAllTargetsOnPolicyInCurrentTxn(
      long policyCatalogId, long policyId, int policyTypeCode) {
    return this.store
        .getSlicePolicyMappingRecordsByPolicy()
        .readRange(this.store.buildPrefixKeyComposite(policyTypeCode, policyCatalogId, policyId));
  }

  private Optional<String> getEntityLocationWithoutScheme(PolarisBaseEntity entity) {
    if (entity.getType() == PolarisEntityType.TABLE_LIKE) {
      if (entity.getSubType() == PolarisEntitySubType.ICEBERG_TABLE
          || entity.getSubType() == PolarisEntitySubType.ICEBERG_VIEW) {
        return Optional.of(
            StorageLocation.of(
                    entity.getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION))
                .withoutScheme());
      }
    }
    if (entity.getType() == PolarisEntityType.NAMESPACE) {
      return Optional.of(
          StorageLocation.of(
                  entity.getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION))
              .withoutScheme());
    }
    return Optional.empty();
  }

  /** {@inheritDoc} */
  @Override
  public <T extends PolarisEntity & LocationBasedEntity>
      Optional<Optional<String>> hasOverlappingSiblings(T entity) {
    // TODO we could optimize this full scan
    StorageLocation entityLocationWithoutScheme =
        StorageLocation.of(StorageLocation.of(entity.getBaseLocation()).withoutScheme());
    List<PolarisBaseEntity> allEntities = this.store.getSliceEntities().readRange("");
    for (PolarisBaseEntity siblingEntity : allEntities) {
      Optional<StorageLocation> maybeSiblingLocationWithoutScheme =
          getEntityLocationWithoutScheme(siblingEntity).map(StorageLocation::of);
      if (maybeSiblingLocationWithoutScheme.isPresent()) {
        if (maybeSiblingLocationWithoutScheme.get().isChildOf(entityLocationWithoutScheme)
            || entityLocationWithoutScheme.isChildOf(maybeSiblingLocationWithoutScheme.get())) {
          return Optional.of(Optional.of(maybeSiblingLocationWithoutScheme.toString()));
        }
      }
    }
    return Optional.of(Optional.empty());
  }
}
