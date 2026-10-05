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
package org.apache.polaris.persistence.atomic.dynamodb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisEntityUtils;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.EntityAlreadyExistsException;
import org.apache.polaris.core.persistence.IntegrationPersistence;
import org.apache.polaris.core.persistence.PolicyMappingAlreadyExistsException;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.persistence.pagination.EntityIdToken;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PolicyMappingPersistence;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem;
import org.apache.polaris.persistence.atomic.dynamodb.models.GrantItem;
import org.apache.polaris.persistence.atomic.dynamodb.models.NameSentinelItem;
import org.apache.polaris.persistence.atomic.dynamodb.models.PolicyMappingItem;
import org.apache.polaris.persistence.atomic.dynamodb.models.PrincipalSecretItem;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

/**
 * DynamoDB {@link BasePersistence} adapter, driven by the reused {@code
 * AtomicOperationMetaStoreManager} and mirroring {@code JdbcBasePersistenceImpl}. Single-table
 * layout: strongly-consistent reads for correctness, GSIs for listing.
 */
public class DynamoDbBasePersistence
    implements BasePersistence, IntegrationPersistence, PolicyMappingPersistence {

  private static final String COND_NOT_EXISTS = "attribute_not_exists(#pk)";

  private static final int MAX_LOCATION_COMPONENTS = 40;

  private final DynamoDbClient client;
  private final String table;
  private final String realmId;
  private final PolarisDiagnostics diagnostics;
  private final PrincipalSecretsGenerator secretsGenerator;

  public DynamoDbBasePersistence(
      DynamoDbOperations ops,
      String realmId,
      PolarisDiagnostics diagnostics,
      PrincipalSecretsGenerator secretsGenerator) {
    this.client = ops.client();
    this.table = ops.tableName();
    this.realmId = realmId;
    this.diagnostics = diagnostics;
    this.secretsGenerator = secretsGenerator;
  }

  // ---- ids
  // ---------------------------------------------------------------------------------------

  @Override
  public long generateNewId(@NonNull PolarisCallContext callCtx) {
    return IdGenerator.getIdGenerator().nextId();
  }

  // ---- writes
  // ------------------------------------------------------------------------------------

  @Override
  public void writeEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity) {
    Map<String, AttributeValue> item = EntityItem.toItem(realmId, entity);
    if (originalEntity == null) {
      createEntity(callCtx, entity, item);
    } else if (!nameOrParentChanged) {
      updateEntityCas(entity, originalEntity, item);
    } else {
      renameEntity(callCtx, entity, originalEntity, item);
    }
  }

  private void createEntity(
      PolarisCallContext callCtx, PolarisBaseEntity entity, Map<String, AttributeValue> item) {
    TransactWriteItem putEntity = putIfNotExists(item);
    TransactWriteItem putSentinel =
        putIfNotExists(
            NameSentinelItem.toItem(
                realmId,
                entity.getCatalogId(),
                entity.getParentId(),
                entity.getTypeCode(),
                entity.getName(),
                entity.getId()));
    try {
      client.transactWriteItems(
          TransactWriteItemsRequest.builder().transactItems(putEntity, putSentinel).build());
    } catch (TransactionCanceledException e) {
      List<CancellationReason> reasons = e.cancellationReasons();
      // index 0 = entity put, index 1 = sentinel put
      if (conditionFailed(reasons, 1)) {
        PolarisBaseEntity existing =
            lookupEntityByName(
                callCtx,
                entity.getCatalogId(),
                entity.getParentId(),
                entity.getTypeCode(),
                entity.getName());
        if (existing != null) {
          throw new EntityAlreadyExistsException(existing, e);
        }
        throw new RetryOnConcurrencyException(
            "Name '%s' claimed concurrently but not yet visible; retry", entity.getName());
      }
      if (conditionFailed(reasons, 0)) {
        return; // same id already present: idempotent create retry
      }
      throw new RetryOnConcurrencyException("Create of '%s' cancelled; retry", entity.getName());
    }
  }

  private void updateEntityCas(
      PolarisBaseEntity entity,
      PolarisBaseEntity originalEntity,
      Map<String, AttributeValue> item) {
    try {
      client.putItem(
          b ->
              b.tableName(table)
                  .item(item)
                  .conditionExpression("#ev = :ev AND #grv = :grv")
                  .expressionAttributeNames(
                      Map.of(
                          "#ev",
                          DynamoDbConstants.A_ENTITY_VERSION,
                          "#grv",
                          DynamoDbConstants.A_GRANT_RECORDS_VERSION))
                  .expressionAttributeValues(
                      Map.of(
                          ":ev", n(originalEntity.getEntityVersion()),
                          ":grv", n(originalEntity.getGrantRecordsVersion()))));
    } catch (ConditionalCheckFailedException e) {
      throw new RetryOnConcurrencyException(
          "Entity '%s' id '%s' concurrently modified; expected entityVersion=%s, grantRecordsVersion=%s",
          entity.getName(),
          entity.getId(),
          originalEntity.getEntityVersion(),
          originalEntity.getGrantRecordsVersion());
    }
  }

  /** Rename / re-parent: CAS the entity and move the name sentinel, all-or-nothing. */
  private void renameEntity(
      PolarisCallContext callCtx,
      PolarisBaseEntity entity,
      PolarisBaseEntity originalEntity,
      Map<String, AttributeValue> item) {
    TransactWriteItem casEntity =
        TransactWriteItem.builder()
            .put(
                Put.builder()
                    .tableName(table)
                    .item(item)
                    .conditionExpression("#ev = :ev AND #grv = :grv")
                    .expressionAttributeNames(
                        Map.of(
                            "#ev",
                            DynamoDbConstants.A_ENTITY_VERSION,
                            "#grv",
                            DynamoDbConstants.A_GRANT_RECORDS_VERSION))
                    .expressionAttributeValues(
                        Map.of(
                            ":ev", n(originalEntity.getEntityVersion()),
                            ":grv", n(originalEntity.getGrantRecordsVersion())))
                    .build())
            .build();
    TransactWriteItem deleteOldSentinel =
        TransactWriteItem.builder()
            .delete(
                Delete.builder()
                    .tableName(table)
                    .key(
                        key(
                            DynamoDbConstants.sentinelPk(
                                realmId,
                                originalEntity.getCatalogId(),
                                originalEntity.getParentId(),
                                originalEntity.getTypeCode(),
                                originalEntity.getName()),
                            DynamoDbConstants.SK_NAME))
                    .build())
            .build();
    TransactWriteItem putNewSentinel =
        putIfNotExists(
            NameSentinelItem.toItem(
                realmId,
                entity.getCatalogId(),
                entity.getParentId(),
                entity.getTypeCode(),
                entity.getName(),
                entity.getId()));
    try {
      client.transactWriteItems(
          TransactWriteItemsRequest.builder()
              .transactItems(casEntity, deleteOldSentinel, putNewSentinel)
              .build());
    } catch (TransactionCanceledException e) {
      List<CancellationReason> reasons = e.cancellationReasons();
      if (conditionFailed(reasons, 2)) {
        PolarisBaseEntity existing =
            lookupEntityByName(
                callCtx,
                entity.getCatalogId(),
                entity.getParentId(),
                entity.getTypeCode(),
                entity.getName());
        if (existing != null) {
          throw new EntityAlreadyExistsException(existing, e);
        }
      }
      throw new RetryOnConcurrencyException(
          "Rename of '%s' id '%s' cancelled; retry", entity.getName(), entity.getId());
    }
  }

  @Override
  public void writeEntities(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisBaseEntity> entities,
      @Nullable List<PolarisBaseEntity> originalEntities) {
    // All-create xor all-update per the contract. A create adds two transact items (entity +
    // sentinel), so a batch create is bounded at 50 by the 100-item transaction cap.
    boolean creating = originalEntities == null;
    List<TransactWriteItem> items = new ArrayList<>();
    Map<Integer, PolarisBaseEntity> sentinelIndexToEntity = new HashMap<>();
    for (int i = 0; i < entities.size(); i++) {
      PolarisBaseEntity entity = entities.get(i);
      Map<String, AttributeValue> item = EntityItem.toItem(realmId, entity);
      if (creating) {
        items.add(putIfNotExists(item));
        sentinelIndexToEntity.put(items.size(), entity); // index of the sentinel added next
        items.add(
            putIfNotExists(
                NameSentinelItem.toItem(
                    realmId,
                    entity.getCatalogId(),
                    entity.getParentId(),
                    entity.getTypeCode(),
                    entity.getName(),
                    entity.getId())));
      } else {
        PolarisBaseEntity original = originalEntities.get(i);
        items.add(
            TransactWriteItem.builder()
                .put(
                    Put.builder()
                        .tableName(table)
                        .item(item)
                        .conditionExpression("#ev = :ev AND #grv = :grv")
                        .expressionAttributeNames(
                            Map.of(
                                "#ev",
                                DynamoDbConstants.A_ENTITY_VERSION,
                                "#grv",
                                DynamoDbConstants.A_GRANT_RECORDS_VERSION))
                        .expressionAttributeValues(
                            Map.of(
                                ":ev", n(original.getEntityVersion()),
                                ":grv", n(original.getGrantRecordsVersion())))
                        .build())
                .build());
      }
    }
    try {
      client.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(items).build());
    } catch (TransactionCanceledException e) {
      if (creating) {
        List<CancellationReason> reasons = e.cancellationReasons();
        for (Map.Entry<Integer, PolarisBaseEntity> entry : sentinelIndexToEntity.entrySet()) {
          if (conditionFailed(reasons, entry.getKey())) {
            PolarisBaseEntity entity = entry.getValue();
            PolarisBaseEntity existing =
                lookupEntityByName(
                    callCtx,
                    entity.getCatalogId(),
                    entity.getParentId(),
                    entity.getTypeCode(),
                    entity.getName());
            if (existing != null) {
              throw new EntityAlreadyExistsException(existing, e);
            }
          }
        }
      }
      throw new RetryOnConcurrencyException("Multi-entity write cancelled; retry");
    }
  }

  @Override
  public void writeToGrantRecords(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisGrantRecord grantRec) {
    // Idempotent: every field of a grant is part of its key.
    client.putItem(b -> b.tableName(table).item(GrantItem.toItem(realmId, grantRec)));
  }

  @Override
  public void writeEvents(@NonNull List<EventEntity> events) {
    // No-op: event persistence is a later phase in the design doc.
  }

  // ---- deletes
  // -----------------------------------------------------------------------------------

  @Override
  public void deleteEntity(@NonNull PolarisCallContext callCtx, @NonNull PolarisBaseEntity entity) {
    TransactWriteItem delEntity =
        TransactWriteItem.builder()
            .delete(
                Delete.builder()
                    .tableName(table)
                    .key(
                        key(
                            DynamoDbConstants.entityPk(realmId, entity.getId()),
                            DynamoDbConstants.SK_ENTITY))
                    .build())
            .build();
    TransactWriteItem delSentinel =
        TransactWriteItem.builder()
            .delete(
                Delete.builder()
                    .tableName(table)
                    .key(
                        key(
                            DynamoDbConstants.sentinelPk(
                                realmId,
                                entity.getCatalogId(),
                                entity.getParentId(),
                                entity.getTypeCode(),
                                entity.getName()),
                            DynamoDbConstants.SK_NAME))
                    .build())
            .build();
    client.transactWriteItems(
        TransactWriteItemsRequest.builder().transactItems(delEntity, delSentinel).build());
  }

  @Override
  public void deleteFromGrantRecords(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisGrantRecord grantRec) {
    client.deleteItem(
        b ->
            b.tableName(table)
                .key(
                    key(
                        DynamoDbConstants.entityPk(realmId, grantRec.getSecurableId()),
                        DynamoDbConstants.grantSk(
                            grantRec.getGranteeId(), grantRec.getPrivilegeCode()))));
  }

  @Override
  public void deleteAllEntityGrantRecords(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore entity,
      @NonNull List<PolarisGrantRecord> grantsOnGrantee,
      @NonNull List<PolarisGrantRecord> grantsOnSecurable) {
    List<PolarisGrantRecord> all = new ArrayList<>(grantsOnGrantee);
    all.addAll(grantsOnSecurable);
    for (PolarisGrantRecord g : all) {
      client.deleteItem(
          b ->
              b.tableName(table)
                  .key(
                      key(
                          DynamoDbConstants.entityPk(realmId, g.getSecurableId()),
                          DynamoDbConstants.grantSk(g.getGranteeId(), g.getPrivilegeCode()))));
    }
  }

  @Override
  public void deleteAll(@NonNull PolarisCallContext callCtx) {
    // PK is realmId#id, not prefix-queryable, so realm teardown is a filtered Scan + delete.
    String prefix = realmId + "#";
    Map<String, AttributeValue> start = null;
    do {
      ScanRequest.Builder scan =
          ScanRequest.builder()
              .tableName(table)
              .filterExpression("begins_with(#pk, :p)")
              .expressionAttributeNames(Map.of("#pk", DynamoDbConstants.PK))
              .expressionAttributeValues(Map.of(":p", s(prefix)));
      if (start != null) {
        scan.exclusiveStartKey(start);
      }
      ScanResponse resp = client.scan(scan.build());
      for (Map<String, AttributeValue> item : resp.items()) {
        client.deleteItem(
            b ->
                b.tableName(table)
                    .key(
                        Map.of(
                            DynamoDbConstants.PK, item.get(DynamoDbConstants.PK),
                            DynamoDbConstants.SK, item.get(DynamoDbConstants.SK))));
      }
      start = resp.hasLastEvaluatedKey() ? resp.lastEvaluatedKey() : null;
    } while (start != null);
  }

  // ---- lookups
  // -----------------------------------------------------------------------------------

  @Override
  public @Nullable PolarisBaseEntity lookupEntity(
      @NonNull PolarisCallContext callCtx, long catalogId, long entityId, int typeCode) {
    return getEntityById(entityId);
  }

  @Override
  public @Nullable PolarisBaseEntity lookupEntityByName(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @NonNull String name) {
    Map<String, AttributeValue> sentinel =
        getItemStrong(
            DynamoDbConstants.sentinelPk(realmId, catalogId, parentId, typeCode, name),
            DynamoDbConstants.SK_NAME);
    if (sentinel == null) {
      return null;
    }
    return getEntityById(NameSentinelItem.entityId(sentinel));
  }

  @Override
  public @NonNull List<PolarisBaseEntity> lookupEntities(
      @NonNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    List<PolarisBaseEntity> out = new ArrayList<>(entityIds.size());
    for (PolarisEntityId id : entityIds) {
      out.add(getEntityById(id.id()));
    }
    return out;
  }

  @Override
  public @NonNull List<PolarisChangeTrackingVersions> lookupEntityVersions(
      @NonNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    List<PolarisChangeTrackingVersions> out = new ArrayList<>(entityIds.size());
    for (PolarisEntityId id : entityIds) {
      PolarisBaseEntity e = getEntityById(id.id());
      out.add(
          e == null
              ? null
              : new PolarisChangeTrackingVersions(
                  e.getEntityVersion(), e.getGrantRecordsVersion()));
    }
    return out;
  }

  @Override
  public @NonNull Page<EntityNameLookupRecord> listEntities(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
    Stream<PolarisBaseEntity> stream =
        childrenForListing(catalogId, parentId, entityType.getCode(), entitySubType, pageToken);
    return Page.mapped(pageToken, stream, EntityNameLookupRecord::new, EntityIdToken::fromEntity);
  }

  @Override
  public @NonNull <T> Page<T> listFullEntities(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull Predicate<PolarisBaseEntity> entityFilter,
      @NonNull Function<PolarisBaseEntity, T> transformer,
      PageToken pageToken) {
    Stream<PolarisBaseEntity> stream =
        childrenForListing(catalogId, parentId, entityType.getCode(), entitySubType, pageToken)
            .filter(entityFilter);
    return Page.mapped(pageToken, stream, transformer, EntityIdToken::fromEntity);
  }

  @Override
  public int lookupEntityGrantRecordsVersion(
      @NonNull PolarisCallContext callCtx, long catalogId, long entityId) {
    PolarisBaseEntity e = getEntityById(entityId);
    return e == null ? 0 : e.getGrantRecordsVersion();
  }

  @Override
  public @Nullable PolarisGrantRecord lookupGrantRecord(
      @NonNull PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    Map<String, AttributeValue> item =
        getItemStrong(
            DynamoDbConstants.entityPk(realmId, securableId),
            DynamoDbConstants.grantSk(granteeId, privilegeCode));
    return item == null ? null : GrantItem.toGrantRecord(item);
  }

  @Override
  public @NonNull List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      @NonNull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    QueryRequest req =
        QueryRequest.builder()
            .tableName(table)
            .keyConditionExpression("#pk = :pk AND begins_with(#sk, :g)")
            .expressionAttributeNames(
                Map.of("#pk", DynamoDbConstants.PK, "#sk", DynamoDbConstants.SK))
            .expressionAttributeValues(
                Map.of(
                    ":pk", s(DynamoDbConstants.entityPk(realmId, securableId)),
                    ":g", s(DynamoDbConstants.SK_GRANT_PREFIX)))
            .build();
    List<PolarisGrantRecord> out = new ArrayList<>();
    for (Map<String, AttributeValue> item : queryAll(req)) {
      out.add(GrantItem.toGrantRecord(item));
    }
    return out;
  }

  @Override
  public @NonNull List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      @NonNull PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    QueryRequest req =
        QueryRequest.builder()
            .tableName(table)
            .indexName(DynamoDbConstants.GSI2)
            .keyConditionExpression("#pk = :pk")
            .expressionAttributeNames(Map.of("#pk", DynamoDbConstants.GSI2_PK))
            .expressionAttributeValues(
                Map.of(":pk", s(DynamoDbConstants.byGranteeGsiPk(realmId, granteeId))))
            .build();
    List<PolarisGrantRecord> out = new ArrayList<>();
    for (Map<String, AttributeValue> item : queryAll(req)) {
      out.add(GrantItem.toGrantRecord(item));
    }
    return out;
  }

  @Override
  public boolean hasChildren(
      @NonNull PolarisCallContext callContext,
      @Nullable PolarisEntityType optionalEntityType,
      long catalogId,
      long parentId) {
    if (optionalEntityType != null) {
      return hasAnyChild(catalogId, parentId, optionalEntityType.getCode());
    }
    for (PolarisEntityType type : PolarisEntityType.values()) {
      if (hasAnyChild(catalogId, parentId, type.getCode())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public <T extends PolarisEntity & LocationBasedEntity>
      Optional<Optional<String>> hasOverlappingSiblings(
          @NonNull PolarisCallContext callContext,
          @NonNull List<PolarisEntityCore> parentPath,
          T entity) {
    // No range query over locations, so scan the catalog and compute overlap in memory.
    String baseLocation = entity.getBaseLocation();
    if (baseLocation == null) {
      return Optional.of(Optional.empty());
    }
    if (baseLocation.chars().filter(ch -> ch == '/').count() > MAX_LOCATION_COMPONENTS) {
      return Optional.empty();
    }

    Set<Long> ancestorIds =
        parentPath.stream().map(PolarisEntityCore::getId).collect(Collectors.toSet());
    StorageLocation entityLocation = StorageLocation.of(baseLocation);

    for (PolarisBaseEntity candidate : scanCatalogEntities(entity.getCatalogId())) {
      if (candidate.getId() == entity.getId()) {
        continue;
      }
      Optional<StorageLocation> resultLocation =
          PolarisEntityUtils.asLocationBasedEntity(PolarisEntity.of(candidate))
              .map(LocationBasedEntity::getBaseLocation)
              .filter(location -> location != null && !location.isBlank())
              .map(StorageLocation::of);
      if (resultLocation.isEmpty()) {
        continue;
      }
      boolean containsEntity = entityLocation.isChildOf(resultLocation.get());
      boolean containedByEntity = resultLocation.get().isChildOf(entityLocation);
      // An ancestor that merely contains the entity is not a sibling overlap.
      if (containsEntity && !containedByEntity && ancestorIds.contains(candidate.getId())) {
        continue;
      }
      if (containsEntity || containedByEntity) {
        return Optional.of(Optional.of(resultLocation.get().toString()));
      }
    }
    return Optional.of(Optional.empty());
  }

  // ---- integration persistence (principal secrets, storage integrations)
  // ------------------------

  @Override
  public @Nullable PolarisPrincipalSecrets loadPrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId) {
    Map<String, AttributeValue> item =
        getItemStrong(DynamoDbConstants.secretPk(realmId, clientId), DynamoDbConstants.SK_SECRET);
    return item == null ? null : PrincipalSecretItem.toSecrets(item);
  }

  @Override
  public @NonNull PolarisPrincipalSecrets generateNewPrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String principalName, long principalId) {
    PolarisPrincipalSecrets candidate;
    do {
      candidate = secretsGenerator.produceSecrets(principalName, principalId);
    } while (loadPrincipalSecrets(callCtx, candidate.getPrincipalClientId()) != null);
    PolarisPrincipalSecrets secrets = candidate;
    try {
      client.putItem(
          b ->
              b.tableName(table)
                  .item(PrincipalSecretItem.toItem(realmId, secrets))
                  .conditionExpression(COND_NOT_EXISTS)
                  .expressionAttributeNames(Map.of("#pk", DynamoDbConstants.PK)));
    } catch (ConditionalCheckFailedException e) {
      throw new RetryOnConcurrencyException(
          "Client id '%s' claimed concurrently; retry", secrets.getPrincipalClientId());
    }
    return secrets;
  }

  @Override
  public @Nullable PolarisPrincipalSecrets storePrincipalSecrets(
      @NonNull PolarisCallContext callCtx,
      long principalId,
      @NonNull String resolvedClientId,
      String customClientSecret) {
    PolarisPrincipalSecrets secrets =
        new PolarisPrincipalSecrets(principalId, resolvedClientId, customClientSecret);
    try {
      client.putItem(
          b ->
              b.tableName(table)
                  .item(PrincipalSecretItem.toItem(realmId, secrets))
                  .conditionExpression(COND_NOT_EXISTS)
                  .expressionAttributeNames(Map.of("#pk", DynamoDbConstants.PK)));
    } catch (ConditionalCheckFailedException e) {
      throw new AlreadyExistsException(
          "Principal secrets already exist for clientId: " + resolvedClientId, e);
    }
    return secrets;
  }

  @Override
  public @Nullable PolarisPrincipalSecrets rotatePrincipalSecrets(
      @NonNull PolarisCallContext callCtx,
      @NonNull String clientId,
      long principalId,
      boolean reset,
      @NonNull String oldSecretHash) {
    PolarisPrincipalSecrets secrets = loadPrincipalSecrets(callCtx, clientId);
    diagnostics.checkNotNull(
        secrets, "cannot_find_secrets", "client_id={} principalId={}", clientId, principalId);
    diagnostics.check(
        principalId == secrets.getPrincipalId(),
        "principal_id_mismatch",
        "expectedId={} id={}",
        principalId,
        secrets.getPrincipalId());

    secrets.rotateSecrets(oldSecretHash);
    if (reset) {
      secrets.rotateSecrets(secrets.getMainSecretHash());
    }
    PolarisPrincipalSecrets rotated = secrets;
    try {
      // CAS on the stored main hash: a concurrent rotation would have changed it.
      client.putItem(
          b ->
              b.tableName(table)
                  .item(PrincipalSecretItem.toItem(realmId, rotated))
                  .conditionExpression("#msh = :old")
                  .expressionAttributeNames(Map.of("#msh", DynamoDbConstants.A_MAIN_SECRET_HASH))
                  .expressionAttributeValues(Map.of(":old", s(oldSecretHash))));
    } catch (ConditionalCheckFailedException e) {
      throw new RetryOnConcurrencyException(
          "Principal secrets for clientId '%s' concurrently modified", clientId);
    }
    return rotated;
  }

  @Override
  public void deletePrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId, long principalId) {
    client.deleteItem(
        b ->
            b.tableName(table)
                .key(
                    key(
                        DynamoDbConstants.secretPk(realmId, clientId),
                        DynamoDbConstants.SK_SECRET)));
  }

  @Override
  public @Nullable PolarisStorageIntegration createStorageIntegration(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    // No-op in OSS (as in relational-jdbc); the hook remains for custom deployments.
    return null;
  }

  @Override
  public void persistStorageIntegrationIfNeeded(
      @NonNull PolarisCallContext callContext,
      @NonNull PolarisBaseEntity entity,
      @Nullable PolarisStorageIntegration storageIntegration) {
    // No-op in OSS (see createStorageIntegration).
  }

  // ---- policy mapping persistence
  // ---------------------------------------------------------------

  @Override
  public void writeToPolicyMappingRecords(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisPolicyMappingRecord record) {
    PolicyType policyType = PolicyType.fromCode(record.getPolicyTypeCode());
    if (policyType == null) {
      throw new IllegalArgumentException("Invalid policy type code: " + record.getPolicyTypeCode());
    }
    if (policyType.isInheritable()) {
      // An inheritable policy type allows at most one policy of that type per target.
      List<PolarisPolicyMappingRecord> existing =
          loadPoliciesOnTargetByType(
              callCtx,
              record.getTargetCatalogId(),
              record.getTargetId(),
              record.getPolicyTypeCode());
      if (existing.size() > 1) {
        throw new PolicyMappingAlreadyExistsException(existing.get(0));
      } else if (existing.size() == 1) {
        PolarisPolicyMappingRecord current = existing.get(0);
        if (current.getPolicyCatalogId() != record.getPolicyCatalogId()
            || current.getPolicyId() != record.getPolicyId()) {
          throw new PolicyMappingAlreadyExistsException(current);
        }
        // Same policy: fall through and overwrite (parameters update).
      }
    }
    client.transactWriteItems(
        TransactWriteItemsRequest.builder()
            .transactItems(
                plainPut(PolicyMappingItem.toTargetItem(realmId, record)),
                plainPut(PolicyMappingItem.toPolicyItem(realmId, record)))
            .build());
  }

  @Override
  public void deleteFromPolicyMappingRecords(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisPolicyMappingRecord record) {
    client.transactWriteItems(
        TransactWriteItemsRequest.builder()
            .transactItems(
                plainDelete(
                    DynamoDbConstants.entityPk(realmId, record.getTargetId()),
                    DynamoDbConstants.policyTargetSk(
                        record.getPolicyTypeCode(),
                        record.getPolicyCatalogId(),
                        record.getPolicyId())),
                plainDelete(
                    DynamoDbConstants.entityPk(realmId, record.getPolicyId()),
                    DynamoDbConstants.policyRefSk(
                        record.getTargetCatalogId(), record.getTargetId())))
            .build());
  }

  @Override
  public void deleteAllEntityPolicyMappingRecords(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity entity,
      @NonNull List<PolarisPolicyMappingRecord> mappingOnTarget,
      @NonNull List<PolarisPolicyMappingRecord> mappingOnPolicy) {
    List<PolarisPolicyMappingRecord> all = new ArrayList<>(mappingOnTarget);
    all.addAll(mappingOnPolicy);
    for (PolarisPolicyMappingRecord r : all) {
      client.deleteItem(
          b ->
              b.tableName(table)
                  .key(
                      key(
                          DynamoDbConstants.entityPk(realmId, r.getTargetId()),
                          DynamoDbConstants.policyTargetSk(
                              r.getPolicyTypeCode(), r.getPolicyCatalogId(), r.getPolicyId()))));
      client.deleteItem(
          b ->
              b.tableName(table)
                  .key(
                      key(
                          DynamoDbConstants.entityPk(realmId, r.getPolicyId()),
                          DynamoDbConstants.policyRefSk(r.getTargetCatalogId(), r.getTargetId()))));
    }
  }

  @Override
  public @Nullable PolarisPolicyMappingRecord lookupPolicyMappingRecord(
      @NonNull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int policyTypeCode,
      long policyCatalogId,
      long policyId) {
    Map<String, AttributeValue> item =
        getItemStrong(
            DynamoDbConstants.entityPk(realmId, targetId),
            DynamoDbConstants.policyTargetSk(policyTypeCode, policyCatalogId, policyId));
    return item == null ? null : PolicyMappingItem.toRecord(item);
  }

  @Override
  public @NonNull List<PolarisPolicyMappingRecord> loadPoliciesOnTargetByType(
      @NonNull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int policyTypeCode) {
    return queryPolicyMappings(
        DynamoDbConstants.entityPk(realmId, targetId),
        DynamoDbConstants.policyTargetTypePrefix(policyTypeCode));
  }

  @Override
  public @NonNull List<PolarisPolicyMappingRecord> loadAllPoliciesOnTarget(
      @NonNull PolarisCallContext callCtx, long targetCatalogId, long targetId) {
    return queryPolicyMappings(
        DynamoDbConstants.entityPk(realmId, targetId), DynamoDbConstants.SK_POLICY_PREFIX);
  }

  @Override
  public @NonNull List<PolarisPolicyMappingRecord> loadAllTargetsOnPolicy(
      @NonNull PolarisCallContext callCtx,
      long policyCatalogId,
      long policyId,
      int policyTypeCode) {
    return queryPolicyMappings(
        DynamoDbConstants.entityPk(realmId, policyId), DynamoDbConstants.SK_POLICY_REF_PREFIX);
  }

  // ---- helpers
  // -----------------------------------------------------------------------------------

  private @Nullable PolarisBaseEntity getEntityById(long entityId) {
    Map<String, AttributeValue> item =
        getItemStrong(DynamoDbConstants.entityPk(realmId, entityId), DynamoDbConstants.SK_ENTITY);
    return item == null ? null : EntityItem.toEntity(item);
  }

  private @Nullable Map<String, AttributeValue> getItemStrong(String pk, String sk) {
    Map<String, AttributeValue> item =
        client.getItem(b -> b.tableName(table).key(key(pk, sk)).consistentRead(true)).item();
    return item == null || item.isEmpty() ? null : item;
  }

  private List<PolarisBaseEntity> queryChildren(long catalogId, long parentId, int typeCode) {
    List<PolarisBaseEntity> out = new ArrayList<>();
    for (Map<String, AttributeValue> item :
        queryAll(childrenQuery(catalogId, parentId, typeCode, false))) {
      out.add(EntityItem.toEntity(item));
    }
    return out;
  }

  /**
   * Children of a parent as an id-ordered stream for {@link Page#mapped}, resumed after the page
   * token's {@link EntityIdToken}. Ordered by id (not name) to match relational-jdbc so the shared
   * token round-trips identically.
   */
  private Stream<PolarisBaseEntity> childrenForListing(
      long catalogId,
      long parentId,
      int typeCode,
      PolarisEntitySubType entitySubType,
      PageToken pageToken) {
    List<PolarisBaseEntity> children = queryChildren(catalogId, parentId, typeCode);
    children.sort(Comparator.comparingLong(PolarisBaseEntity::getId));
    long resumeAfterId =
        pageToken.valueAs(EntityIdToken.class).map(EntityIdToken::entityId).orElse(Long.MIN_VALUE);
    return children.stream()
        .filter(e -> e.getId() > resumeAfterId)
        .filter(
            e ->
                entitySubType == PolarisEntitySubType.ANY_SUBTYPE
                    || e.getSubTypeCode() == entitySubType.getCode());
  }

  private boolean hasAnyChild(long catalogId, long parentId, int typeCode) {
    return !client.query(childrenQuery(catalogId, parentId, typeCode, true)).items().isEmpty();
  }

  /** Scan all entity items in one catalog of this realm (overlap check only; not a hot path). */
  private List<PolarisBaseEntity> scanCatalogEntities(long catalogId) {
    List<PolarisBaseEntity> out = new ArrayList<>();
    Map<String, AttributeValue> start = null;
    do {
      ScanRequest.Builder scan =
          ScanRequest.builder()
              .tableName(table)
              .filterExpression("#sk = :entity AND #cat = :cat AND begins_with(#pk, :p)")
              .expressionAttributeNames(
                  Map.of(
                      "#sk", DynamoDbConstants.SK,
                      "#cat", DynamoDbConstants.A_CATALOG_ID,
                      "#pk", DynamoDbConstants.PK))
              .expressionAttributeValues(
                  Map.of(
                      ":entity", s(DynamoDbConstants.SK_ENTITY),
                      ":cat", n(catalogId),
                      ":p", s(realmId + "#")));
      if (start != null) {
        scan.exclusiveStartKey(start);
      }
      ScanResponse resp = client.scan(scan.build());
      for (Map<String, AttributeValue> item : resp.items()) {
        out.add(EntityItem.toEntity(item));
      }
      start = resp.hasLastEvaluatedKey() ? resp.lastEvaluatedKey() : null;
    } while (start != null);
    return out;
  }

  private QueryRequest childrenQuery(
      long catalogId, long parentId, int typeCode, boolean limitOne) {
    QueryRequest.Builder b =
        QueryRequest.builder()
            .tableName(table)
            .indexName(DynamoDbConstants.GSI1)
            .keyConditionExpression("#pk = :pk")
            .expressionAttributeNames(Map.of("#pk", DynamoDbConstants.GSI1_PK))
            .expressionAttributeValues(
                Map.of(
                    ":pk",
                    s(DynamoDbConstants.byNameGsiPk(realmId, catalogId, parentId, typeCode))));
    if (limitOne) {
      b.limit(1);
    }
    return b.build();
  }

  /** Run a Query to completion, following {@code LastEvaluatedKey} across pages. */
  private List<Map<String, AttributeValue>> queryAll(QueryRequest req) {
    List<Map<String, AttributeValue>> out = new ArrayList<>();
    Map<String, AttributeValue> start = null;
    do {
      QueryRequest page = start == null ? req : req.toBuilder().exclusiveStartKey(start).build();
      QueryResponse resp = client.query(page);
      out.addAll(resp.items());
      start = resp.hasLastEvaluatedKey() ? resp.lastEvaluatedKey() : null;
    } while (start != null);
    return out;
  }

  private TransactWriteItem putIfNotExists(Map<String, AttributeValue> item) {
    return TransactWriteItem.builder()
        .put(
            Put.builder()
                .tableName(table)
                .item(item)
                .conditionExpression(COND_NOT_EXISTS)
                .expressionAttributeNames(Map.of("#pk", DynamoDbConstants.PK))
                .build())
        .build();
  }

  private TransactWriteItem plainPut(Map<String, AttributeValue> item) {
    return TransactWriteItem.builder()
        .put(Put.builder().tableName(table).item(item).build())
        .build();
  }

  private TransactWriteItem plainDelete(String pk, String sk) {
    return TransactWriteItem.builder()
        .delete(Delete.builder().tableName(table).key(key(pk, sk)).build())
        .build();
  }

  /**
   * Query all policy-mapping items under one partition whose sort key starts with {@code prefix}.
   */
  private List<PolarisPolicyMappingRecord> queryPolicyMappings(String pk, String skPrefix) {
    QueryRequest req =
        QueryRequest.builder()
            .tableName(table)
            .keyConditionExpression("#pk = :pk AND begins_with(#sk, :p)")
            .expressionAttributeNames(
                Map.of("#pk", DynamoDbConstants.PK, "#sk", DynamoDbConstants.SK))
            .expressionAttributeValues(Map.of(":pk", s(pk), ":p", s(skPrefix)))
            .build();
    List<PolarisPolicyMappingRecord> out = new ArrayList<>();
    for (Map<String, AttributeValue> item : queryAll(req)) {
      out.add(PolicyMappingItem.toRecord(item));
    }
    return out;
  }

  private static boolean conditionFailed(List<CancellationReason> reasons, int index) {
    return reasons != null
        && reasons.size() > index
        && "ConditionalCheckFailed".equals(reasons.get(index).code());
  }

  private static Map<String, AttributeValue> key(String pk, String sk) {
    Map<String, AttributeValue> k = new HashMap<>();
    k.put(DynamoDbConstants.PK, s(pk));
    k.put(DynamoDbConstants.SK, s(sk));
    return k;
  }

  private static AttributeValue s(String v) {
    return AttributeValue.fromS(v);
  }

  private static AttributeValue n(long v) {
    return AttributeValue.fromN(Long.toString(v));
  }
}
