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

import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.A_CATALOG_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.A_ENTITY_VERSION;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.A_GRANT_RECORDS_VERSION;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.A_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.PK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.SK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.SK_ENTITY;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.SK_NAME;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.entityPk;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.sentinelPk;
import static org.apache.polaris.persistence.atomic.dynamodb.TransactionFailures.conditionFailedAt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.EntityAlreadyExistsException;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem;
import org.apache.polaris.persistence.atomic.dynamodb.models.NameSentinelItem;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.TransactionConflictException;

/**
 * DynamoDB implementation of {@link BasePersistence}, meant to run under {@code
 * AtomicOperationMetaStoreManager}.
 *
 * <p>Every correctness decision rides a strongly consistent single-item read or a conditional
 * write:
 *
 * <ul>
 *   <li>A create writes the entity item and its name sentinel in one {@code TransactWriteItems},
 *       both {@code attribute_not_exists}, so ids and names stay unique.
 *   <li>An update is a compare-and-set on {@code entityVersion} and {@code grantRecordsVersion}.
 *   <li>A rename or re-parent moves the name sentinel in the same transaction as that
 *       compare-and-set.
 * </ul>
 *
 * <p>This class currently covers entity writes and lookups. Listing, multi-entity writes, grant
 * records and realm teardown throw {@link UnsupportedOperationException} until they are
 * implemented.
 */
public class DynamoDbBasePersistence implements BasePersistence {

  /** DynamoDB's limit on keys per {@code BatchGetItem} request. */
  private static final int MAX_BATCH_GET_KEYS = 100;

  /** Upper bound on re-requests of keys DynamoDB returns as unprocessed. */
  private static final int MAX_BATCH_GET_ATTEMPTS = 10;

  private static final String VERSIONS_MATCH = "#ev = :ev AND #grv = :grv";
  private static final String KEY_ABSENT = "attribute_not_exists(#pk)";
  private static final String SENTINEL_ABSENT_OR_OWNED = "attribute_not_exists(#pk) OR #id = :id";

  private final DynamoDbClient client;
  private final String tableName;
  private final String realmId;
  private final IdGenerator idGenerator = new IdGenerator();

  public DynamoDbBasePersistence(DynamoDbClient client, String tableName, String realmId) {
    this.client = client;
    this.tableName = tableName;
    this.realmId = realmId;
  }

  // ---- Ids -------------------------------------------------------------------------------------

  @Override
  public long generateNewId(@NonNull PolarisCallContext callCtx) {
    return idGenerator.nextId();
  }

  // ---- Entity writes ---------------------------------------------------------------------------

  @Override
  public void writeEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity) {
    if (originalEntity == null) {
      createEntity(callCtx, entity);
    } else if (nameOrParentChanged && !sameSentinel(entity, originalEntity)) {
      renameEntity(callCtx, entity, originalEntity);
    } else {
      // The manager also passes nameOrParentChanged=true when the name and parent are unchanged
      // (for example when updating a principal). The sentinel stays put, so this is a plain update.
      updateEntity(entity, originalEntity);
    }
  }

  private void createEntity(PolarisCallContext callCtx, PolarisBaseEntity entity) {
    // Action order matters: cancellation reasons are reported by index.
    final int entityIndex = 0;
    final int sentinelIndex = 1;
    try {
      client.transactWriteItems(
          b ->
              b.transactItems(
                  putIfAbsent(EntityItem.toItem(realmId, entity)),
                  putIfAbsent(sentinelItem(entity))));
    } catch (TransactionCanceledException e) {
      if (conditionFailedAt(e, sentinelIndex)) {
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
            e,
            "Name '%s' was claimed concurrently but is no longer present; retry",
            entity.getName());
      }
      if (conditionFailedAt(e, entityIndex)) {
        // The id is taken. The manager treats a matching id as an idempotent retry of this create.
        PolarisBaseEntity existing = readEntity(entity.getId());
        if (existing != null) {
          throw new EntityAlreadyExistsException(existing, e);
        }
      }
      throw new RetryOnConcurrencyException(
          e, "Create of '%s' was cancelled; retry", entity.getName());
    }
  }

  private void updateEntity(PolarisBaseEntity entity, PolarisBaseEntity originalEntity) {
    try {
      client.putItem(
          b ->
              b.tableName(tableName)
                  .item(EntityItem.toItem(realmId, entity))
                  .conditionExpression(VERSIONS_MATCH)
                  .expressionAttributeNames(versionNames())
                  .expressionAttributeValues(versionValues(originalEntity)));
    } catch (ConditionalCheckFailedException | TransactionConflictException e) {
      throw concurrentModification(e, originalEntity);
    }
  }

  private void renameEntity(
      PolarisCallContext callCtx, PolarisBaseEntity entity, PolarisBaseEntity originalEntity) {
    final int newSentinelIndex = 2;
    try {
      client.transactWriteItems(
          b ->
              b.transactItems(
                  putIfVersionsMatch(EntityItem.toItem(realmId, entity), originalEntity),
                  deleteSentinelIfOwned(originalEntity),
                  putIfAbsent(sentinelItem(entity))));
    } catch (TransactionCanceledException e) {
      if (conditionFailedAt(e, newSentinelIndex)) {
        PolarisBaseEntity existing =
            lookupEntityByName(
                callCtx,
                entity.getCatalogId(),
                entity.getParentId(),
                entity.getTypeCode(),
                entity.getName());
        if (existing != null && existing.getId() != entity.getId()) {
          throw new EntityAlreadyExistsException(existing, e);
        }
      }
      throw concurrentModification(e, originalEntity);
    }
  }

  @Override
  public void deleteEntity(@NonNull PolarisCallContext callCtx, @NonNull PolarisBaseEntity entity) {
    // Delete by the stored state, not the caller's copy: a concurrent rename may have moved the
    // sentinel, and deleting the caller's (old) name could remove a sentinel another entity now
    // owns.
    PolarisBaseEntity current = readEntity(entity.getId());
    if (current == null) {
      return;
    }
    try {
      client.transactWriteItems(
          b ->
              b.transactItems(
                  TransactWriteItem.builder()
                      .delete(
                          Delete.builder()
                              .tableName(tableName)
                              .key(key(entityPk(realmId, current.getId()), SK_ENTITY))
                              .conditionExpression(VERSIONS_MATCH)
                              .expressionAttributeNames(versionNames())
                              .expressionAttributeValues(versionValues(current))
                              .build())
                      .build(),
                  deleteSentinelIfOwned(current)));
    } catch (TransactionCanceledException e) {
      throw concurrentModification(e, current);
    }
  }

  // ---- Entity lookups --------------------------------------------------------------------------

  @Override
  public @Nullable PolarisBaseEntity lookupEntity(
      @NonNull PolarisCallContext callCtx, long catalogId, long entityId, int typeCode) {
    PolarisBaseEntity entity = readEntity(entityId);
    if (entity == null || entity.getCatalogId() != catalogId || entity.getTypeCode() != typeCode) {
      return null;
    }
    return entity;
  }

  @Override
  public @Nullable PolarisBaseEntity lookupEntityByName(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @NonNull String name) {
    Map<String, AttributeValue> sentinel =
        readItem(sentinelPk(realmId, catalogId, parentId, typeCode, name), SK_NAME);
    if (sentinel == null) {
      return null;
    }
    PolarisBaseEntity entity = readEntity(NameSentinelItem.entityId(sentinel));
    if (entity == null
        || entity.getCatalogId() != catalogId
        || entity.getParentId() != parentId
        || entity.getTypeCode() != typeCode
        || !name.equals(entity.getName())) {
      return null;
    }
    return entity;
  }

  @Override
  public @NonNull List<PolarisBaseEntity> lookupEntities(
      @NonNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    if (entityIds == null || entityIds.isEmpty()) {
      return new ArrayList<>();
    }
    Map<PolarisEntityId, PolarisBaseEntity> byId = new HashMap<>();
    for (Map<String, AttributeValue> item : batchReadEntities(entityIds, null)) {
      PolarisBaseEntity entity = EntityItem.toEntity(item);
      byId.put(new PolarisEntityId(entity.getCatalogId(), entity.getId()), entity);
    }
    List<PolarisBaseEntity> result = new ArrayList<>(entityIds.size());
    for (PolarisEntityId id : entityIds) {
      result.add(byId.get(id));
    }
    return result;
  }

  @Override
  public @NonNull List<PolarisChangeTrackingVersions> lookupEntityVersions(
      @NonNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    if (entityIds == null || entityIds.isEmpty()) {
      return new ArrayList<>();
    }
    Map<PolarisEntityId, PolarisChangeTrackingVersions> byId = new HashMap<>();
    for (Map<String, AttributeValue> item : batchReadEntities(entityIds, "#id, #cid, #ev, #grv")) {
      byId.put(
          new PolarisEntityId(number(item, A_CATALOG_ID), number(item, A_ID)),
          new PolarisChangeTrackingVersions(
              (int) number(item, A_ENTITY_VERSION), (int) number(item, A_GRANT_RECORDS_VERSION)));
    }
    List<PolarisChangeTrackingVersions> result = new ArrayList<>(entityIds.size());
    for (PolarisEntityId id : entityIds) {
      result.add(byId.get(id));
    }
    return result;
  }

  @Override
  public int lookupEntityGrantRecordsVersion(
      @NonNull PolarisCallContext callCtx, long catalogId, long entityId) {
    PolarisChangeTrackingVersions versions =
        lookupEntityVersions(callCtx, List.of(new PolarisEntityId(catalogId, entityId))).getFirst();
    return versions == null ? 0 : versions.grantRecordsVersion();
  }

  // ---- Not yet implemented ---------------------------------------------------------------------

  @Override
  public void writeEntities(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisBaseEntity> entities,
      @Nullable List<PolarisBaseEntity> originalEntities) {
    throw notYetImplemented("writeEntities");
  }

  @Override
  public void writeToGrantRecords(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisGrantRecord grantRec) {
    throw notYetImplemented("writeToGrantRecords");
  }

  @Override
  public void writeEvents(@NonNull List<EventEntity> events) {
    throw notYetImplemented("writeEvents");
  }

  @Override
  public void deleteFromGrantRecords(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisGrantRecord grantRec) {
    throw notYetImplemented("deleteFromGrantRecords");
  }

  @Override
  public void deleteAllEntityGrantRecords(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore entity,
      @NonNull List<PolarisGrantRecord> grantsOnGrantee,
      @NonNull List<PolarisGrantRecord> grantsOnSecurable) {
    throw notYetImplemented("deleteAllEntityGrantRecords");
  }

  @Override
  public void deleteAll(@NonNull PolarisCallContext callCtx) {
    throw notYetImplemented("deleteAll");
  }

  @Override
  public @NonNull Page<EntityNameLookupRecord> listEntities(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
    throw notYetImplemented("listEntities");
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
    throw notYetImplemented("listFullEntities");
  }

  @Override
  public @Nullable PolarisGrantRecord lookupGrantRecord(
      @NonNull PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    throw notYetImplemented("lookupGrantRecord");
  }

  @Override
  public @NonNull List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      @NonNull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    throw notYetImplemented("loadAllGrantRecordsOnSecurable");
  }

  @Override
  public @NonNull List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      @NonNull PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    throw notYetImplemented("loadAllGrantRecordsOnGrantee");
  }

  @Override
  public boolean hasChildren(
      @NonNull PolarisCallContext callContext,
      @Nullable PolarisEntityType optionalEntityType,
      long catalogId,
      long parentId) {
    throw notYetImplemented("hasChildren");
  }

  // ---- Helpers ---------------------------------------------------------------------------------

  private @Nullable PolarisBaseEntity readEntity(long entityId) {
    Map<String, AttributeValue> item = readItem(entityPk(realmId, entityId), SK_ENTITY);
    return item == null ? null : EntityItem.toEntity(item);
  }

  /** Strongly consistent single-item read; {@code null} when the item is absent. */
  private @Nullable Map<String, AttributeValue> readItem(String pk, String sk) {
    Map<String, AttributeValue> item =
        client.getItem(b -> b.tableName(tableName).key(key(pk, sk)).consistentRead(true)).item();
    return item == null || item.isEmpty() ? null : item;
  }

  /**
   * Strongly consistent batch read of entity items, in chunks of {@value #MAX_BATCH_GET_KEYS}. A
   * {@code null} projection returns whole items. Ids are de-duplicated, since {@code BatchGetItem}
   * rejects repeated keys.
   */
  private List<Map<String, AttributeValue>> batchReadEntities(
      List<PolarisEntityId> entityIds, @Nullable String projection) {
    Map<Long, Map<String, AttributeValue>> keysById = new LinkedHashMap<>();
    for (PolarisEntityId id : entityIds) {
      keysById.computeIfAbsent(id.id(), i -> key(entityPk(realmId, i), SK_ENTITY));
    }
    List<Map<String, AttributeValue>> keys = new ArrayList<>(keysById.values());
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    for (int start = 0; start < keys.size(); start += MAX_BATCH_GET_KEYS) {
      List<Map<String, AttributeValue>> chunk =
          keys.subList(start, Math.min(start + MAX_BATCH_GET_KEYS, keys.size()));
      KeysAndAttributes.Builder request =
          KeysAndAttributes.builder().keys(chunk).consistentRead(true);
      if (projection != null) {
        request
            .projectionExpression(projection)
            .expressionAttributeNames(
                Map.of(
                    "#id", A_ID,
                    "#cid", A_CATALOG_ID,
                    "#ev", A_ENTITY_VERSION,
                    "#grv", A_GRANT_RECORDS_VERSION));
      }
      Map<String, KeysAndAttributes> pending = Map.of(tableName, request.build());
      for (int attempt = 0; !pending.isEmpty(); attempt++) {
        if (attempt == MAX_BATCH_GET_ATTEMPTS) {
          throw new IllegalStateException(
              "BatchGetItem on " + tableName + " left keys unprocessed after retries");
        }
        Map<String, KeysAndAttributes> requestItems = pending;
        BatchGetItemResponse response = client.batchGetItem(b -> b.requestItems(requestItems));
        items.addAll(response.responses().getOrDefault(tableName, List.of()));
        pending = response.hasUnprocessedKeys() ? response.unprocessedKeys() : Map.of();
      }
    }
    return items;
  }

  private Map<String, AttributeValue> sentinelItem(PolarisBaseEntity entity) {
    return NameSentinelItem.toItem(
        realmId,
        entity.getCatalogId(),
        entity.getParentId(),
        entity.getTypeCode(),
        entity.getName(),
        entity.getId());
  }

  private static boolean sameSentinel(PolarisBaseEntity a, PolarisBaseEntity b) {
    return a.getCatalogId() == b.getCatalogId()
        && a.getParentId() == b.getParentId()
        && a.getTypeCode() == b.getTypeCode()
        && a.getName().equals(b.getName());
  }

  private TransactWriteItem putIfAbsent(Map<String, AttributeValue> item) {
    return TransactWriteItem.builder()
        .put(
            Put.builder()
                .tableName(tableName)
                .item(item)
                .conditionExpression(KEY_ABSENT)
                .expressionAttributeNames(Map.of("#pk", PK))
                .build())
        .build();
  }

  private TransactWriteItem putIfVersionsMatch(
      Map<String, AttributeValue> item, PolarisBaseEntity expected) {
    return TransactWriteItem.builder()
        .put(
            Put.builder()
                .tableName(tableName)
                .item(item)
                .conditionExpression(VERSIONS_MATCH)
                .expressionAttributeNames(versionNames())
                .expressionAttributeValues(versionValues(expected))
                .build())
        .build();
  }

  /**
   * Deletes the entity's name sentinel unless another entity owns it. An absent sentinel is
   * allowed, so a delete never gets stuck on a sentinel that is already gone.
   */
  private TransactWriteItem deleteSentinelIfOwned(PolarisBaseEntity entity) {
    return TransactWriteItem.builder()
        .delete(
            Delete.builder()
                .tableName(tableName)
                .key(
                    key(
                        sentinelPk(
                            realmId,
                            entity.getCatalogId(),
                            entity.getParentId(),
                            entity.getTypeCode(),
                            entity.getName()),
                        SK_NAME))
                .conditionExpression(SENTINEL_ABSENT_OR_OWNED)
                .expressionAttributeNames(Map.of("#pk", PK, "#id", A_ID))
                .expressionAttributeValues(Map.of(":id", numberValue(entity.getId())))
                .build())
        .build();
  }

  private static Map<String, String> versionNames() {
    return Map.of("#ev", A_ENTITY_VERSION, "#grv", A_GRANT_RECORDS_VERSION);
  }

  private static Map<String, AttributeValue> versionValues(PolarisBaseEntity expected) {
    return Map.of(
        ":ev", numberValue(expected.getEntityVersion()),
        ":grv", numberValue(expected.getGrantRecordsVersion()));
  }

  private static RetryOnConcurrencyException concurrentModification(
      Exception cause, PolarisBaseEntity expected) {
    return new RetryOnConcurrencyException(
        cause,
        "Entity '%s' id '%s' concurrently modified; expected entityVersion=%s, grantRecordsVersion=%s",
        expected.getName(),
        expected.getId(),
        expected.getEntityVersion(),
        expected.getGrantRecordsVersion());
  }

  private static UnsupportedOperationException notYetImplemented(String operation) {
    return new UnsupportedOperationException(
        operation + " is not yet implemented by the DynamoDB persistence backend");
  }

  private static Map<String, AttributeValue> key(String pk, String sk) {
    return Map.of(PK, AttributeValue.fromS(pk), SK, AttributeValue.fromS(sk));
  }

  private static AttributeValue numberValue(long value) {
    return AttributeValue.fromN(Long.toString(value));
  }

  private static long number(Map<String, AttributeValue> item, String attribute) {
    AttributeValue value = item.get(attribute);
    return value == null || value.n() == null ? 0L : Long.parseLong(value.n());
  }
}
