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
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.GSI1;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.GSI1_PK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.GSI1_SK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.PK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.SK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.SK_ENTITY;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.SK_NAME;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.childrenGsiPk;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.childrenGsiSk;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.entityPk;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.sentinelPk;
import static org.apache.polaris.persistence.atomic.dynamodb.TransactionFailures.conditionFailedAt;
import static org.apache.polaris.persistence.atomic.dynamodb.TransactionFailures.onlyConflicts;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
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
import org.apache.polaris.core.persistence.pagination.EntityIdToken;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem;
import org.apache.polaris.persistence.atomic.dynamodb.models.NameSentinelItem;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.Get;
import software.amazon.awssdk.services.dynamodb.model.ItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactGetItem;
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
 *   <li>Multi-entity lookups read with {@code TransactGetItems}, so they see one snapshot.
 * </ul>
 *
 * <p>Listing queries the children index ({@link DynamoDbLayout#GSI1}) in id order and pages with
 * {@link EntityIdToken}, as {@code relational-jdbc} does. The index is eventually consistent, so
 * listing is the one read that can briefly miss a just-written child.
 *
 * <p>This class currently covers entity writes, lookups and listing. Multi-entity writes, grant
 * records and realm teardown throw {@link UnsupportedOperationException} until they are
 * implemented.
 */
public class DynamoDbBasePersistence implements BasePersistence {

  /** DynamoDB's limit on items per {@code TransactGetItems} request. */
  private static final int MAX_TRANSACT_GET_ITEMS = 100;

  /** Upper bound on attempts of a snapshot read cancelled by concurrent writes. */
  private static final int MAX_TRANSACT_GET_ATTEMPTS = 5;

  /** First wait before retrying a cancelled snapshot read; doubles on each attempt, capped. */
  private static final long TRANSACT_GET_BASE_BACKOFF_MILLIS = 20;

  private static final long TRANSACT_GET_MAX_BACKOFF_MILLIS = 1_000;

  /** Re-reads a delete may do when a concurrent update changes the entity's versions. */
  private static final int MAX_DELETE_ATTEMPTS = 3;

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
    try {
      client.transactWriteItems(b ->
          b.transactItems(
              putIfAbsent(EntityItem.toItem(realmId, entity)), putIfAbsent(sentinelItem(entity))));
    } catch (TransactionCanceledException e) {
      if (!onlyConflicts(e)) {
        throw e;
      }
      // Resolve by name, as JDBC does. A genuine retry of this create finds its own id here, which
      // the manager treats as idempotent. An id clash with a free name finds nothing, so it is not
      // mistaken for a successful create.
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
          e, "Create of '%s' conflicted with a concurrent write", entity.getName());
    }
  }

  private void updateEntity(PolarisBaseEntity entity, PolarisBaseEntity originalEntity) {
    try {
      client.putItem(b ->
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
      client.transactWriteItems(b ->
          b.transactItems(
              putIfVersionsMatch(EntityItem.toItem(realmId, entity), originalEntity),
              deleteSentinelIfOwned(originalEntity),
              putIfAbsent(sentinelItem(entity))));
    } catch (TransactionCanceledException e) {
      if (!onlyConflicts(e)) {
        throw e;
      }
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
    // owns. Like JDBC, a concurrent update does not fail the delete: re-read and try again.
    for (int attempt = 1; ; attempt++) {
      PolarisBaseEntity current = readEntity(entity.getId());
      if (current == null) {
        return;
      }
      try {
        client.transactWriteItems(
            b -> b.transactItems(deleteIfVersionsMatch(current), deleteSentinelIfOwned(current)));
        return;
      } catch (TransactionCanceledException e) {
        if (!onlyConflicts(e)) {
          throw e;
        }
        if (attempt == MAX_DELETE_ATTEMPTS) {
          throw concurrentModification(e, current);
        }
      }
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
      return List.of();
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
    return Collections.unmodifiableList(result);
  }

  @Override
  public @NonNull List<PolarisChangeTrackingVersions> lookupEntityVersions(
      @NonNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    if (entityIds == null || entityIds.isEmpty()) {
      return List.of();
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
    return Collections.unmodifiableList(result);
  }

  @Override
  public int lookupEntityGrantRecordsVersion(
      @NonNull PolarisCallContext callCtx, long catalogId, long entityId) {
    PolarisChangeTrackingVersions versions =
        lookupEntityVersions(callCtx, List.of(new PolarisEntityId(catalogId, entityId))).getFirst();
    return versions == null ? 0 : versions.grantRecordsVersion();
  }

  // ---- Listing ---------------------------------------------------------------------------------

  @Override
  public @NonNull Page<EntityNameLookupRecord> listEntities(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
    // One more than the page size, so the page can still tell whether a next page exists.
    OptionalInt pageSize = pageToken.pageSize();
    Integer queryLimit =
        pageSize.isPresent() && pageSize.getAsInt() < Integer.MAX_VALUE
            ? pageSize.getAsInt() + 1
            : null;
    Stream<PolarisBaseEntity> children =
        queryChildren(catalogId, parentId, entityType, entitySubType, pageToken, queryLimit);
    return Page.mapped(pageToken, children, EntityNameLookupRecord::new, EntityIdToken::fromEntity);
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
      @NonNull PageToken pageToken) {
    // No query limit: entityFilter runs after the read, so a limit could under-fill the page.
    Stream<PolarisBaseEntity> children =
        queryChildren(catalogId, parentId, entityType, entitySubType, pageToken, null)
            .filter(entityFilter);
    return Page.mapped(pageToken, children, transformer, EntityIdToken::fromEntity);
  }

  @Override
  public boolean hasChildren(
      @NonNull PolarisCallContext callContext,
      @Nullable PolarisEntityType optionalEntityType,
      long catalogId,
      long parentId) {
    // The children index is partitioned by type, so "any type" checks each type in turn.
    List<PolarisEntityType> types =
        optionalEntityType == null
            ? List.of(PolarisEntityType.values())
            : List.of(optionalEntityType);
    for (PolarisEntityType type : types) {
      QueryResponse response =
          client.query(
              b ->
                  b.tableName(tableName)
                      .indexName(GSI1)
                      .keyConditionExpression("#pk = :pk")
                      .expressionAttributeNames(Map.of("#pk", GSI1_PK))
                      .expressionAttributeValues(
                          Map.of(
                              ":pk",
                              AttributeValue.fromS(
                                  childrenGsiPk(realmId, catalogId, parentId, type.getCode()))))
                      .limit(1));
      if (response.count() > 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Streams a parent's children of one type from the children index, in id order. When the token
   * requests pagination and carries an {@link EntityIdToken}, the stream starts after that id, as
   * in {@code relational-jdbc}. Pages of the index are read lazily as the stream is consumed. The
   * index is eventually consistent, so a just-written child may be missing for a moment.
   */
  private Stream<PolarisBaseEntity> queryChildren(
      long catalogId,
      long parentId,
      PolarisEntityType entityType,
      PolarisEntitySubType entitySubType,
      PageToken pageToken,
      @Nullable Integer queryLimit) {
    Map<String, AttributeValue> values = new HashMap<>();
    values.put(
        ":pk",
        AttributeValue.fromS(childrenGsiPk(realmId, catalogId, parentId, entityType.getCode())));
    String keyCondition = "#pk = :pk";
    Optional<EntityIdToken> after =
        pageToken.paginationRequested() ? pageToken.valueAs(EntityIdToken.class) : Optional.empty();
    if (after.isPresent()) {
      keyCondition += " AND #sk > :sk";
      values.put(":sk", AttributeValue.fromS(childrenGsiSk(after.get().entityId())));
    }
    QueryRequest request =
        QueryRequest.builder()
            .tableName(tableName)
            .indexName(GSI1)
            .keyConditionExpression(keyCondition)
            .expressionAttributeNames(
                after.isPresent() ? Map.of("#pk", GSI1_PK, "#sk", GSI1_SK) : Map.of("#pk", GSI1_PK))
            .expressionAttributeValues(values)
            .limit(queryLimit)
            .build();
    Stream<PolarisBaseEntity> children =
        client.queryPaginator(request).items().stream().map(EntityItem::toEntity);
    if (entitySubType == PolarisEntitySubType.ANY_SUBTYPE) {
      return children;
    }
    return children.filter(e -> e.getSubTypeCode() == entitySubType.getCode());
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
   * Reads entity items as a consistent snapshot with {@code TransactGetItems}, in chunks of {@value
   * #MAX_TRANSACT_GET_ITEMS}. Each chunk is its own snapshot, so a lookup of more ids than that is
   * consistent per chunk only. A {@code null} projection returns whole items. Ids are
   * de-duplicated, since {@code TransactGetItems} rejects repeated keys.
   */
  private List<Map<String, AttributeValue>> batchReadEntities(
      List<PolarisEntityId> entityIds, @Nullable String projection) {
    Map<Long, Map<String, AttributeValue>> keysById = new LinkedHashMap<>();
    for (PolarisEntityId id : entityIds) {
      keysById.computeIfAbsent(id.id(), i -> key(entityPk(realmId, i), SK_ENTITY));
    }
    List<Map<String, AttributeValue>> keys = new ArrayList<>(keysById.values());
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    for (int start = 0; start < keys.size(); start += MAX_TRANSACT_GET_ITEMS) {
      List<TransactGetItem> gets = new ArrayList<>();
      for (Map<String, AttributeValue> key :
          keys.subList(start, Math.min(start + MAX_TRANSACT_GET_ITEMS, keys.size()))) {
        Get.Builder get = Get.builder().tableName(tableName).key(key);
        if (projection != null) {
          get.projectionExpression(projection)
              .expressionAttributeNames(
                  Map.of(
                      "#id", A_ID,
                      "#cid", A_CATALOG_ID,
                      "#ev", A_ENTITY_VERSION,
                      "#grv", A_GRANT_RECORDS_VERSION));
        }
        gets.add(TransactGetItem.builder().get(get.build()).build());
      }
      for (ItemResponse response : transactGet(gets)) {
        if (response.hasItem() && !response.item().isEmpty()) {
          items.add(response.item());
        }
      }
    }
    return items;
  }

  /** Runs one snapshot read, retrying when a concurrent write transaction cancels it. */
  private List<ItemResponse> transactGet(List<TransactGetItem> gets) {
    for (int attempt = 1; ; attempt++) {
      try {
        return client.transactGetItems(b -> b.transactItems(gets)).responses();
      } catch (TransactionCanceledException e) {
        if (!onlyConflicts(e)) {
          throw e;
        }
        if (attempt == MAX_TRANSACT_GET_ATTEMPTS) {
          throw new RetryOnConcurrencyException(
              e,
              "Snapshot read of %s entities kept conflicting with concurrent writes",
              gets.size());
        }
        backOff(attempt);
      }
    }
  }

  private static void backOff(int attempt) {
    long millis =
        Math.min(
            TRANSACT_GET_MAX_BACKOFF_MILLIS, TRANSACT_GET_BASE_BACKOFF_MILLIS << (attempt - 1));
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while retrying TransactGetItems", e);
    }
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

  private TransactWriteItem deleteIfVersionsMatch(PolarisBaseEntity expected) {
    return TransactWriteItem.builder()
        .delete(
            Delete.builder()
                .tableName(tableName)
                .key(key(entityPk(realmId, expected.getId()), SK_ENTITY))
                .conditionExpression(VERSIONS_MATCH)
                .expressionAttributeNames(versionNames())
                .expressionAttributeValues(versionValues(expected))
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
