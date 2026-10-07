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
package org.apache.polaris.persistence.nosql.dynamodb;

import static java.util.Collections.singletonMap;
import static org.apache.polaris.persistence.nosql.api.backend.PersistId.persistId;
import static org.apache.polaris.persistence.nosql.dynamodb.DynamoDbConstants.BATCH_GET_LIMIT;
import static org.apache.polaris.persistence.nosql.dynamodb.DynamoDbConstants.BATCH_WRITE_LIMIT;
import static org.apache.polaris.persistence.nosql.dynamodb.DynamoDbConstants.KEY_NAME;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_CREATED_AT;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_REAL_PART_NUM;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_TYPE;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_VALUE;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_VERSION;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_CREATED_AT;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_POINTER;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_PREVIOUS;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_OBJS;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_REFS;
import static software.amazon.awssdk.core.SdkBytes.fromByteArray;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS;

import jakarta.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.FetchedObj;
import org.apache.polaris.persistence.nosql.api.backend.PersistId;
import org.apache.polaris.persistence.nosql.api.backend.WriteObj;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.nosql.api.exceptions.UnknownOperationResultException;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.apache.polaris.persistence.nosql.impl.PersistenceImplementation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoDbBackend implements Backend {
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDbBackend.class);

  final DynamoDbClient client;
  private final boolean closeClient;

  final String tableRefs;
  final String tableObjs;

  public DynamoDbBackend(DynamoDbBackendConfig config) {
    this.client = config.client();
    this.tableRefs =
        config.tablePrefix().map(prefix -> prefix + '_' + TABLE_REFS).orElse(TABLE_REFS);
    this.tableObjs =
        config.tablePrefix().map(prefix -> prefix + '_' + TABLE_OBJS).orElse(TABLE_OBJS);
    this.closeClient = config.closeClient();
  }

  @Override
  public Optional<String> setupSchema() {
    createIfMissing(tableRefs);
    createIfMissing(tableObjs);
    return Optional.empty();
  }

  private void createIfMissing(String name) {
    if (!tableExists(name)) {
      createTable(name);
    }
  }

  private boolean tableExists(String name) {
    try {
      var table = client.describeTable(DescribeTableRequest.builder().tableName(name).build());
      verifyKeySchema(table.table());
      return true;
    } catch (ResourceNotFoundException e) {
      LOGGER.debug("Didn't find table '{}', going to create one.", name, e);
      return false;
    }
  }

  private void createTable(String name) {
    client.createTable(
        b ->
            b.tableName(name)
                .attributeDefinitions(
                    AttributeDefinition.builder()
                        .attributeName(KEY_NAME)
                        .attributeType(ScalarAttributeType.B)
                        .build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .keySchema(
                    KeySchemaElement.builder()
                        .attributeName(KEY_NAME)
                        .keyType(KeyType.HASH)
                        .build()));
  }

  private static void verifyKeySchema(TableDescription description) {
    var elements = description.keySchema();

    if (elements.size() == 1) {
      var key = elements.getFirst();
      if (key.attributeName().equals(KEY_NAME)) {
        if (key.keyType() == KeyType.HASH) {
          return;
        }
      }
    }
    throw new IllegalStateException(
        String.format(
            "Invalid key schema for table: %s. Key schema should be a hash partitioned "
                + "attribute with the name '%s'.",
            description.tableName(), KEY_NAME));
  }

  @Override
  @Nonnull
  public String type() {
    return DynamoDbBackendFactory.NAME;
  }

  @Override
  public boolean supportsRealmDeletion() {
    return false;
  }

  @Override
  public void deleteRealms(Set<String> realmIds) {
    throw new UnsupportedOperationException("Backend does not support deletion of realms.");
  }

  @Override
  public void batchDeleteRefs(Map<String, Set<String>> realmRefs) {
    batchDelete(tableRefs, realmRefs, DynamoDbBackend::referenceKeyMap);
  }

  @Override
  public void batchDeleteObjs(Map<String, Set<PersistId>> realmObjs) {
    batchDelete(tableObjs, realmObjs, DynamoDbBackend::objKeyMap);
  }

  <ID> void batchDelete(
      String table,
      Map<String, Set<ID>> realmDeletes,
      BiFunction<String, ID, Map<String, AttributeValue>> idToKeyMap) {
    try (var batch = new BatchWrite(this, table)) {
      for (Map.Entry<String, Set<ID>> realmEntry : realmDeletes.entrySet()) {
        var realm = realmEntry.getKey();
        for (var id : realmEntry.getValue()) {
          batch.addDelete(idToKeyMap.apply(realm, id));
        }
      }
    }
  }

  @Override
  public void scanBackend(
      @Nonnull ReferenceScanCallback referenceConsumer, @Nonnull ObjScanCallback objConsumer) {
    client
        .scanPaginator(b -> b.tableName(tableRefs))
        .forEach(
            scanResponse ->
                scanResponse
                    .items()
                    .forEach(
                        i -> {
                          try (var in = i.get(KEY_NAME).b().asInputStream();
                              var data = new DataInputStream(in)) {
                            var realmId = data.readUTF();
                            var ref = data.readUTF();
                            var createdAt = Long.parseLong(i.get(COL_REF_CREATED_AT).n());
                            referenceConsumer.call(realmId, ref, createdAt);
                          } catch (IOException e) {
                            throw new RuntimeException(e);
                          }
                        }));
    client
        .scanPaginator(b -> b.tableName(tableObjs))
        .forEach(
            scanResponse ->
                scanResponse
                    .items()
                    .forEach(
                        i -> {
                          try (var in = i.get(KEY_NAME).b().asInputStream();
                              var data = new DataInputStream(in)) {
                            var realmId = data.readUTF();
                            var id = data.readLong();
                            var part = data.readInt();
                            var createdAt = Long.parseLong(i.get(COL_REF_CREATED_AT).n());
                            var type = i.get(COL_OBJ_TYPE).s();
                            objConsumer.call(realmId, type, persistId(id, part), createdAt);
                          } catch (IOException e) {
                            throw new RuntimeException(e);
                          }
                        }));
  }

  @Override
  public boolean createReference(@Nonnull String realmId, @Nonnull Reference newRef) {
    try {
      client.putItem(
          b ->
              b.tableName(tableRefs)
                  .conditionExpression("attribute_not_exists(" + COL_REF_POINTER + ")")
                  .item(referenceAttributeValues(realmId, newRef)));
      return true;
    } catch (ConditionalCheckFailedException e) {
      throwIfAmbiguousConditionalCheckFailure(e);
      return false;
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public void createReferences(@Nonnull String realmId, @Nonnull List<Reference> newRefs) {
    for (var newRef : newRefs) {
      createReference(realmId, newRef);
    }
  }

  @Override
  public boolean updateReference(
      @Nonnull String realmId,
      @Nonnull Reference updatedRef,
      @Nonnull Optional<ObjRef> expectedPointer) {
    try {
      try {
        client.putItem(
            b ->
                b.tableName(tableRefs)
                    .conditionExpression("(" + COL_REF_POINTER + " = :pointer)")
                    .expressionAttributeValues(Map.of(":pointer", pointerSdkBytes(expectedPointer)))
                    .item(referenceAttributeValues(realmId, updatedRef)));
      } catch (RuntimeException e) {
        throw unhandledException(e);
      }

      return true;
    } catch (ConditionalCheckFailedException e) {
      throwIfAmbiguousConditionalCheckFailure(e);
      fetchReference(realmId, updatedRef.name());
      return false;
    }
  }

  @Nonnull
  @Override
  public Reference fetchReference(@Nonnull String realmId, @Nonnull String name) {
    GetItemResponse item;
    try {
      item = client.getItem(b -> b.tableName(tableRefs).key(referenceKeyMap(realmId, name)));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
    if (!item.hasItem()) {
      throw new ReferenceNotFoundException(name);
    }

    var i = item.item();

    return referenceFromItem(name, i);
  }

  @Nonnull
  @Override
  public Map<PersistId, FetchedObj> fetch(@Nonnull String realmId, @Nonnull Set<PersistId> ids) {
    var result = new HashMap<PersistId, FetchedObj>();

    var keys = new ArrayList<Map<String, AttributeValue>>();
    for (var id : ids) {
      keys.add(objKeyMap(realmId, id));
      if (keys.size() == BATCH_GET_LIMIT) {
        doFetchObjs(keys, result);
        keys.clear();
      }
    }

    if (!keys.isEmpty()) {
      doFetchObjs(keys, result);
    }

    return result;
  }

  private void doFetchObjs(
      ArrayList<Map<String, AttributeValue>> keys, HashMap<PersistId, FetchedObj> result) {
    var response =
        client.batchGetItem(
            BatchGetItemRequest.builder()
                .requestItems(Map.of(tableObjs, KeysAndAttributes.builder().keys(keys).build()))
                .build());
    for (var objMap : response.responses().getOrDefault(tableObjs, List.of())) {
      var key = objMap.get(KEY_NAME).b();
      try (var in = new DataInputStream(key.asInputStream())) {
        in.readUTF(); // realm
        var id = in.readLong();
        var part = in.readInt();
        var persistId = PersistId.persistId(id, part);
        result.put(persistId, fetchedObj(objMap));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void write(@Nonnull String realmId, @Nonnull List<WriteObj> writes) {
    var numWrites = writes.size();
    for (var i = 0; i < numWrites; i += BATCH_WRITE_LIMIT) {
      var batchWrites = writes.subList(i, Math.min(numWrites, i + BATCH_WRITE_LIMIT));
      var req = BatchWriteItemRequest.builder();
      req.requestItems(
          Map.of(
              tableObjs,
              batchWrites.stream()
                  .map(
                      w ->
                          WriteRequest.builder()
                              .putRequest(
                                  PutRequest.builder()
                                      .item(
                                          objAttributeValues(
                                              realmId,
                                              PersistId.persistId(w.id(), w.part()),
                                              w.type(),
                                              w.partNum(),
                                              w.createdAtMicros(),
                                              w.serialized(),
                                              null))
                                      .build())
                              .build())
                  .toList()));
      client.batchWriteItem(req.build());
    }
  }

  private Map<String, AttributeValue> objAttributeValues(
      String realmId,
      PersistId persistId,
      String objTypeId,
      int partNum,
      long createdAtMicros,
      byte[] serialized,
      String versionToken) {
    var item = new HashMap<String, AttributeValue>();
    item.put(KEY_NAME, objKey(realmId, persistId));
    item.put(COL_OBJ_TYPE, fromS(objTypeId));
    item.put(COL_OBJ_REAL_PART_NUM, fromN(Integer.toString(partNum)));
    item.put(COL_OBJ_VALUE, fromB(fromByteArray(serialized)));
    item.put(COL_OBJ_CREATED_AT, fromN(Long.toString(createdAtMicros)));
    if (versionToken != null) {
      item.put(COL_OBJ_VERSION, fromS(versionToken));
    }
    return item;
  }

  private Map<String, AttributeValueUpdate> objAttributeUpdates(
      long createdAtMicros, byte[] serialized, String versionToken) {
    var updates = new HashMap<String, AttributeValueUpdate>();
    updates.put(COL_OBJ_REAL_PART_NUM, AttributeValueUpdate.builder().value(fromN("1")).build());
    updates.put(
        COL_OBJ_CREATED_AT,
        AttributeValueUpdate.builder().value(fromN(Long.toString(createdAtMicros))).build());
    updates.put(COL_OBJ_VERSION, AttributeValueUpdate.builder().value(fromS(versionToken)).build());
    updates.put(
        COL_OBJ_VALUE,
        AttributeValueUpdate.builder().value(fromB(fromByteArray(serialized))).build());
    return updates;
  }

  @Override
  public void delete(@Nonnull String realmId, @Nonnull Set<PersistId> ids) {
    batchDelete(tableObjs, Map.of(realmId, ids), DynamoDbBackend::objKeyMap);
  }

  @Override
  public boolean conditionalInsert(
      @Nonnull String realmId,
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String versionToken,
      @Nonnull byte[] serializedValue) {
    try {
      client.putItem(
          b ->
              b.tableName(tableObjs)
                  .conditionExpression("attribute_not_exists(" + COL_OBJ_CREATED_AT + ")")
                  .item(
                      objAttributeValues(
                          realmId,
                          persistId,
                          objTypeId,
                          1,
                          createdAtMicros,
                          serializedValue,
                          versionToken)));
      return true;
    } catch (ConditionalCheckFailedException e) {
      throwIfAmbiguousConditionalCheckFailure(e);
      return false;
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public boolean conditionalUpdate(
      @Nonnull String realmId,
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String updateToken,
      @Nonnull String expectedToken,
      @Nonnull byte[] serializedValue) {
    try {
      client.updateItem(
          b ->
              b.tableName(tableObjs)
                  .key(objKeyMap(realmId, persistId))
                  .attributeUpdates(
                      objAttributeUpdates(createdAtMicros, serializedValue, updateToken))
                  .expected(conditionalUpdateExpectedValues(expectedToken)));
      return true;
    } catch (ConditionalCheckFailedException e) {
      throwIfAmbiguousConditionalCheckFailure(e);
      return false;
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public boolean conditionalDelete(
      @Nonnull String realmId, @Nonnull PersistId persistId, @Nonnull String expectedToken) {
    try {
      client.deleteItem(
          b ->
              b.tableName(tableObjs)
                  .key(objKeyMap(realmId, persistId))
                  .expected(conditionalUpdateExpectedValues(expectedToken)));
      return true;
    } catch (ConditionalCheckFailedException checkFailedException) {
      throwIfAmbiguousConditionalCheckFailure(checkFailedException);
      return false;
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public void close() {
    if (closeClient) {
      client.close();
    }
  }

  @Nonnull
  @Override
  public Persistence newPersistence(
      Function<Backend, Backend> backendWrapper,
      @Nonnull PersistenceParams persistenceParams,
      String realmId,
      MonotonicClock monotonicClock,
      IdGenerator idGenerator) {
    return new PersistenceImplementation(
        backendWrapper.apply(this), persistenceParams, realmId, monotonicClock, idGenerator);
  }

  static RuntimeException unhandledException(RuntimeException e) {
    if (e instanceof SdkException sdkException) {
      if (sdkException.retryable()
          || e instanceof ApiCallTimeoutException
          || e instanceof ApiCallAttemptTimeoutException
          || e instanceof AbortedException) {
        return new UnknownOperationResultException(e);
      }
    }
    if (e instanceof AwsServiceException awsServiceException) {
      if (awsServiceException.isThrottlingException()) {
        return new UnknownOperationResultException(e);
      }
    }
    return e;
  }

  static void throwIfAmbiguousConditionalCheckFailure(ConditionalCheckFailedException exception) {
    var attempts = exception.numAttempts();
    if (attempts != null && attempts > 1) {
      throw new UnknownOperationResultException(exception);
    }
  }

  static Map<String, AttributeValue> referenceKeyMap(
      @Nonnull String realm, @Nonnull String reference) {
    return singletonMap(KEY_NAME, referenceKey(realm, reference));
  }

  static AttributeValue referenceKey(@Nonnull String realm, @Nonnull String reference) {
    try (var bytes = new ByteArrayOutputStream();
        var data = new DataOutputStream(bytes)) {
      data.writeUTF(realm);
      data.writeUTF(reference);
      data.flush();
      bytes.flush();
      return fromB(fromByteArray(bytes.toByteArray()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static Map<String, AttributeValue> objKeyMap(
      @Nonnull String realm, @Nonnull PersistId persistId) {
    return singletonMap(KEY_NAME, objKey(realm, persistId));
  }

  static AttributeValue objKey(@Nonnull String realm, @Nonnull PersistId persistId) {
    try (var bytes = new ByteArrayOutputStream();
        var data = new DataOutputStream(bytes)) {
      data.writeUTF(realm);
      data.writeLong(persistId.id());
      data.writeInt(persistId.part());
      data.flush();
      bytes.flush();
      return fromB(fromByteArray(bytes.toByteArray()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static FetchedObj fetchedObj(Map<String, AttributeValue> objMap) {
    var createdAtMicros = Long.parseLong(objMap.get(COL_OBJ_CREATED_AT).n());
    var realNumParts = Integer.parseInt(objMap.get(COL_OBJ_REAL_PART_NUM).n());
    var objTypeId = objMap.get(COL_OBJ_TYPE).s();
    var versionTokenValue = objMap.get(COL_OBJ_VERSION);
    var versionToken = versionTokenValue != null ? versionTokenValue.s() : null;
    var serialized = objMap.get(COL_OBJ_VALUE).b().asByteArray();
    return new FetchedObj(objTypeId, createdAtMicros, versionToken, serialized, realNumParts);
  }

  static Reference referenceFromItem(String name, Map<String, AttributeValue> i) {
    var ref =
        Reference.builder()
            .name(name)
            .previousPointers(previousPointersFromBytes(i.get(COL_REF_PREVIOUS).b().asByteArray()))
            .createdAtMicros(Long.parseLong(i.get(COL_REF_CREATED_AT).n()));
    var pointer = ObjRef.fromBytes(i.get(COL_REF_POINTER).b().asByteArray());
    if (pointer != null) {
      ref.pointer(pointer);
    }
    return ref.build();
  }

  static byte[] previousPointerBytes(long[] pointers) {
    try (var previousOut = new ByteArrayOutputStream();
        var data = new DataOutputStream(previousOut)) {
      data.writeInt(pointers.length);
      for (var p : pointers) {
        data.writeLong(p);
      }
      data.flush();
      previousOut.flush();
      return previousOut.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static long[] previousPointersFromBytes(byte[] pointers) {
    try (var previousIn = new ByteArrayInputStream(pointers);
        var data = new DataInputStream(previousIn)) {
      var len = data.readInt();
      var previousPointers = new long[len];
      for (int i = 0; i < len; i++) {
        previousPointers[i] = data.readLong();
      }
      return previousPointers;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, AttributeValue> referenceAttributeValues(
      String realmId, Reference reference) {
    var item = new HashMap<String, AttributeValue>();
    item.put(KEY_NAME, referenceKey(realmId, reference.name()));
    item.put(COL_REF_POINTER, pointerSdkBytes(reference));
    item.put(COL_REF_CREATED_AT, fromN(Long.toString(reference.createdAtMicros())));

    item.put(
        COL_REF_PREVIOUS, fromB(fromByteArray(previousPointerBytes(reference.previousPointers()))));

    return item;
  }

  private static AttributeValue pointerSdkBytes(Reference reference) {
    return pointerSdkBytes(reference.pointer());
  }

  private static AttributeValue pointerSdkBytes(Optional<ObjRef> pointer) {
    return fromB(fromByteArray(pointer.map(ObjRef::toBytes).orElse(new byte[0])));
  }

  static Map<String, ExpectedAttributeValue> conditionalUpdateExpectedValues(String expectedToken) {
    return Map.of(
        COL_OBJ_VERSION, ExpectedAttributeValue.builder().value(fromS(expectedToken)).build());
  }
}
