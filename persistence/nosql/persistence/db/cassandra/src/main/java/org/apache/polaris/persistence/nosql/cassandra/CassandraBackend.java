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
package org.apache.polaris.persistence.nosql.cassandra;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Map.entry;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.api.backend.PersistId.persistId;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.ADD_REFERENCE;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_OBJ_CREATED_AT;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_OBJ_KEY;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_OBJ_REAL_PART_NUM;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_OBJ_TYPE;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_OBJ_VALUE;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_OBJ_VERSION;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_REALM;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_REF_CREATED_AT;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_REF_NAME;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CREATE_TABLE_OBJS;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CREATE_TABLE_REFS;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.DELETE_OBJ;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.DELETE_OBJ_CONDITIONAL;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.DELETE_REF;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.EXPECTED_SUFFIX;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.FIND_OBJS;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.FIND_REFERENCES;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.MAX_CONCURRENT_BATCH_READS;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.MAX_CONCURRENT_DELETES;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.MAX_CONCURRENT_STORES;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.SCAN_OBJS;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.SCAN_REFS;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.SELECT_BATCH_SIZE;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.STORE_OBJ;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.UPDATE_OBJ;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.UPDATE_REFERENCE_POINTER;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.WRITE_OBJ;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_OBJS;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_REFS;
import static org.apache.polaris.persistence.nosql.impl.PersistenceImplementation.deserialize;
import static org.apache.polaris.persistence.nosql.impl.PersistenceImplementation.serialize;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.QueryConsistencyException;
import jakarta.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
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
import org.apache.polaris.persistence.nosql.api.ref.ImmutableReference;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.apache.polaris.persistence.nosql.impl.PersistenceImplementation;
import org.jspecify.annotations.NonNull;

final class CassandraBackend implements Backend {

  private final Map<String, PreparedStatement> statements = new ConcurrentHashMap<>();
  private final CqlSession session;
  private final CassandraBackendConfig config;

  CassandraBackend(CassandraBackendConfig config) {
    this.config = config;
    this.session = config.client();
  }

  @Override
  @Nonnull
  public String type() {
    return CassandraBackendFactory.NAME;
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

  @Override
  public Optional<String> setupSchema() {
    var metadata = session.getMetadata();
    var keyspace = metadata.getKeyspace(config.keyspace());

    checkState(
        keyspace.isPresent(),
        "Cassandra Keyspace '%s' must exist, but does not exist.",
        config.keyspace());

    createTableIfNotExists(
        keyspace.get(),
        TABLE_REFS,
        CREATE_TABLE_REFS,
        CassandraConstants.EXPECTED_REF_COLUMNS,
        List.of(CQL_COL_REALM, CQL_COL_REF_NAME));
    createTableIfNotExists(
        keyspace.get(),
        TABLE_OBJS,
        CREATE_TABLE_OBJS,
        CassandraConstants.EXPECTED_OBJ_COLUMNS,
        List.of(CQL_COL_REALM, CQL_COL_OBJ_KEY));
    return Optional.of(
        "keyspace: "
            + config.keyspace()
            + " DDL timeout: "
            + config.ddlTimeout()
            + " DML timeout: "
            + config.dmlTimeout());
  }

  private void createTableIfNotExists(
      KeyspaceMetadata meta,
      String tableName,
      String createTable,
      Set<CqlColumn> expectedColumns,
      List<CqlColumn> expectedPrimaryKey) {

    var table = meta.getTable(tableName);

    createTable = format(createTable, meta.getName());

    if (table.isPresent()) {

      checkState(
          checkPrimaryKey(table.get(), expectedPrimaryKey),
          "Expected primary key columns %s do not match existing primary key columns %s for table '%s'. DDL template:\n%s",
          expectedPrimaryKey.stream()
              .map(col -> entry(col.name(), col.type().dataType()))
              .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)),
          table.get().getPartitionKey().stream()
              .map(col -> entry(col.getName(), col.getType()))
              .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)),
          tableName,
          createTable);

      var incompatibleColumns = checkColumns(table.get(), expectedColumns);
      if (!incompatibleColumns.isEmpty()) {
        throw new IllegalStateException(
            format(
                "The database table %s has incompatible mandatory columns %s.%nFound columns : %s%nExpected columns : %s%nDDL template:\n%s",
                tableName,
                sortedColumnNames(incompatibleColumns),
                sortedColumnNames(table.get().getColumns().keySet()),
                sortedColumnNames(expectedColumns),
                createTable));
      }

      // Existing table looks compatible
      return;
    }

    var stmt = SimpleStatement.builder(createTable).setTimeout(config.ddlTimeout()).build();
    session.execute(stmt);
  }

  private static String sortedColumnNames(Collection<?> input) {
    return input.stream().map(Object::toString).sorted().collect(Collectors.joining(","));
  }

  private boolean checkPrimaryKey(TableMetadata table, List<CqlColumn> expectedPrimaryKey) {
    var partitionKey = table.getPartitionKey();
    if (table.getClusteringColumns().isEmpty()
        && partitionKey.size() == expectedPrimaryKey.size()) {
      for (var i = 0; i < partitionKey.size(); i++) {
        var column = partitionKey.get(i);
        var expectedColumn = expectedPrimaryKey.get(i);
        if (!column.getName().asInternal().equals(expectedColumn.name())
            || !column.getType().equals(expectedColumn.type().dataType())) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private static List<String> checkColumns(TableMetadata table, Set<CqlColumn> expectedColumns) {
    var incompatible = new ArrayList<String>();
    for (var expectedColumn : expectedColumns) {
      var actualColumn = table.getColumn(expectedColumn.name());
      if (actualColumn.isEmpty()) {
        incompatible.add(expectedColumn.name());
      } else if (!actualColumn.get().getType().equals(expectedColumn.type().dataType())) {
        incompatible.add(
            "%s (expected %s, found %s)"
                .formatted(
                    expectedColumn.name(),
                    expectedColumn.type().dataType(),
                    actualColumn.get().getType()));
      }
    }
    return incompatible;
  }

  @Override
  public void deleteRealms(Set<String> realmIds) {
    throw new UnsupportedOperationException("Backend does not support deletion of realms.");
  }

  @Override
  public boolean supportsRealmDeletion() {
    return false;
  }

  @Override
  public void batchDeleteRefs(Map<String, Set<String>> realmRefs) {
    try (var requests = new LimitedConcurrentRequests(MAX_CONCURRENT_DELETES)) {
      for (var r : realmRefs.entrySet()) {
        var realmId = r.getKey();
        for (var ref : r.getValue()) {
          requests.submitted(executeAsync(buildIdempotentStatement(DELETE_REF, realmId, ref)));
        }
      }
    } catch (DriverException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public void batchDeleteObjs(Map<String, Set<PersistId>> realmObjs) {
    try (var requests = new LimitedConcurrentRequests(MAX_CONCURRENT_DELETES)) {
      for (var r : realmObjs.entrySet()) {
        var realmId = r.getKey();
        for (var obj : r.getValue()) {
          requests.submitted(
              executeAsync(
                  buildIdempotentStatement(
                      DELETE_OBJ, realmId, serializePersistId(persistId(obj.id(), obj.part())))));
        }
      }
    } catch (DriverException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public void scanBackend(
      @Nonnull ReferenceScanCallback referenceConsumer, @Nonnull ObjScanCallback objConsumer) {
    var stmt = buildIdempotentStatement(SCAN_REFS);
    var rs = execute(stmt);
    for (Row row : rs) {
      var realm = requireNonNull(row.getString(CQL_COL_REALM.name()));
      var ref = requireNonNull(row.getString(CQL_COL_REF_NAME.name()));
      var cr = row.getLong(CQL_COL_REF_CREATED_AT.name());
      referenceConsumer.call(realm, ref, cr);
    }
    stmt = buildIdempotentStatement(SCAN_OBJS);
    rs = execute(stmt);
    for (Row row : rs) {
      var realm = requireNonNull(row.getString(CQL_COL_REALM.name()));
      var pid = deSerializePersistId(requireNonNull(row.getByteBuffer(CQL_COL_OBJ_KEY.name())));
      var cr = row.getLong(CQL_COL_OBJ_CREATED_AT.name());
      String type = requireNonNull(row.getString(CQL_COL_OBJ_TYPE.name()));
      objConsumer.call(realm, type, persistId(pid.id(), pid.part()), cr);
    }
  }

  @Override
  public boolean createReference(@Nonnull String realmId, @Nonnull Reference newRef) {
    var serializedPreviousPointers = serialize(newRef.previousPointers());
    var previous =
        serializedPreviousPointers != null ? ByteBuffer.wrap(serializedPreviousPointers) : null;
    var stmt =
        buildLwtStatement(
            ADD_REFERENCE,
            realmId,
            newRef.name(),
            serializeObjId(newRef.pointer()),
            newRef.createdAtMicros(),
            previous);
    return executeCas(stmt);
  }

  @Override
  public void createReferences(@Nonnull String realmId, @Nonnull List<Reference> newRefs) {
    for (Reference newRef : newRefs) {
      createReference(realmId, newRef);
    }
  }

  @Override
  public boolean updateReference(
      @Nonnull String realmId,
      @Nonnull Reference updatedRef,
      @Nonnull Optional<ObjRef> expectedPointer) {
    var serializedPreviousPointers = serialize(updatedRef.previousPointers());
    var previous =
        serializedPreviousPointers != null ? ByteBuffer.wrap(serializedPreviousPointers) : null;
    var stmt =
        buildLwtStatement(
            UPDATE_REFERENCE_POINTER,
            serializeObjId(updatedRef.pointer()),
            previous,
            realmId,
            updatedRef.name(),
            serializeObjId(expectedPointer),
            updatedRef.createdAtMicros());
    if (executeCas(stmt)) {
      return true;
    }
    fetchReference(realmId, updatedRef.name());
    return false;
  }

  @Nonnull
  @Override
  public Reference fetchReference(@Nonnull String realmId, @Nonnull String name) {
    try {
      var row = execute(buildLwtStatement(FIND_REFERENCES, realmId, List.of(name))).one();
      if (row == null) {
        throw new ReferenceNotFoundException(name);
      }
      return deserializeReference(row);
    } catch (DriverException e) {
      throw unhandledException(e);
    }
  }

  public static Reference deserializeReference(Row row) {
    var previousBytes = row.getByteBuffer(3);
    var previousPointers =
        previousBytes != null ? deserialize(previousBytes, long[].class) : new long[0];
    return ImmutableReference.builder()
        .name(row.getString(0))
        .pointer(Optional.ofNullable(deSerializeObjId(row.getByteBuffer(1))))
        .createdAtMicros(row.getLong(2))
        .previousPointers(previousPointers)
        .build();
  }

  @Nonnull
  @Override
  @SuppressWarnings("ByteBufferBackingArray")
  public Map<PersistId, FetchedObj> fetch(@Nonnull String realmId, @Nonnull Set<PersistId> ids) {
    Function<Row, FetchedObj> rowMapper =
        row -> {
          var objTypeId = row.getString(CQL_COL_OBJ_TYPE.name());
          var versionToken = row.getString(CQL_COL_OBJ_VERSION.name());
          var serialized = requireNonNull(row.getByteBuffer(CQL_COL_OBJ_VALUE.name()));
          var createdAtMicros = row.getLong(CQL_COL_OBJ_CREATED_AT.name());
          var realPartNum = row.getInt(CQL_COL_OBJ_REAL_PART_NUM.name());
          if (serialized.position() == 0
              && serialized.remaining() == serialized.capacity()
              && serialized.hasArray()) {
            // fast path can use the byte[] of the ByteBuffer
            return new FetchedObj(
                objTypeId, createdAtMicros, versionToken, serialized.array(), realPartNum);
          }
          // slow path, must copy
          var bin = new byte[serialized.remaining()];
          serialized.get(bin);
          return new FetchedObj(objTypeId, createdAtMicros, versionToken, bin, realPartNum);
        };
    Function<Row, PersistId> idMapper =
        row -> deSerializePersistId(requireNonNull(row.getByteBuffer(CQL_COL_OBJ_KEY.name())));

    try (CassandraBackend.BatchedQuery<PersistId, FetchedObj> batchedQuery =
        newBatchedQuery(
            keys -> {
              Function<List<PersistId>, List<ByteBuffer>> idsToByteBuffers =
                  queryIds -> queryIds.stream().map(CassandraBackend::serializePersistId).toList();
              return executeAsync(
                  buildIdempotentStatement(FIND_OBJS, realmId, idsToByteBuffers.apply(keys)));
            },
            rowMapper,
            idMapper,
            ids.size())) {

      for (var id : ids) {
        if (id != null) {
          batchedQuery.add(id);
        }
      }

      return batchedQuery.finish();
    } catch (DriverException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public void write(@Nonnull String realmId, @Nonnull List<WriteObj> writes) {
    var numWrites = writes.size();
    var results = new AtomicIntegerArray(numWrites);

    try (LimitedConcurrentRequests requests =
        new LimitedConcurrentRequests(MAX_CONCURRENT_STORES)) {
      for (int i = 0; i < numWrites; i++) {
        var write = writes.get(i);
        var idx = i;
        var cs =
            writeSingleObj(
                    realmId,
                    PersistId.persistId(write.id(), write.part()),
                    write.type(),
                    write.createdAtMicros(),
                    null,
                    write.serialized(),
                    true,
                    write.partNum(),
                    this::executeAsync)
                .handle((resultSet, e) -> {
                  if (e != null) {
                    if (e instanceof DriverException de) {
                      throw unhandledException(de);
                    }
                    if (e instanceof RuntimeException re) {
                      throw re;
                    }
                    throw new RuntimeException(e);
                  }
                  if (resultSet.wasApplied()) {
                    results.set(idx, 1);
                  } else {
                    results.set(idx, 2);
                  }
                  return null;
                });
        requests.submitted(cs);
      }
    } catch (DriverException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public void delete(@Nonnull String realmId, @Nonnull Set<PersistId> ids) {
    try (var requests = new LimitedConcurrentRequests(MAX_CONCURRENT_DELETES)) {
      for (var id : ids) {
        if (id != null) {
          requests.submitted(
              executeAsync(buildIdempotentStatement(DELETE_OBJ, realmId, serializePersistId(id))));
        }
      }
    } catch (DriverException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public boolean conditionalInsert(
      @Nonnull String realmId,
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String versionToken,
      @Nonnull byte[] serializedValue) {
    return writeSingleObj(
        realmId,
        persistId,
        objTypeId,
        createdAtMicros,
        versionToken,
        serializedValue,
        false,
        1,
        this::executeCas);
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
    BoundStatementBuilder stmt =
        newLwtStatementBuilder(UPDATE_OBJ)
            .setString(CQL_COL_REALM.name(), realmId)
            .setByteBuffer(CQL_COL_OBJ_KEY.name(), serializePersistId(persistId))
            .setString(CQL_COL_OBJ_TYPE.name(), objTypeId)
            .setString(CQL_COL_OBJ_VERSION.name() + EXPECTED_SUFFIX, expectedToken)
            .setLong(CQL_COL_OBJ_CREATED_AT.name(), createdAtMicros)
            .setString(CQL_COL_OBJ_VERSION.name(), updateToken)
            .setByteBuffer(CQL_COL_OBJ_VALUE.name(), ByteBuffer.wrap(serializedValue));

    return executeCas(stmt.build());
  }

  @Override
  public boolean conditionalDelete(
      @Nonnull String realmId, @Nonnull PersistId persistId, @Nonnull String expectedToken) {
    return executeCas(
        newLwtStatementBuilder(DELETE_OBJ_CONDITIONAL)
            .setString(CQL_COL_REALM.name(), realmId)
            .setByteBuffer(CQL_COL_OBJ_KEY.name(), serializePersistId(persistId))
            .setString(CQL_COL_OBJ_VERSION.name(), expectedToken)
            .build());
  }

  @FunctionalInterface
  private interface WriteSingleObj<R> {
    R apply(BoundStatement stmt);
  }

  private <R> R writeSingleObj(
      @Nonnull String realmId,
      @Nonnull PersistId fetchId,
      @Nonnull String objTypeId,
      long createdAtMicros,
      String versionToken,
      @Nonnull byte[] serializedValue,
      boolean idempotent,
      int partNum,
      WriteSingleObj<R> consumer) {
    return consumer.apply(
        (idempotent ? newBoundStatementBuilder(WRITE_OBJ, true) : newLwtStatementBuilder(STORE_OBJ))
            .setString(CQL_COL_REALM.name(), realmId)
            .setByteBuffer(CQL_COL_OBJ_KEY.name(), serializePersistId(fetchId))
            .setString(CQL_COL_OBJ_TYPE.name(), objTypeId)
            .setLong(CQL_COL_OBJ_CREATED_AT.name(), createdAtMicros)
            .setString(CQL_COL_OBJ_VERSION.name(), versionToken)
            .setByteBuffer(CQL_COL_OBJ_VALUE.name(), ByteBuffer.wrap(serializedValue))
            .setInt(CQL_COL_OBJ_REAL_PART_NUM.name(), partNum)
            .build());
  }

  static ByteBuffer serializeObjId(@Nonnull Optional<ObjRef> id) {
    return ByteBuffer.wrap(serialize(id.orElse(null)));
  }

  static ObjRef deSerializeObjId(ByteBuffer id) {
    if (id != null) {
      return deserialize(id, ObjRef.class);
    }
    return null;
  }

  static @Nonnull ByteBuffer serializePersistId(@Nonnull PersistId id) {
    return ByteBuffer.wrap(serialize(id));
  }

  static @Nonnull PersistId deSerializePersistId(@Nonnull ByteBuffer id) {
    return deserialize(id, PersistId.class);
  }

  @Override
  public void close() {
    if (config.closeClient()) {
      session.close();
    }
  }

  <K, R> BatchedQuery<K, R> newBatchedQuery(
      Function<List<K>, CompletionStage<AsyncResultSet>> queryBuilder,
      Function<Row, R> rowToResult,
      Function<Row, K> idExtractor,
      int results) {
    return new BatchedQueryImpl<>(queryBuilder, rowToResult, idExtractor, results);
  }

  interface BatchedQuery<K, R> extends AutoCloseable {
    void add(K key);

    Map<K, R> finish();

    @Override
    void close();
  }

  private static final class BatchedQueryImpl<K, R> implements BatchedQuery<K, R> {

    private final Function<List<K>, CompletionStage<AsyncResultSet>> queryBuilder;
    private final List<K> keys = new ArrayList<>();
    private final Semaphore permits = new Semaphore(MAX_CONCURRENT_BATCH_READS);
    private final Function<Row, R> rowToResult;
    private final Function<Row, K> idExtractor;
    private final Map<K, R> result;
    private boolean completionAttempted;
    private volatile Throwable failure;
    private volatile int queryCount;
    private volatile int queriesCompleted;

    BatchedQueryImpl(
        Function<List<K>, CompletionStage<AsyncResultSet>> queryBuilder,
        Function<Row, R> rowToResult,
        Function<Row, K> idExtractor,
        int results) {
      this.result = new ConcurrentHashMap<>(results);
      this.rowToResult = rowToResult;
      this.idExtractor = idExtractor;
      this.queryBuilder = queryBuilder;
    }

    @Override
    public void add(K key) {
      keys.add(key);
      if (keys.size() == SELECT_BATCH_SIZE) {
        flush();
      }
    }

    private void noteException(Throwable ex) {
      synchronized (this) {
        var curr = failure;
        if (curr != null) {
          curr.addSuppressed(ex);
        } else {
          failure = ex;
        }
      }
    }

    private void flush() {
      if (keys.isEmpty()) {
        return;
      }

      var batchKeys = new ArrayList<>(keys);
      keys.clear();

      try {
        permits.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }

      synchronized (this) {
        queryCount++;
      }

      Runnable terminate =
          () -> {
            // Remove the completed query from the queue, so another query can be submitted.
            permits.release();
            // Increment the number of completed queries and notify the "driver".
            synchronized (this) {
              queriesCompleted++;
              this.notifyAll();
            }
          };

      CompletionStage<AsyncResultSet> query;
      try {
        query = requireNonNull(queryBuilder.apply(batchKeys));
      } catch (Throwable t) {
        noteException(t);
        terminate.run();
        return;
      }

      var pageHandler =
          new BiFunction<AsyncResultSet, Throwable, Object>() {
            @Override
            public Object apply(AsyncResultSet rs, Throwable ex) {
              if (ex != null) {
                noteException(ex);
                terminate.run();
              } else {
                try {
                  for (var row : rs.currentPage()) {
                    var id = idExtractor.apply(row);
                    var resultItem = rowToResult.apply(row);
                    if (resultItem != null) {
                      result.put(id, resultItem);
                    }
                  }

                  if (rs.hasMorePages()) {
                    rs.fetchNextPage().handleAsync(this);
                  } else {
                    terminate.run();
                  }

                } catch (Throwable t) {
                  noteException(t);
                  terminate.run();
                }
              }
              return null;
            }
          };

      query.handleAsync(pageHandler);
    }

    @Override
    public void close() {
      synchronized (this) {
        if (completionAttempted) {
          return;
        }
      }
      finish();
    }

    @Override
    public Map<K, R> finish() {
      synchronized (this) {
        if (completionAttempted) {
          return result;
        }
        completionAttempted = true;
      }
      flush();

      while (true) {
        synchronized (this) {
          // If a failure happened, there's not much that can be done, just re-throw
          var f = failure;
          if (f != null) {
            if (f instanceof RuntimeException re) {
              throw re;
            }
            throw new RuntimeException(f);
          } else if (queriesCompleted == queryCount) {
            // No failure, all queries completed.
            break;
          }

          try {
            this.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
      }

      return result;
    }
  }

  @Nonnull
  BoundStatement buildIdempotentStatement(String cql, Object... values) {
    return newBoundStatementBuilder(cql, true, values).build();
  }

  @Nonnull
  BoundStatement buildLwtStatement(String cql, Object... values) {
    return newBoundStatementBuilder(cql, false, values)
        .setSerialConsistencyLevel(config.serialConsistencyLevel())
        .build();
  }

  @Nonnull
  BoundStatementBuilder newLwtStatementBuilder(String cql) {
    return newBoundStatementBuilder(cql, false)
        .setSerialConsistencyLevel(config.serialConsistencyLevel());
  }

  @Nonnull
  BoundStatementBuilder newBoundStatementBuilder(String cql, boolean idempotent, Object... values) {
    return preparedStatement(cql)
        .boundStatementBuilder(values)
        .setTimeout(config.dmlTimeout())
        .setConsistencyLevel(config.consistencyLevel())
        .setIdempotence(idempotent);
  }

  private @NonNull PreparedStatement preparedStatement(String cql) {
    return statements.computeIfAbsent(cql, c -> session.prepare(format(c, config.keyspace())));
  }

  boolean executeCas(BoundStatement stmt) {
    try {
      var rs = session.execute(stmt);
      return rs.wasApplied();
    } catch (DriverException e) {
      throw unhandledCasException(e);
    }
  }

  ResultSet execute(BoundStatement stmt) {
    try {
      return session.execute(stmt);
    } catch (DriverException e) {
      throw unhandledException(e);
    }
  }

  CompletionStage<AsyncResultSet> executeAsync(BoundStatement stmt) {
    return session.executeAsync(stmt);
  }

  static RuntimeException unhandledException(DriverException e) {
    if (isUnknownOperationResult(e)) {
      return new UnknownOperationResultException(e);
    } else if (e instanceof AllNodesFailedException all) {
      if (all.getAllErrors().values().stream()
          .flatMap(List::stream)
          .filter(DriverException.class::isInstance)
          .map(DriverException.class::cast)
          .anyMatch(CassandraBackend::isUnknownOperationResult)) {
        return new UnknownOperationResultException(e);
      }
    }
    return e;
  }

  static RuntimeException unhandledCasException(DriverException e) {
    if (isUnknownCasOperationResult(e)) {
      return new UnknownOperationResultException(e);
    }
    return unhandledException(e);
  }

  private static boolean isUnknownCasOperationResult(DriverException e) {
    if (e instanceof ClosedConnectionException) {
      return true;
    }
    return e instanceof AllNodesFailedException all
        && all.getAllErrors().values().stream()
            .flatMap(List::stream)
            .anyMatch(ClosedConnectionException.class::isInstance);
  }

  private static boolean isUnknownOperationResult(DriverException e) {
    return e instanceof QueryConsistencyException || e instanceof DriverTimeoutException;
  }
}
