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

import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_OBJ_KEY;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_OBJ_VALUE;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_OBJ_VERSION;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_REF_CREATED_AT;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_REF_NAME;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.SELECT_BATCH_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverExecutionException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.backend.PersistId;
import org.apache.polaris.persistence.nosql.api.backend.WriteObj;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.nosql.api.exceptions.UnknownOperationResultException;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(10)
class CassandraBackendTest {
  private final CqlSession session = mock(CqlSession.class);
  private final PreparedStatement prepared = mock(PreparedStatement.class);
  private final BoundStatementBuilder builder = mock(BoundStatementBuilder.class, RETURNS_SELF);
  private final BoundStatement statement = mock(BoundStatement.class);

  @BeforeEach
  void prepareStatements() {
    when(session.prepare(anyString())).thenReturn(prepared);
    when(prepared.boundStatementBuilder(any(Object[].class))).thenReturn(builder);
    when(builder.build()).thenReturn(statement);
  }

  @ParameterizedTest
  @CsvSource({"LOCAL_QUORUM,LOCAL_SERIAL", "QUORUM,SERIAL", "ALL,SERIAL"})
  void referenceAndObjectReadsUseSeparateConsistencyLevels(
      DefaultConsistencyLevel consistency, DefaultConsistencyLevel serialConsistency) {
    var serialReferenceStatement = mock(BoundStatement.class);
    var objectStatement = mock(BoundStatement.class);
    when(builder.build()).thenReturn(statement, objectStatement);
    when(statement.setConsistencyLevel(serialConsistency)).thenReturn(serialReferenceStatement);
    var referenceResult = mock(ResultSet.class);
    var row = mock(Row.class);
    when(session.execute(serialReferenceStatement)).thenReturn(referenceResult);
    when(referenceResult.one()).thenReturn(row);
    when(row.getString(0)).thenReturn("ref");
    when(row.getLong(2)).thenReturn(123L);
    var objectResult = mock(AsyncResultSet.class);
    when(objectResult.currentPage()).thenReturn(List.of());
    when(session.executeAsync(objectStatement))
        .thenReturn(CompletableFuture.completedFuture(objectResult));
    var backend = backend(consistency, serialConsistency);

    assertThat(backend.fetchReference("realm", "ref").name()).isEqualTo("ref");
    assertThat(backend.fetch("realm", Set.of(PersistId.persistId(1L, 0)))).isEmpty();

    verify(statement).setConsistencyLevel(serialConsistency);
    verify(builder, times(2)).setConsistencyLevel(consistency);
    verify(builder, times(2)).setIdempotence(true);
    verify(session).execute(serialReferenceStatement);
    verify(session).executeAsync(objectStatement);
    verify(session).prepare(endsWith(CQL_COL_REF_NAME + "=?"));
    verify(prepared).boundStatementBuilder("realm", "ref");
  }

  @ParameterizedTest
  @MethodSource("objectFetchBatches")
  void objectFetchUsesEqualityForSingletonBatches(int count, int singletons, int multiKeyBatches) {
    var result = mock(AsyncResultSet.class);
    when(result.currentPage()).thenReturn(List.of());
    when(session.executeAsync(statement)).thenReturn(CompletableFuture.completedFuture(result));
    var ids =
        IntStream.range(0, count)
            .mapToObj(id -> PersistId.persistId(id, 0))
            .collect(Collectors.toSet());

    assertThat(backend().fetch("realm", ids)).isEmpty();

    verify(session, times(singletons)).prepare(endsWith(CQL_COL_OBJ_KEY + "=?"));
    verify(session, times(multiKeyBatches > 0 ? 1 : 0))
        .prepare(endsWith(CQL_COL_OBJ_KEY + " IN ?"));
    verify(prepared, times(singletons)).boundStatementBuilder(eq("realm"), any(ByteBuffer.class));
    verify(prepared, times(multiKeyBatches)).boundStatementBuilder(eq("realm"), anyList());
    verify(session, times(singletons + multiKeyBatches)).executeAsync(statement);
  }

  static Stream<Arguments> objectFetchBatches() {
    return Stream.of(
        Arguments.of(0, 0, 0),
        Arguments.of(1, 1, 0),
        Arguments.of(2, 0, 1),
        Arguments.of(SELECT_BATCH_SIZE, 0, 1),
        Arguments.of(SELECT_BATCH_SIZE + 1, 1, 1));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void writeTimeoutHasUnknownOutcome(int completionWrappers) {
    var timeout = new DriverTimeoutException("write timed out");
    RuntimeException failure = timeout;
    for (var i = 0; i < completionWrappers; i++) {
      failure = new CompletionException(failure);
    }
    when(session.executeAsync(statement)).thenReturn(CompletableFuture.failedFuture(failure));
    var writes = List.of(new WriteObj("test", 1L, 0, 123L, new byte[] {1}, 1));

    assertThatThrownBy(() -> backend().write("realm", writes))
        .isInstanceOf(UnknownOperationResultException.class)
        .cause()
        .hasSuppressedException(timeout);
  }

  @ParameterizedTest
  @MethodSource("bulkOperations")
  void bulkFailureClassificationDoesNotDependOnCompletionOrder(
      String operation, boolean unknownFirst) {
    var definite = new IllegalArgumentException("request rejected");
    var unknown = new DriverTimeoutException("request timed out");
    RuntimeException first = unknownFirst ? unknown : definite;
    RuntimeException second = unknownFirst ? definite : unknown;
    when(session.executeAsync(statement))
        .thenReturn(CompletableFuture.failedFuture(first))
        .thenReturn(CompletableFuture.failedFuture(second));
    var backend = backend();
    var ids = Set.of(PersistId.persistId(1L, 0), PersistId.persistId(2L, 0));

    assertThatThrownBy(
            () -> {
              switch (operation) {
                case "write" ->
                    backend.write(
                        "realm",
                        List.of(
                            new WriteObj("test", 1L, 0, 123L, new byte[] {1}, 1),
                            new WriteObj("test", 2L, 0, 123L, new byte[] {2}, 1)));
                case "delete" -> backend.delete("realm", ids);
                case "batchDeleteObjs" -> backend.batchDeleteObjs(Map.of("realm", ids));
                case "batchDeleteRefs" ->
                    backend.batchDeleteRefs(Map.of("realm", Set.of("one", "two")));
                default -> throw new AssertionError(operation);
              }
            })
        .isInstanceOf(UnknownOperationResultException.class)
        .cause()
        .hasSuppressedException(first)
        .hasSuppressedException(second);
    assertThat(definite.getSuppressed()).isEmpty();
    assertThat(unknown.getSuppressed()).isEmpty();
  }

  static Stream<Arguments> bulkOperations() {
    return Stream.of("write", "delete", "batchDeleteObjs", "batchDeleteRefs")
        .flatMap(
            operation -> Stream.of(Arguments.of(operation, false), Arguments.of(operation, true)));
  }

  @Test
  void synchronousSubmissionFailurePreservesOutstandingUnknownOutcome() {
    var unknown = new DriverTimeoutException("request timed out");
    var submissionFailure = new IllegalArgumentException("submission failed");
    when(session.executeAsync(statement))
        .thenReturn(CompletableFuture.failedFuture(unknown))
        .thenThrow(submissionFailure);

    assertThatThrownBy(() -> backend().batchDeleteRefs(Map.of("realm", Set.of("one", "two"))))
        .isInstanceOf(UnknownOperationResultException.class)
        .cause()
        .hasSuppressedException(unknown)
        .hasSuppressedException(submissionFailure);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void mapperFindsTimeoutInSuppressedNodeErrors(boolean cas) {
    var timeout = new DriverTimeoutException("request timed out");
    var nodeFailure =
        AllNodesFailedException.fromErrors(List.of(Map.entry(mock(Node.class), timeout)));
    var failure = new IllegalStateException("batch failed");
    var sibling = new DriverExecutionException(new IllegalArgumentException("rejected"));
    sibling.addSuppressed(new CompletionException(nodeFailure));
    failure.addSuppressed(sibling);

    var mapped =
        cas
            ? CassandraBackend.unhandledCasException(failure)
            : CassandraBackend.unhandledException(failure);
    assertThat(mapped).isInstanceOf(UnknownOperationResultException.class).hasCause(failure);
  }

  @ParameterizedTest
  @MethodSource("casConnectionFailures")
  void mapperPreservesCasDistinctionForNestedConnectionFailures(DriverException failure) {
    var aggregate = new IllegalStateException("batch failed");
    aggregate.addSuppressed(new CompletionException(failure));

    assertThat(CassandraBackend.unhandledCasException(aggregate))
        .isInstanceOf(UnknownOperationResultException.class)
        .hasCause(aggregate);
    assertThat(CassandraBackend.unhandledException(aggregate)).isSameAs(aggregate);
  }

  @Test
  void driverExecutionWrapperPreservesUnknownOutcome() {
    var failure = new DriverExecutionException(new DriverTimeoutException("timed out"));
    assertThat(CassandraBackend.unhandledCasException(failure))
        .isInstanceOf(UnknownOperationResultException.class)
        .hasCause(failure);
  }

  @Test
  void mapperHandlesCyclesAndSharedExceptions() {
    var failure = new IllegalStateException("batch failed");
    var nested = new IllegalStateException("nested");
    failure.initCause(nested);
    nested.initCause(failure);
    nested.addSuppressed(new DriverTimeoutException("timed out"));
    failure.addSuppressed(nested);

    assertThat(CassandraBackend.unhandledException(failure))
        .isInstanceOf(UnknownOperationResultException.class)
        .hasCause(failure);
  }

  @Test
  void localInterruptionAloneIsNotAnUnknownOutcome() {
    var failure = new RuntimeException(new InterruptedException("waiting for permit"));
    assertThat(CassandraBackend.unhandledCasException(failure)).isSameAs(failure);
    failure.addSuppressed(new DriverTimeoutException("outstanding request timed out"));
    assertThat(CassandraBackend.unhandledCasException(failure))
        .isInstanceOf(UnknownOperationResultException.class)
        .hasCause(failure);
  }

  @Test
  void fetchMapsWrappedTimeout() {
    var timeout = new DriverTimeoutException("read timed out");
    when(session.executeAsync(statement))
        .thenReturn(CompletableFuture.failedFuture(new CompletionException(timeout)));

    assertThatThrownBy(() -> backend().fetch("realm", Set.of(PersistId.persistId(1L, 0))))
        .isInstanceOf(UnknownOperationResultException.class)
        .hasRootCause(timeout);
  }

  @Test
  void immutableWriteLeavesVersionTokenUnset() {
    when(session.executeAsync(statement))
        .thenReturn(CompletableFuture.completedFuture(mock(AsyncResultSet.class)));

    backend().write("realm", List.of(new WriteObj("test", 1L, 0, 123L, new byte[] {1}, 1)));

    verify(builder, never()).setString(eq(CQL_COL_OBJ_VERSION.name()), any());
  }

  @Test
  void conditionalInsertBindsVersionToken() {
    var result = mock(ResultSet.class);
    when(result.wasApplied()).thenReturn(true);
    when(session.execute(statement)).thenReturn(result);

    assertThat(
            backend()
                .conditionalInsert(
                    "realm", "test", PersistId.persistId(1L, 0), 123L, "version", new byte[] {1}))
        .isTrue();

    verify(builder).setString(CQL_COL_OBJ_VERSION.name(), "version");
    verify(builder).setIdempotence(false);
  }

  @Test
  void referenceDeletionUsesLwt() {
    when(session.executeAsync(statement))
        .thenReturn(CompletableFuture.completedFuture(mock(AsyncResultSet.class)));

    backend().batchDeleteRefs(Map.of("realm", Set.of("ref")));

    verify(session).prepare(endsWith(CQL_COL_REF_NAME + "=? IF EXISTS"));
    verify(builder).setIdempotence(false);
    verify(builder).setSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL);
  }

  @ParameterizedTest
  @MethodSource("casConnectionFailures")
  void referenceDeletionPreservesUnknownOutcome(DriverException failure) {
    when(session.executeAsync(statement)).thenReturn(CompletableFuture.failedFuture(failure));

    assertThatThrownBy(() -> backend().batchDeleteRefs(Map.of("realm", Set.of("ref"))))
        .isInstanceOf(UnknownOperationResultException.class)
        .cause()
        .hasSuppressedException(failure);
  }

  @Test
  void objectDeletionUsesOrdinaryWrites() {
    when(session.executeAsync(statement))
        .thenReturn(CompletableFuture.completedFuture(mock(AsyncResultSet.class)));
    var ids = Set.of(PersistId.persistId(1L, 0));

    backend().delete("realm", ids);
    backend().batchDeleteObjs(Map.of("realm", ids));

    verify(session, times(2)).prepare(endsWith(CQL_COL_OBJ_KEY + "=?"));
    verify(builder, times(2)).setIdempotence(true);
    verify(builder, never()).setSerialConsistencyLevel(any());
  }

  @ParameterizedTest
  @MethodSource("serializedBuffers")
  void fetchUsesOnlyTheSerializedBufferContents(ByteBuffer serialized) {
    var id = PersistId.persistId(1L, 0);
    var row = mock(Row.class);
    when(row.getByteBuffer(CQL_COL_OBJ_KEY.name()))
        .thenReturn(CassandraBackend.serializePersistId(id));
    when(row.getByteBuffer(CQL_COL_OBJ_VALUE.name())).thenReturn(serialized);
    var result = mock(AsyncResultSet.class);
    when(result.currentPage()).thenReturn(List.of(row));
    when(session.executeAsync(statement)).thenReturn(CompletableFuture.completedFuture(result));

    assertThat(backend().fetch("realm", Set.of(id)).get(id).serialized()).containsExactly(1, 2, 3);
  }

  static Stream<ByteBuffer> serializedBuffers() {
    return Stream.of(
        ByteBuffer.wrap(new byte[] {1, 2, 3}),
        ByteBuffer.wrap(new byte[] {9, 1, 2, 3, 9}, 1, 3).slice(),
        ByteBuffer.wrap(new byte[] {1, 2, 3, 9}).slice(0, 3),
        ByteBuffer.wrap(new byte[] {1, 2, 3}).asReadOnlyBuffer(),
        ByteBuffer.allocateDirect(3).put(new byte[] {1, 2, 3}).flip());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void referenceUpdateUsesCasResult(boolean applied) {
    var result = mock(ResultSet.class);
    var columns = mock(ColumnDefinitions.class);
    when(session.execute(statement)).thenReturn(result);
    when(result.wasApplied()).thenReturn(applied);
    when(result.getColumnDefinitions()).thenReturn(columns);
    when(columns.contains(CQL_COL_REF_CREATED_AT.name())).thenReturn(true);
    var reference = reference("ref");

    assertThat(backend().updateReference("realm", reference, Optional.empty())).isEqualTo(applied);

    verify(session).execute(statement);
  }

  @Test
  void referenceUpdateDetectsMissingReferenceFromCasResult() {
    var result = mock(ResultSet.class);
    when(session.execute(statement)).thenReturn(result);
    when(result.getColumnDefinitions()).thenReturn(mock(ColumnDefinitions.class));
    var reference = reference("missing");

    assertThatThrownBy(() -> backend().updateReference("realm", reference, Optional.empty()))
        .isInstanceOf(ReferenceNotFoundException.class)
        .hasMessage("missing");

    verify(session).execute(statement);
  }

  @ParameterizedTest
  @MethodSource("casConnectionFailures")
  void referenceUpdatePreservesUnknownOutcome(DriverException failure) {
    when(session.execute(statement)).thenThrow(failure);
    var reference = reference("ref");

    assertThatThrownBy(() -> backend().updateReference("realm", reference, Optional.empty()))
        .isInstanceOf(UnknownOperationResultException.class)
        .hasCause(failure);
  }

  @ParameterizedTest
  @MethodSource("casConnectionFailures")
  void casConnectionFailureHasUnknownOutcome(DriverException exception) {
    var mapped = CassandraBackend.unhandledCasException(exception);

    assertThat(mapped).isInstanceOf(UnknownOperationResultException.class);
    assertThat(mapped.getCause()).isSameAs(exception);
    assertThat(CassandraBackend.unhandledException(exception)).isSameAs(exception);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void referenceUpdateTimeoutPreservesUnknownOutcome(boolean nodeFailure) {
    DriverException failure = new DriverTimeoutException("CAS timed out");
    if (nodeFailure) {
      failure = AllNodesFailedException.fromErrors(List.of(Map.entry(mock(Node.class), failure)));
    }
    when(session.execute(statement)).thenThrow(failure);
    assertThatThrownBy(() -> backend().updateReference("realm", reference("ref"), Optional.empty()))
        .isInstanceOf(UnknownOperationResultException.class)
        .hasCause(failure);
  }

  static Stream<DriverException> casConnectionFailures() {
    return Stream.of(
            new ClosedConnectionException("connection closed"),
            new HeartbeatException(
                InetSocketAddress.createUnresolved("localhost", 9042),
                "heartbeat failed",
                new DriverTimeoutException("heartbeat timed out")))
        .flatMap(failure ->
            Stream.of(
                failure,
                AllNodesFailedException.fromErrors(List.of(Map.entry(mock(Node.class), failure)))));
  }

  @Test
  void batchedQueryCollectsAllPages() {
    var firstPage = resultSet(0, 2);
    var secondPage = resultSet(2, 3);
    when(firstPage.hasMorePages()).thenReturn(true);
    when(firstPage.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(secondPage));

    try (var query =
        backend()
            .newBatchedQuery(
                keys -> {
                  assertThat(keys).containsExactly(0, 1, 2);
                  return CompletableFuture.completedFuture(firstPage);
                },
                row -> row.getInt("id"),
                row -> row.getInt("id"),
                3)) {
      IntStream.range(0, 3).forEach(query::add);

      assertThat(query.finish()).containsExactlyInAnyOrderEntriesOf(Map.of(0, 0, 1, 1, 2, 2));
    }
    verify(firstPage).fetchNextPage();
  }

  @Test
  void batchedQueryCollectsResultsFromConcurrentQueries() {
    var firstResult = resultSet(0, SELECT_BATCH_SIZE);
    var secondResult = resultSet(SELECT_BATCH_SIZE, SELECT_BATCH_SIZE + 1);
    var firstQuery = new CompletableFuture<AsyncResultSet>();
    var secondQuery = new CompletableFuture<AsyncResultSet>();
    var secondSubmission = new CompletableFuture<Void>();
    var queries = new ArrayList<>(List.of(firstQuery, secondQuery));
    var query =
        backend()
            .newBatchedQuery(
                ignored -> {
                  var next = queries.removeFirst();
                  if (queries.isEmpty()) {
                    assertThat(firstQuery).isNotDone();
                    secondSubmission.complete(null);
                  }
                  return next;
                },
                row -> row.getInt("id"),
                row -> row.getInt("id"),
                SELECT_BATCH_SIZE + 1);

    for (var id = 0; id < SELECT_BATCH_SIZE + 1; id++) {
      query.add(id);
    }

    // Complete both queries only after finish() submits the singleton remainder.
    var completion =
        secondSubmission.thenRunAsync(() -> {
          firstQuery.complete(firstResult);
          secondQuery.complete(secondResult);
        });

    assertThat(query.finish())
        .containsExactlyInAnyOrderEntriesOf(
            IntStream.range(0, SELECT_BATCH_SIZE + 1)
                .boxed()
                .collect(Collectors.toMap(id -> id, id -> id)));
    completion.join();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void batchedQueryPreservesSynchronousFailure(boolean close) {
    var query =
        backend()
            .newBatchedQuery(
                ignored -> {
                  throw new IllegalStateException("prepare failed");
                },
                row -> row,
                row -> row,
                1);
    query.add(mock(Row.class));

    assertThatIllegalStateException()
        .isThrownBy(() -> {
          if (close) {
            try (query) {
              query.finish();
            }
          } else {
            query.finish();
          }
        })
        .withMessage("prepare failed");
  }

  @ParameterizedTest
  @CsvSource({"LOCAL_QUORUM,LOCAL_SERIAL", "QUORUM,SERIAL", "ALL,SERIAL"})
  void appliesConfiguredConsistencyToStatements(
      DefaultConsistencyLevel consistency, DefaultConsistencyLevel serialConsistency) {
    var template = mock(BoundStatement.class);
    when(template.getPreparedStatement()).thenReturn(prepared);
    when(prepared.getVariableDefinitions()).thenReturn(mock(ColumnDefinitions.class));
    when(template.getValues()).thenReturn(List.of());
    when(template.codecRegistry()).thenReturn(CodecRegistry.DEFAULT);
    when(template.protocolVersion()).thenReturn(DefaultProtocolVersion.V4);
    when(prepared.boundStatementBuilder())
        .thenAnswer(ignored -> new BoundStatementBuilder(template));
    var backend = backend(consistency, serialConsistency);
    var ordinary = backend.buildIdempotentStatement("SELECT now() FROM system.local");
    var conditional = backend.buildLwtStatement("SELECT now() FROM system.local");
    var conditionalBuilder =
        backend.newLwtStatementBuilder("SELECT now() FROM system.local").build();
    assertThat(ordinary.getConsistencyLevel()).isEqualTo(consistency);
    assertThat(ordinary.isIdempotent()).isTrue();
    assertThat(conditional.getConsistencyLevel()).isEqualTo(consistency);
    assertThat(conditional.getSerialConsistencyLevel()).isEqualTo(serialConsistency);
    assertThat(conditional.isIdempotent()).isFalse();
    assertThat(conditionalBuilder.getConsistencyLevel()).isEqualTo(consistency);
    assertThat(conditionalBuilder.getSerialConsistencyLevel()).isEqualTo(serialConsistency);
  }

  private static Reference reference(String name) {
    return Reference.builder()
        .name(name)
        .createdAtMicros(123L)
        .pointer(ObjRef.objRef("test", 2L))
        .previousPointers()
        .build();
  }

  private CassandraBackend backend() {
    return backend(DefaultConsistencyLevel.LOCAL_QUORUM, DefaultConsistencyLevel.LOCAL_SERIAL);
  }

  private CassandraBackend backend(
      DefaultConsistencyLevel consistency, DefaultConsistencyLevel serialConsistency) {
    return new CassandraBackend(
        new CassandraBackendConfig(
            session,
            "polaris",
            Duration.ofSeconds(5),
            Duration.ofSeconds(3),
            consistency,
            serialConsistency,
            false));
  }

  private static AsyncResultSet resultSet(int firstId, int endId) {
    List<Row> rows = new ArrayList<>();
    for (var id = firstId; id < endId; id++) {
      Row row = mock(Row.class);
      when(row.getInt("id")).thenReturn(id);
      rows.add(row);
    }
    AsyncResultSet resultSet = mock(AsyncResultSet.class);
    when(resultSet.currentPage()).thenReturn(rows);
    return resultSet;
  }
}
