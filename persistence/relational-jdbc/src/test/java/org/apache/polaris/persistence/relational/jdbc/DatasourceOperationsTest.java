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
package org.apache.polaris.persistence.relational.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations.Operation;
import org.apache.polaris.persistence.relational.jdbc.models.Converter;
import org.apache.polaris.persistence.relational.jdbc.models.ImmutableModelEvent;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEntity;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatasourceOperationsTest {
  @Mock private DataSource mockDataSource;

  @Mock private Connection mockConnection;

  @Mock private PreparedStatement mockPreparedStatement;

  @Mock private ResultSet mockResultSet;

  @Mock private RelationalJdbcConfiguration relationalJdbcConfiguration;

  @Mock private DatabaseMetaData mockDatabaseMetaData;

  @Mock Operation<String> mockOperation;

  @Mock Converter<String> mockConverter;

  private DatasourceOperations datasourceOperations;

  @BeforeEach
  void setUp() throws SQLException {
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.getMetaData()).thenReturn(mockDatabaseMetaData);
    when(mockDatabaseMetaData.getDatabaseProductName()).thenReturn("h2");
    datasourceOperations = new DatasourceOperations(mockDataSource, relationalJdbcConfiguration);
  }

  @Test
  void testExecuteUpdate_success() throws Exception {
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    QueryGenerator.PreparedQuery query =
        new QueryGenerator.PreparedQuery("UPDATE users SET active = ?", List.of());
    when(mockConnection.prepareStatement(query.sql())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeUpdate()).thenReturn(1);

    int result = datasourceOperations.executeUpdate(query);

    assertEquals(1, result);
  }

  @Test
  void testExecuteUpdate_failure() throws Exception {
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    QueryGenerator.PreparedQuery query = new QueryGenerator.PreparedQuery("INVALID SQL", List.of());
    when(mockConnection.prepareStatement(query.sql())).thenReturn(mockPreparedStatement);

    when(mockPreparedStatement.executeUpdate()).thenThrow(new SQLException("demo", "42P07"));

    assertThrows(SQLException.class, () -> datasourceOperations.executeUpdate(query));
  }

  @Test
  void executeBatchUpdate_success() throws Exception {
    // There is no way to track how many statements are in a batch, so we are testing how many times
    // `executeBatch` is being called
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    String sql =
        "INSERT INTO EVENTS (catalog_id, event_id, request_id, event_type, timestamp_ms, principal_name, resource_type, resource_identifier, additional_properties, realm_id) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )";
    List<List<Object>> queryParams = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      ModelEvent modelEvent =
          ImmutableModelEvent.builder()
              .resourceType(EventEntity.ResourceType.CATALOG)
              .resourceIdentifier("catalog_" + i)
              .catalogId(i % 2 == 0 ? "catalog_" + i : null)
              .eventId("event_" + i)
              .requestId("request_" + i)
              .eventType("event_type1")
              .timestampMs(1234)
              .principalName("principal_" + i)
              .additionalProperties("")
              .build();
      queryParams.add(
          modelEvent.toMap(datasourceOperations.getDatabaseType()).values().stream().toList());
    }
    when(mockConnection.prepareStatement(any())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeBatch()).thenReturn(new int[] {100});

    int result =
        datasourceOperations.executeBatchUpdate(
            new QueryGenerator.PreparedBatchQuery(sql, queryParams));
    assertEquals(
        queryParams.size() + 100,
        result); // ExecuteBatch will be called once more than actual batches
  }

  @Test
  void testExecuteSelect_exception() throws Exception {
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    QueryGenerator.PreparedQuery query =
        new QueryGenerator.PreparedQuery("SELECT * FROM users", List.of());
    when(mockConnection.prepareStatement(query.sql())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenThrow(new SQLException("demo", "42P07"));

    assertThrows(
        SQLException.class, () -> datasourceOperations.executeSelect(query, new ModelEntity()));
  }

  @Test
  void testExecuteSelectRetryReplacesPartialResults() throws Exception {
    QueryGenerator.PreparedQuery query =
        new QueryGenerator.PreparedQuery("SELECT * FROM users", List.of());
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(2));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(1000L));
    when(relationalJdbcConfiguration.initialDelayInMs()).thenReturn(Optional.of(0L));
    when(mockConnection.prepareStatement(query.sql())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next())
        .thenReturn(true, true)
        .thenThrow(new SQLException("Retryable error", "40001"))
        .thenReturn(true, true, false);
    when(mockConverter.fromResultSet(mockResultSet))
        .thenReturn("first", "second", "third", "fourth");

    assertEquals(
        List.of("third", "fourth"), datasourceOperations.executeSelect(query, mockConverter));
  }

  @Test
  void testExecuteSelectWithConnectionRetryReplacesPartialResults() throws Exception {
    QueryGenerator.PreparedQuery query =
        new QueryGenerator.PreparedQuery("SELECT * FROM users", List.of());
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(2));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(1000L));
    when(relationalJdbcConfiguration.initialDelayInMs()).thenReturn(Optional.of(0L));
    when(mockConnection.prepareStatement(query.sql())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next())
        .thenReturn(true, true)
        .thenThrow(new SQLException("Retryable error", "40001"))
        .thenReturn(true, true, false);
    when(mockConverter.fromResultSet(mockResultSet))
        .thenReturn("first", "second", "third", "fourth");

    assertEquals(
        List.of("third", "fourth"),
        datasourceOperations.executeSelect(mockConnection, query, mockConverter));
  }

  @Test
  void testExecuteSelect_withConnection_usesProvidedConnection() throws Exception {
    QueryGenerator.PreparedQuery query =
        new QueryGenerator.PreparedQuery("SELECT * FROM users", List.of());
    when(mockConnection.prepareStatement(query.sql())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenThrow(new SQLException("demo", "42P07"));

    // The connection-aware overload must use the caller-provided Connection
    // (no internal borrow should occur for the SELECT itself).
    assertThrows(
        SQLException.class,
        () -> datasourceOperations.executeSelect(mockConnection, query, new ModelEntity()));

    verify(mockConnection).prepareStatement(query.sql());
  }

  @Test
  void testExecuteSelectOverStream_withConnection_usesProvidedConnection() throws Exception {
    QueryGenerator.PreparedQuery query =
        new QueryGenerator.PreparedQuery("SELECT * FROM users", List.of());
    when(mockConnection.prepareStatement(query.sql())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenThrow(new SQLException("stream demo", "42P07"));

    assertThrows(
        SQLException.class,
        () ->
            datasourceOperations.executeSelectOverStream(
                mockConnection, query, new ModelEntity(), stream -> stream.forEach(x -> {})));

    verify(mockConnection).prepareStatement(query.sql());
  }

  @Test
  void testRunWithinTransaction_commit() throws Exception {
    // reset to ignore constructor interaction
    reset(mockConnection);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    DatasourceOperations.TransactionCallback callback = connection -> true;
    when(mockConnection.getAutoCommit()).thenReturn(true);
    datasourceOperations.runWithinTransaction(callback);
    verify(mockConnection).setAutoCommit(true);
    verify(mockConnection).setAutoCommit(false);
    verify(mockConnection).commit();
    verify(mockConnection).setAutoCommit(true);
    verify(mockConnection).close();
  }

  @Test
  void testRunWithinTransaction_rollback() throws Exception {
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    DatasourceOperations.TransactionCallback callback = connection -> false;

    datasourceOperations.runWithinTransaction(callback);

    verify(mockConnection).rollback();
  }

  @Test
  void testRunWithinTransaction_exceptionTriggersRollback() throws Exception {
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    DatasourceOperations.TransactionCallback callback =
        connection -> {
          throw new SQLException("Boom");
        };

    assertThrows(SQLException.class, () -> datasourceOperations.runWithinTransaction(callback));

    verify(mockConnection).rollback();
  }

  @Test
  void testSuccessfulExecutionOnFirstAttempt() throws SQLException {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(3));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(1000L));
    when(relationalJdbcConfiguration.initialDelayInMs()).thenReturn(Optional.of(100L));
    when(mockOperation.execute()).thenReturn("Success!");

    String result = datasourceOperations.withRetries(mockOperation);
    assertEquals("Success!", result);
    verify(mockOperation, times(1)).execute();
  }

  @Test
  void testSuccessfulExecutionAfterOneRetry() throws SQLException {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(3));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(2000L));
    when(relationalJdbcConfiguration.initialDelayInMs()).thenReturn(Optional.of(0L));
    // Only serialization failures (definite rollback) are retryable; connection-class errors are
    // treated as ambiguous commit outcomes and must not be retried.
    when(mockOperation.execute())
        .thenThrow(new SQLException("Retryable error", "40001"))
        .thenThrow(new SQLException("Retryable error", "40001"))
        .thenReturn("Success!");

    String result = datasourceOperations.withRetries(mockOperation);
    assertEquals("Success!", result);
    verify(mockOperation, times(3)).execute();
  }

  @Test
  void testRetryAttemptsExceedMaxRetries() throws SQLException {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(2));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(1000L));
    when(relationalJdbcConfiguration.initialDelayInMs()).thenReturn(Optional.of(100L));
    when(mockOperation.execute())
        .thenThrow(
            new SQLException("Retryable error", "40001", new SQLException("Retryable error")));

    SQLException thrown =
        assertThrows(SQLException.class, () -> datasourceOperations.withRetries(mockOperation));
    assertEquals(
        "Failed due to 'Retryable error' (error code 0, sql-state '40001'), after 2 attempts and 1000 milliseconds",
        thrown.getMessage());
    verify(mockOperation, times(2)).execute(); // Tried twice, then threw
  }

  @Test
  void testRetryAttemptsExceedMaxDuration() throws SQLException {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(10));
    when(relationalJdbcConfiguration.maxDurationInMs())
        .thenReturn(Optional.of(250L)); // Short max duration
    when(mockOperation.execute())
        .thenThrow(
            new SQLException("Demo Exception", "40001", new SQLException("Retryable error")));

    long startTime = Instant.now().toEpochMilli();
    assertThrows(SQLException.class, () -> datasourceOperations.withRetries(mockOperation));
    assertTrue((Instant.now().toEpochMilli() - startTime) >= 250);
    // The number of executions depends on the timing and jitter, but should be more than 1
    verify(mockOperation, atLeast(2)).execute();
  }

  @Test
  void testNonRetryableSQLException() throws SQLException {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(3));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(1000L));
    when(relationalJdbcConfiguration.initialDelayInMs()).thenReturn(Optional.of(100L));
    when(mockOperation.execute()).thenThrow(new SQLException("NonRetryable error"));

    SQLException thrown =
        assertThrows(SQLException.class, () -> datasourceOperations.withRetries(mockOperation));
    assertEquals(
        "Failed due to 'NonRetryable error' (error code 0, sql-state 'null'), after 1 attempts and 1000 milliseconds",
        thrown.getMessage());
    verify(mockOperation, times(1)).execute(); // Should not retry
  }

  @Test
  void testInterruptedExceptionDuringRetry() throws SQLException, InterruptedException {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(3));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(1000L));
    when(relationalJdbcConfiguration.initialDelayInMs()).thenReturn(Optional.of(100L));
    when(mockOperation.execute())
        .thenThrow(
            new SQLException("Demo Exception", "40001", new SQLException("Retryable error")));

    Thread.currentThread().interrupt(); // Simulate interruption

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> datasourceOperations.withRetries(mockOperation));
    assertEquals("Retry interrupted", thrown.getMessage());
    assertTrue(Thread.currentThread().isInterrupted());
    verify(mockOperation, atMost(1))
        .execute(); // Might not even be called if interrupted very early
  }

  @Test
  void testDefaultConfigurationValues() throws SQLException {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.empty()); // Defaults to 1
    when(relationalJdbcConfiguration.maxDurationInMs())
        .thenReturn(Optional.empty()); // Defaults to 100
    when(relationalJdbcConfiguration.initialDelayInMs())
        .thenReturn(Optional.empty()); // Defaults to 100
    when(mockOperation.execute())
        .thenThrow(
            new SQLException("Demo Exception", "40001", new SQLException("Retryable error")));

    assertThrows(SQLException.class, () -> datasourceOperations.withRetries(mockOperation));
    verify(mockOperation, times(1)).execute();
  }

  @Test
  void isAmbiguousCommitOutcome_matchesOnlyTheCommitMarker() {
    // A connection or timeout error is not ambiguous until classifyByPhase marks a commit()
    // failure. The marker is what higher layers see, including after withRetries wraps it.
    SQLException connectionFailure = new SQLException("reset", "08006");
    assertTrue(!datasourceOperations.isAmbiguousCommitOutcome(connectionFailure));
    assertTrue(
        !datasourceOperations.isAmbiguousCommitOutcome(new SQLException("canceled", "57014")));
    assertTrue(!datasourceOperations.isAmbiguousCommitOutcome(new SQLTimeoutException("timeout")));
    assertTrue(!datasourceOperations.isAmbiguousCommitOutcome(new SQLException("unique", "23505")));
    assertTrue(
        datasourceOperations.isAmbiguousCommitOutcome(
            new AmbiguousCommitOutcomeException(connectionFailure)));
  }

  @Test
  void withRetries_doesNotRetryAmbiguousConnectionFailure() throws Exception {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(3));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(5000L));
    when(relationalJdbcConfiguration.initialDelayInMs()).thenReturn(Optional.of(1L));
    when(mockOperation.execute())
        .thenThrow(
            new AmbiguousCommitOutcomeException(new SQLException("Connection reset", "08006")));

    assertThrows(SQLException.class, () -> datasourceOperations.withRetries(mockOperation));
    verify(mockOperation, times(1)).execute();
  }

  @Test
  void withRetries_retriesUnmarkedConnectionFailure() throws Exception {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(3));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(5000L));
    when(relationalJdbcConfiguration.initialDelayInMs()).thenReturn(Optional.of(1L));
    // A connection failure that was never marked as an ambiguous commit outcome (it happened
    // before commit()) is a definite non-write and stays retryable.
    when(mockOperation.execute())
        .thenThrow(new SQLException("Connection reset by peer"))
        .thenThrow(new SQLException("Connection reset by peer"))
        .thenReturn("Success!");

    String result = datasourceOperations.withRetries(mockOperation);
    assertEquals("Success!", result);
    verify(mockOperation, times(3)).execute();
  }

  @Test
  void isAmbiguousCommitOutcome_walksCauseChain() {
    // withRetries rewraps the terminal failure in a plain SQLException, so the marker is only
    // reachable through the cause chain.
    SQLException rewrapped =
        new SQLException(
            "Failed after retries",
            "08006",
            0,
            new AmbiguousCommitOutcomeException(new SQLException("connection reset", "08006")));
    assertTrue(datasourceOperations.isAmbiguousCommitOutcome(rewrapped));

    // A connection signal that was never marked, even on a cause, is a definite failure.
    SQLException unmarkedConnection =
        new SQLException("wrapper", null, 0, new SQLException("dropped", "08006"));
    assertTrue(!datasourceOperations.isAmbiguousCommitOutcome(unmarkedConnection));
  }

  @Test
  void executeUpdate_doesNotRetryWhenConnectionAcquisitionFails() throws Exception {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(3));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(5000L));
    when(relationalJdbcConfiguration.initialDelayInMs()).thenReturn(Optional.of(1L));

    QueryGenerator.PreparedQuery query =
        new QueryGenerator.PreparedQuery("UPDATE users SET active = ?", List.of());
    // The connection cannot be acquired. Nothing was committed, but a SQLSTATE is set, so this is
    // not retried; it is also not an ambiguous commit outcome.
    when(mockDataSource.getConnection()).thenThrow(new SQLException("Connection refused", "08001"));

    SQLException thrown =
        assertThrows(SQLException.class, () -> datasourceOperations.executeUpdate(query));

    // Constructor borrows one connection; the update itself must not try again.
    assertTrue(!datasourceOperations.isAmbiguousCommitOutcome(thrown));
    verify(mockDataSource, times(2)).getConnection();
  }

  @Test
  void executeUpdate_prepareStatementFailureIsDefiniteNonWrite() throws Exception {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(1));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(1000L));

    QueryGenerator.PreparedQuery query =
        new QueryGenerator.PreparedQuery("UPDATE users SET active = ?", List.of());
    // The connection is acquired, but a pooled/stale connection reports SQLSTATE 08003 ("connection
    // does not exist") from prepareStatement, before any DML is sent. This is a definite non-write
    // even though 08003 is in the connection-exception class that otherwise looks ambiguous.
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(query.sql()))
        .thenThrow(new SQLException("connection does not exist", "08003"));

    SQLException thrown =
        assertThrows(SQLException.class, () -> datasourceOperations.executeUpdate(query));

    assertTrue(!datasourceOperations.isAmbiguousCommitOutcome(thrown));
    verify(mockPreparedStatement, times(0)).executeUpdate();
  }

  @Test
  void executeUpdate_connectionDropBeforeCommitIsDefiniteNonWrite() throws Exception {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(1));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(1000L));

    QueryGenerator.PreparedQuery query =
        new QueryGenerator.PreparedQuery("UPDATE users SET active = ?", List.of());
    // executeUpdate now runs under an explicit transaction; the connection drops (SQLSTATE 08006)
    // during the DML, before commit. Nothing was committed, so this is a definite non-write, not an
    // ambiguous outcome, even though 08006 is a connection-class state.
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(query.sql())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeUpdate())
        .thenThrow(new SQLException("connection reset", "08006"));

    SQLException thrown =
        assertThrows(SQLException.class, () -> datasourceOperations.executeUpdate(query));

    assertTrue(!datasourceOperations.isAmbiguousCommitOutcome(thrown));
  }

  @Test
  void executeUpdate_deterministicFailureBeforeCommitIsNeitherAmbiguousNorNonWrite()
      throws Exception {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(1));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(1000L));

    QueryGenerator.PreparedQuery query =
        new QueryGenerator.PreparedQuery("UPDATE users SET active = ?", List.of());
    // A deterministic failure (unique violation, SQLSTATE 23505) before commit keeps its original
    // type: it is not ambiguous, and it is not tagged as a not-started transient failure, so
    // higher-level mapping (e.g. already-exists) and non-retryability are preserved.
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(query.sql())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeUpdate())
        .thenThrow(new SQLException("unique violation", "23505"));

    SQLException thrown =
        assertThrows(SQLException.class, () -> datasourceOperations.executeUpdate(query));

    assertTrue(!datasourceOperations.isAmbiguousCommitOutcome(thrown));
  }

  @Test
  void executeUpdate_commitFailureIsAmbiguous() throws Exception {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(1));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(1000L));

    QueryGenerator.PreparedQuery query =
        new QueryGenerator.PreparedQuery("UPDATE users SET active = ?", List.of());
    // The DML runs, then commit() drops the connection (SQLSTATE 08006). The write may or may not
    // have been applied, so the outcome is ambiguous and must not be reported as a non-write.
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(query.sql())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeUpdate()).thenReturn(1);
    doThrow(new SQLException("connection reset", "08006")).when(mockConnection).commit();

    SQLException thrown =
        assertThrows(SQLException.class, () -> datasourceOperations.executeUpdate(query));

    assertTrue(datasourceOperations.isAmbiguousCommitOutcome(thrown));
  }

  @Test
  void executeUpdate_rollbackFailureDoesNotMaskOriginalFailure() throws Exception {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(1));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(1000L));

    QueryGenerator.PreparedQuery query =
        new QueryGenerator.PreparedQuery("UPDATE users SET active = ?", List.of());
    // The DML fails deterministically (unique violation, 23505) before commit, and the follow-up
    // rollback also fails with a connection-class error (08006). The original deterministic failure
    // must survive: if the rollback error replaced it, the failure would be misclassified as an
    // ambiguous/not-started outcome and higher layers would make the wrong cleanup decision.
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(query.sql())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeUpdate())
        .thenThrow(new SQLException("unique violation", "23505"));
    doThrow(new SQLException("connection reset", "08006")).when(mockConnection).rollback();

    SQLException thrown =
        assertThrows(SQLException.class, () -> datasourceOperations.executeUpdate(query));

    assertTrue(!datasourceOperations.isAmbiguousCommitOutcome(thrown));
  }
}
