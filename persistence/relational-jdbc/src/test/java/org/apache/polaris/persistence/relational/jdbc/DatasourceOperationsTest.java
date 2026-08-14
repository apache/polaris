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
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations.Operation;
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

  @Mock private RelationalJdbcConfiguration relationalJdbcConfiguration;

  @Mock private DatabaseMetaData mockDatabaseMetaData;

  @Mock Operation<String> mockOperation;

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
        "INSERT INTO POLARIS_SCHEMA.EVENTS (catalog_id, event_id, request_id, event_type, timestamp_ms, principal_name, resource_type, resource_identifier, additional_properties, realm_id) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )";
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
        SQLException.class, () -> datasourceOperations.executeSelect(query, new ModelEntity(1)));
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
        () -> datasourceOperations.executeSelect(mockConnection, query, new ModelEntity(1)));

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
                mockConnection, query, new ModelEntity(1), stream -> stream.forEach(x -> {})));

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
  void isAmbiguousCommitOutcome_classifiesConnectionAndTimeoutFailures() {
    assertTrue(datasourceOperations.isAmbiguousCommitOutcome(new SQLException("reset", "08006")));
    assertTrue(
        datasourceOperations.isAmbiguousCommitOutcome(new SQLException("canceled", "57014")));
    assertTrue(
        datasourceOperations.isAmbiguousCommitOutcome(
            new SQLException("Connection reset by peer")));
    assertTrue(
        datasourceOperations.isAmbiguousCommitOutcome(new java.sql.SQLTimeoutException("timeout")));
    // Serialization failure is a definite rollback — retryable, not ambiguous commit success.
    assertTrue(
        !datasourceOperations.isAmbiguousCommitOutcome(new SQLException("serialization", "40001")));
    // Constraint violations are definite failures.
    assertTrue(!datasourceOperations.isAmbiguousCommitOutcome(new SQLException("unique", "23505")));
  }

  @Test
  void withRetries_doesNotRetryAmbiguousConnectionFailure() throws Exception {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(3));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(5000L));
    when(relationalJdbcConfiguration.initialDelayInMs()).thenReturn(Optional.of(1L));
    when(mockOperation.execute()).thenThrow(new SQLException("Connection reset", "08006"));

    assertThrows(SQLException.class, () -> datasourceOperations.withRetries(mockOperation));
    verify(mockOperation, times(1)).execute();
  }

  @Test
  void withRetries_retriesAmbiguousConnectionFailureForReads() throws Exception {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(3));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(5000L));
    when(relationalJdbcConfiguration.initialDelayInMs()).thenReturn(Optional.of(1L));
    // A read has no commit outcome to protect, so a transient connection failure is retried.
    when(mockOperation.execute())
        .thenThrow(new SQLException("Connection reset by peer"))
        .thenThrow(new SQLException("Connection reset by peer"))
        .thenReturn("Success!");

    String result = datasourceOperations.withRetries(mockOperation, false);
    assertEquals("Success!", result);
    verify(mockOperation, times(3)).execute();
  }

  @Test
  void isAmbiguousCommitOutcome_walksCauseChain() {
    // withRetries rewraps the original SQLException in a generic SQLException on its terminal
    // throw, so the ambiguous subtype is only reachable through the cause chain.
    SQLException rewrappedTimeout =
        new SQLException(
            "Failed after retries", null, 0, new java.sql.SQLTimeoutException("statement timeout"));
    assertTrue(datasourceOperations.isAmbiguousCommitOutcome(rewrappedTimeout));

    // A definite failure in the cause chain must not be misread as ambiguous.
    SQLException rewrappedConstraint =
        new SQLException("wrapper", null, 0, new SQLException("unique violation", "23505"));
    assertTrue(!datasourceOperations.isAmbiguousCommitOutcome(rewrappedConstraint));
  }

  @Test
  void executeUpdate_retriesWhenConnectionAcquisitionFails() throws Exception {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(3));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(5000L));
    when(relationalJdbcConfiguration.initialDelayInMs()).thenReturn(Optional.of(1L));

    QueryGenerator.PreparedQuery query =
        new QueryGenerator.PreparedQuery("UPDATE users SET active = ?", List.of());
    // The connection cannot be acquired on the first two attempts, then succeeds. A write whose
    // statement never ran is a definite non-write, so it must be retried even for a mutating op.
    when(mockDataSource.getConnection())
        .thenThrow(new SQLException("Connection refused", "08001"))
        .thenThrow(new SQLException("Connection refused", "08001"))
        .thenReturn(mockConnection);
    when(mockConnection.prepareStatement(query.sql())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeUpdate()).thenReturn(1);

    int result = datasourceOperations.executeUpdate(query);

    assertEquals(1, result);
    verify(mockPreparedStatement, times(1)).executeUpdate();
  }

  @Test
  void executeUpdate_connectionAcquisitionFailureIsReportedAsDefiniteNonWrite() throws Exception {
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(1));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(1000L));

    QueryGenerator.PreparedQuery query =
        new QueryGenerator.PreparedQuery("UPDATE users SET active = ?", List.of());
    when(mockDataSource.getConnection()).thenThrow(new SQLException("Connection refused", "08001"));

    SQLException thrown =
        assertThrows(SQLException.class, () -> datasourceOperations.executeUpdate(query));

    // Even though the underlying SQLSTATE (class 08) would otherwise look ambiguous, a connection
    // that was never acquired is a definite non-write; callers rely on this to clean up orphans.
    assertTrue(datasourceOperations.isConnectionAcquisitionFailure(thrown));
    assertTrue(!datasourceOperations.isAmbiguousCommitOutcome(thrown));
  }
}
