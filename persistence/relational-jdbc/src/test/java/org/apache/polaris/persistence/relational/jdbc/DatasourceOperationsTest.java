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
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations.Operation;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEntity;
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
    when(relationalJdbcConfiguration.maxRetries()).thenReturn(Optional.of(4));
    when(relationalJdbcConfiguration.maxDurationInMs()).thenReturn(Optional.of(2000L));
    when(relationalJdbcConfiguration.initialDelayInMs()).thenReturn(Optional.of(0L));
    when(mockOperation.execute())
        .thenThrow(new SQLException("Retryable error", "40001"))
        .thenThrow(new SQLException("connection refused"))
        .thenThrow(new SQLException("connection reset"))
        .thenReturn("Success!");

    String result = datasourceOperations.withRetries(mockOperation);
    assertEquals("Success!", result);
    verify(mockOperation, times(4)).execute();
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
        "Failed due to Retryable error, after , 2 attempts and 1000 milliseconds",
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
        "Failed due to NonRetryable error, after , 1 attempts and 1000 milliseconds",
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
}
