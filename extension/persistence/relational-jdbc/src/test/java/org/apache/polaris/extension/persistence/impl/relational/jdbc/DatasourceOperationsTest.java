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
package org.apache.polaris.extension.persistence.impl.relational.jdbc;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.function.Function;
import javax.sql.DataSource;
import org.apache.polaris.extension.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.extension.persistence.relational.jdbc.ResultSetToObjectConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatasourceOperationsTest {
  @Mock private DataSource mockDataSource;

  @Mock private Connection mockConnection;

  @Mock private Statement mockStatement;

  @Mock private ResultSet mockResultSet;

  private DatasourceOperations datasourceOperations;

  @BeforeEach
  void setUp() throws Exception {
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    datasourceOperations = new DatasourceOperations(mockDataSource);
  }

  @Test
  void testExecuteUpdate_success() throws Exception {
    String query = "UPDATE users SET active = true";
    when(mockStatement.executeUpdate(query)).thenReturn(1);

    int result = datasourceOperations.executeUpdate(query);

    assertEquals(1, result);
    verify(mockStatement).executeUpdate(query);
  }

  @Test
  void testExecuteUpdate_failure() throws Exception {
    String query = "INVALID SQL";
    when(mockStatement.executeUpdate(query)).thenThrow(new SQLException("demo", "42P07"));

    assertThrows(SQLException.class, () -> datasourceOperations.executeUpdate(query));
  }

  @Test
  void testExecuteSelect_success() throws Exception {
    String query = "SELECT * FROM users";
    when(mockStatement.executeQuery(query)).thenReturn(mockResultSet);

    // Simulate ResultSetToObjectConverter returning dummy data
    List<Object> dummyResult = List.of(new Object());
    try (MockedStatic<ResultSetToObjectConverter> mockedConverter =
        mockStatic(ResultSetToObjectConverter.class)) {
      mockedConverter
          .when(
              () ->
                  ResultSetToObjectConverter.collect(
                      mockResultSet, Object.class, Function.identity(), null, Integer.MAX_VALUE))
          .thenReturn(dummyResult);

      List<Object> result =
          datasourceOperations.executeSelect(
              query, Object.class, Function.identity(), null, Integer.MAX_VALUE);
      assertNotNull(result);
      assertEquals(1, result.size());
    }
  }

  @Test
  void testExecuteSelect_exception() throws Exception {
    String query = "SELECT * FROM users";
    when(mockStatement.executeQuery(query)).thenThrow(new SQLException());

    assertThrows(
        SQLException.class,
        () ->
            datasourceOperations.executeSelect(
                query, Object.class, Function.identity(), null, Integer.MAX_VALUE));
  }

  @Test
  void testRunWithinTransaction_commit() throws Exception {
    DatasourceOperations.TransactionCallback callback = statement -> true;

    datasourceOperations.runWithinTransaction(callback);

    verify(mockConnection).setAutoCommit(false);
    verify(mockConnection).commit();
    verify(mockConnection).setAutoCommit(true);
    verify(mockConnection).close();
  }

  @Test
  void testRunWithinTransaction_rollback() throws Exception {
    DatasourceOperations.TransactionCallback callback = statement -> false;

    datasourceOperations.runWithinTransaction(callback);

    verify(mockConnection).rollback();
  }

  @Test
  void testRunWithinTransaction_exceptionTriggersRollback() throws Exception {
    DatasourceOperations.TransactionCallback callback =
        statement -> {
          throw new SQLException("Boom");
        };

    assertThrows(SQLException.class, () -> datasourceOperations.runWithinTransaction(callback));

    verify(mockConnection).rollback();
  }
}
