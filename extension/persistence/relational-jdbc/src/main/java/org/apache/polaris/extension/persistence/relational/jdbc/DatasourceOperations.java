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
package org.apache.polaris.extension.persistence.relational.jdbc;

import static java.nio.charset.StandardCharsets.UTF_8;

import jakarta.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.sql.DataSource;
import javax.swing.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatasourceOperations {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatasourceOperations.class);

  private static final String ALREADY_EXISTS_STATE_POSTGRES = "42P07";
  private static final String CONSTRAINT_VIOLATION_SQL_CODE = "23505";

  private final DataSource datasource;

  public DatasourceOperations(DataSource datasource) {
    this.datasource = datasource;
  }

  public void executeScript(String scriptFilePath) throws SQLException {
    ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
    try (Connection connection = borrowConnection();
        Statement statement = connection.createStatement()) {
      boolean autoCommit = connection.getAutoCommit();
      connection.setAutoCommit(true);
      try {
        BufferedReader reader =
            new BufferedReader(
                new InputStreamReader(
                    Objects.requireNonNull(classLoader.getResourceAsStream(scriptFilePath)),
                    UTF_8));
        StringBuilder sqlBuffer = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
          line = line.trim();
          if (!line.isEmpty() && !line.startsWith("--")) { // Ignore empty lines and comments
            sqlBuffer.append(line).append("\n");
            if (line.endsWith(";")) { // Execute statement when semicolon is found
              String sql = sqlBuffer.toString().trim();
              try {
                int rowsUpdated = statement.executeUpdate(sql);
                LOGGER.debug("Query {} executed {} rows affected", sql, rowsUpdated);
              } catch (SQLException e) {
                LOGGER.error("Error executing query {}", sql, e);
                // re:throw this as unhandled exception
                throw new RuntimeException(e);
              }
              sqlBuffer.setLength(0); // Clear the buffer for the next statement
            }
          }
        }
      } finally {
        connection.setAutoCommit(autoCommit);
      }
    } catch (IOException e) {
      LOGGER.error("Error reading the script file", e);
      throw new RuntimeException(e);
    } catch (SQLException e) {
      LOGGER.error("Error executing the script file", e);
      throw e;
    }
  }

  public <T, R> List<R> executeSelect(
      @Nonnull String query,
      @Nonnull Class<T> targetClass,
      @Nonnull Function<T, R> transformer,
      Predicate<R> entityFilter,
      int limit)
      throws SQLException {
    try (Connection connection = borrowConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query)) {
      return ResultSetToObjectConverter.collect(
          resultSet, targetClass, transformer, entityFilter, limit);
    } catch (SQLException e) {
      LOGGER.error("Error executing query {}", query, e);
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public int executeUpdate(String query) throws SQLException {
    try (Connection connection = borrowConnection();
        Statement statement = connection.createStatement()) {
      boolean autoCommit = connection.getAutoCommit();
      connection.setAutoCommit(true);
      try {
        return statement.executeUpdate(query);
      } catch (SQLException e) {
        LOGGER.error("Error executing query {}", query, e);
        throw e;
      } finally {
        connection.setAutoCommit(autoCommit);
      }
    }
  }

  public void runWithinTransaction(TransactionCallback callback) throws SQLException {
    try (Connection connection = borrowConnection()) {
      boolean autoCommit = connection.getAutoCommit();
      connection.setAutoCommit(false);
      boolean success = false;
      try {
        try (Statement statement = connection.createStatement()) {
          success = callback.execute(statement);
        }
      } finally {
        if (success) {
          connection.commit();
        } else {
          connection.rollback();
        }
        connection.setAutoCommit(autoCommit);
      }
    } catch (SQLException e) {
      LOGGER.error("Caught Error while executing transaction", e);
      throw e;
    }
  }

  // Interface for transaction callback
  public interface TransactionCallback {
    boolean execute(Statement statement) throws SQLException;
  }

  public boolean isConstraintViolation(SQLException e) {
    return CONSTRAINT_VIOLATION_SQL_CODE.equals(e.getSQLState());
  }

  public boolean isAlreadyExistsException(SQLException e) {
    return ALREADY_EXISTS_STATE_POSTGRES.equals(e.getSQLState());
  }

  private Connection borrowConnection() throws SQLException {
    return datasource.getConnection();
  }
}
