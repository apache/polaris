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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.apache.polaris.extension.persistence.relational.jdbc.models.Converter;

public class DatasourceOperations {

  private static final String CONSTRAINT_VIOLATION_SQL_CODE = "23505";

  private final DataSource datasource;

  public DatasourceOperations(DataSource datasource) {
    this.datasource = datasource;
  }

  /**
   * Execute SQL script
   *
   * @param scriptFilePath : Path of SQL script.
   * @throws SQLException : Exception while executing the script.
   */
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
                statement.executeUpdate(sql);
              } catch (SQLException e) {
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
      throw new RuntimeException(e);
    }
  }

  /**
   * Executes SELECT Query and returns the results after applying a transformer
   *
   * @param query : Query to executed
   * @param converterInstance : An instance of the type being selected, used to convert to a
   *     business entity like PolarisBaseEntity
   * @param transformer Transformation of entity class to Result class
   * @return The list of results yielded by the query
   * @param <T> : Persistence entity class
   * @param <R> : Business entity class
   * @throws SQLException : Exception during the query execution.
   */
  public <T, R> List<R> executeSelect(
      @Nonnull String query,
      @Nonnull Converter<T> converterInstance,
      @Nonnull Function<T, R> transformer)
      throws SQLException {
    ArrayList<R> results = new ArrayList<>();
    executeSelectOverStream(
        query, converterInstance, stream -> stream.map(transformer).forEach(results::add));
    return results;
  }

  /**
   * Executes SELECT Query and takes a consumer over the results. For callers that want more
   * sophisticated control over how query results are handled.
   *
   * @param query : Query to executed
   * @param converterInstance : An entity of the type being selected
   * @param consumer : An function to consume the returned results
   * @param <T> : Entity class
   * @throws SQLException : Exception during the query execution.
   */
  public <T> void executeSelectOverStream(
      @Nonnull String query,
      @Nonnull Converter<T> converterInstance,
      @Nonnull Consumer<Stream<T>> consumer)
      throws SQLException {
    try (Connection connection = borrowConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query)) {
      ResultSetIterator<T> iterator = new ResultSetIterator<>(resultSet, converterInstance);
      consumer.accept(iterator.toStream());
    } catch (SQLException e) {
      throw e;
    } catch (RuntimeException e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      } else {
        throw e;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Executes the UPDATE or INSERT Query
   *
   * @param query : query to be executed
   * @return : Number of rows modified / inserted.
   * @throws SQLException : Exception during Query Execution.
   */
  public int executeUpdate(String query) throws SQLException {
    try (Connection connection = borrowConnection();
        Statement statement = connection.createStatement()) {
      boolean autoCommit = connection.getAutoCommit();
      connection.setAutoCommit(true);
      try {
        return statement.executeUpdate(query);
      } finally {
        connection.setAutoCommit(autoCommit);
      }
    }
  }

  /**
   * Transaction callback to be executed.
   *
   * @param callback : TransactionCallback to be executed within transaction
   * @throws SQLException : Exception caught during transaction execution.
   */
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
    }
  }

  // Interface for transaction callback
  public interface TransactionCallback {
    boolean execute(Statement statement) throws SQLException;
  }

  public boolean isConstraintViolation(SQLException e) {
    return CONSTRAINT_VIOLATION_SQL_CODE.equals(e.getSQLState());
  }

  private Connection borrowConnection() throws SQLException {
    return datasource.getConnection();
  }
}
