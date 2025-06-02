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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
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
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.apache.polaris.core.persistence.EntityAlreadyExistsException;
import org.apache.polaris.persistence.relational.jdbc.models.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatasourceOperations {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasourceOperations.class);

  private static final String CONSTRAINT_VIOLATION_SQL_CODE = "23505";

  // POSTGRES RETRYABLE EXCEPTIONS
  private static final String SERIALIZATION_FAILURE_SQL_CODE = "40001";

  private final DataSource datasource;
  private final RelationalJdbcConfiguration relationalJdbcConfiguration;

  private final Random random = new Random();

  public DatasourceOperations(
      DataSource datasource, RelationalJdbcConfiguration relationalJdbcConfiguration) {
    this.datasource = datasource;
    this.relationalJdbcConfiguration = relationalJdbcConfiguration;
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
   * @return The list of results yielded by the query
   * @param <T> : Business entity class
   * @throws SQLException : Exception during the query execution.
   */
  public <T> List<T> executeSelect(@Nonnull String query, @Nonnull Converter<T> converterInstance)
      throws SQLException {
    ArrayList<T> results = new ArrayList<>();
    executeSelectOverStream(query, converterInstance, stream -> stream.forEach(results::add));
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
    withRetries(
        () -> {
          try (Connection connection = borrowConnection();
              Statement statement = connection.createStatement();
              ResultSet resultSet = statement.executeQuery(query)) {
            ResultSetIterator<T> iterator = new ResultSetIterator<>(resultSet, converterInstance);
            consumer.accept(iterator.toStream());
            return null;
          }
        });
  }

  /**
   * Executes the UPDATE or INSERT Query
   *
   * @param query : query to be executed
   * @return : Number of rows modified / inserted.
   * @throws SQLException : Exception during Query Execution.
   */
  public int executeUpdate(String query) throws SQLException {
    return withRetries(
        () -> {
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
        });
  }

  /**
   * Transaction callback to be executed.
   *
   * @param callback : TransactionCallback to be executed within transaction
   * @throws SQLException : Exception caught during transaction execution.
   */
  public void runWithinTransaction(TransactionCallback callback) throws SQLException {
    withRetries(
        () -> {
          try (Connection connection = borrowConnection()) {
            boolean autoCommit = connection.getAutoCommit();
            boolean success = false;
            connection.setAutoCommit(false);
            try {
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
              }
            } finally {
              connection.setAutoCommit(autoCommit);
            }
          }
          return null;
        });
  }

  private boolean isRetryable(SQLException e) {
    String sqlState = e.getSQLState();

    if (sqlState != null) {
      return sqlState.equals(SERIALIZATION_FAILURE_SQL_CODE); // Serialization failure
    }

    // Additionally, one might check for specific error messages or other conditions
    return e.getMessage().toLowerCase(Locale.ROOT).contains("connection refused")
        || e.getMessage().toLowerCase(Locale.ROOT).contains("connection reset");
  }

  // TODO: consider refactoring to use a retry library, inorder to have fair retries
  // and more knobs for tuning retry pattern.
  @VisibleForTesting
  <T> T withRetries(Operation<T> operation) throws SQLException {
    int attempts = 0;
    // maximum number of retries.
    int maxAttempts = relationalJdbcConfiguration.maxRetries().orElse(1);
    // How long we should try, since the first attempt.
    long maxDuration = relationalJdbcConfiguration.maxDurationInMs().orElse(5000L);
    // How long to wait before first failure.
    long delay = relationalJdbcConfiguration.initialDelayInMs().orElse(100L);

    // maximum time we will retry till.
    long maxRetryTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) + maxDuration;

    while (attempts < maxAttempts) {
      try {
        return operation.execute();
      } catch (SQLException | RuntimeException e) {
        SQLException sqlException;
        if (e instanceof RuntimeException) {
          // Handle Exceptions from ResultSet Iterator consumer, as it throws a RTE, ignore RTE from
          // the transactions.
          if (e.getCause() instanceof SQLException
              && !(e instanceof EntityAlreadyExistsException)) {
            sqlException = (SQLException) e.getCause();
          } else {
            throw e;
          }
        } else {
          sqlException = (SQLException) e;
        }

        attempts++;
        long timeLeft =
            Math.max((maxRetryTime - TimeUnit.NANOSECONDS.toMillis(System.nanoTime())), 0L);
        if (timeLeft == 0 || attempts >= maxAttempts || !isRetryable(sqlException)) {
          String exceptionMessage =
              String.format(
                  "Failed due to %s, after , %s attempts and %s milliseconds",
                  sqlException.getMessage(), attempts, maxDuration);
          throw new SQLException(
              exceptionMessage, sqlException.getSQLState(), sqlException.getErrorCode(), e);
        }
        // Add jitter
        long timeToSleep = Math.min(timeLeft, delay + (long) (random.nextFloat() * 0.2 * delay));
        LOGGER.debug(
            "Sleeping {} ms before retrying {} on attempt {} / {}, reason {}",
            timeToSleep,
            operation,
            attempts,
            maxAttempts,
            e.getMessage(),
            e);
        try {
          Thread.sleep(timeToSleep);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Retry interrupted", ie);
        }
        delay *= 2; // Exponential backoff
      }
    }
    // This should never be reached
    return null;
  }

  public interface Operation<T> {
    T execute() throws SQLException;
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
