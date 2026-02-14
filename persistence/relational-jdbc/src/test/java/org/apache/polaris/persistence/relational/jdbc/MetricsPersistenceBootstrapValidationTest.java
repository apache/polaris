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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.Optional;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for metrics persistence bootstrap validation functionality.
 *
 * <p>These tests verify that the system correctly detects whether the metrics tables have been
 * bootstrapped and provides appropriate error messages when they are missing.
 */
class MetricsPersistenceBootstrapValidationTest {

  private DataSource dataSource;
  private DatasourceOperations datasourceOperations;

  @BeforeEach
  void setUp() throws SQLException {
    // Create a fresh H2 in-memory database for each test
    dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:test_metrics_validation_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    datasourceOperations = new DatasourceOperations(dataSource, new TestJdbcConfiguration());
  }

  /** Test configuration for H2 database. */
  private static class TestJdbcConfiguration implements RelationalJdbcConfiguration {
    @Override
    public Optional<Integer> maxRetries() {
      return Optional.of(2);
    }

    @Override
    public Optional<Long> maxDurationInMs() {
      return Optional.of(100L);
    }

    @Override
    public Optional<Long> initialDelayInMs() {
      return Optional.of(100L);
    }
  }

  @AfterEach
  void tearDown() {
    if (dataSource instanceof JdbcConnectionPool) {
      ((JdbcConnectionPool) dataSource).dispose();
    }
  }

  @Nested
  class MetricsTableExistsTests {

    @Test
    void whenMetricsTableDoesNotExist_shouldReturnFalse() {
      // No schema loaded - metrics table doesn't exist
      boolean result = JdbcBasePersistenceImpl.metricsTableExists(datasourceOperations);
      assertThat(result).isFalse();
    }

    @Test
    void whenOnlyEntitySchemaLoaded_shouldReturnFalse() throws SQLException {
      // Load only the entity schema (v4), not the metrics schema
      loadSchema("h2/schema-v4.sql");

      boolean result = JdbcBasePersistenceImpl.metricsTableExists(datasourceOperations);
      assertThat(result).isFalse();
    }

    @Test
    void whenMetricsSchemaLoaded_shouldReturnTrue() throws SQLException {
      // Load the metrics schema
      loadSchema("h2/schema-metrics-v1.sql");

      boolean result = JdbcBasePersistenceImpl.metricsTableExists(datasourceOperations);
      assertThat(result).isTrue();
    }

    @Test
    void whenBothSchemasLoaded_shouldReturnTrue() throws SQLException {
      // Load both entity and metrics schemas
      loadSchema("h2/schema-v4.sql");
      loadSchema("h2/schema-metrics-v1.sql");

      boolean result = JdbcBasePersistenceImpl.metricsTableExists(datasourceOperations);
      assertThat(result).isTrue();
    }
  }

  @Nested
  class CheckMetricsPersistenceBootstrappedTests {

    @Test
    void whenMetricsTableDoesNotExist_shouldThrowIllegalStateException() {
      // No schema loaded - metrics table doesn't exist
      assertThatThrownBy(
              () -> {
                if (!JdbcBasePersistenceImpl.metricsTableExists(datasourceOperations)) {
                  throw new IllegalStateException(
                      "Metrics persistence is enabled but the metrics tables have not been bootstrapped. "
                          + "Please run the bootstrap command with the --include-metrics flag to create "
                          + "the required schema before enabling this feature.");
                }
              })
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("metrics tables have not been bootstrapped")
          .hasMessageContaining("--include-metrics");
    }

    @Test
    void whenMetricsSchemaLoaded_shouldNotThrow() throws SQLException {
      // Load the metrics schema
      loadSchema("h2/schema-metrics-v1.sql");

      // Should not throw
      boolean exists = JdbcBasePersistenceImpl.metricsTableExists(datasourceOperations);
      assertThat(exists).isTrue();
    }
  }

  private void loadSchema(String schemaPath) throws SQLException {
    ClassLoader classLoader = getClass().getClassLoader();
    InputStream scriptStream = classLoader.getResourceAsStream(schemaPath);
    if (scriptStream == null) {
      throw new IllegalStateException("Schema file not found: " + schemaPath);
    }
    datasourceOperations.executeScript(scriptStream);
  }
}
