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

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests against a real H2 database for the configurable schema name: the schema is
 * created programmatically at bootstrap and selected as the session schema on every borrowed
 * connection, so unqualified table references resolve against it.
 */
class DatasourceOperationsSchemaTest {

  private static DataSource createDataSource() {
    return JdbcConnectionPool.create(
        "jdbc:h2:mem:schema_test_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1", "sa", "");
  }

  private static RelationalJdbcConfiguration configWithSchema(Optional<String> schemaName) {
    return new RelationalJdbcConfiguration() {
      @Override
      public Optional<Integer> maxRetries() {
        return Optional.empty();
      }

      @Override
      public Optional<Long> maxDurationInMs() {
        return Optional.empty();
      }

      @Override
      public Optional<Long> initialDelayInMs() {
        return Optional.empty();
      }

      @Override
      public Optional<String> databaseType() {
        return Optional.of("h2");
      }

      @Override
      public Optional<String> schemaName() {
        return schemaName;
      }
    };
  }

  private static InputStream openSchemaScript() {
    return DatasourceOperations.class.getClassLoader().getResourceAsStream("h2/schema-v4.sql");
  }

  /** Schemas holding an ENTITIES table, as reported by the database catalog. */
  private static List<String> schemasContainingEntitiesTable(DataSource dataSource)
      throws SQLException {
    List<String> schemas = new ArrayList<>();
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement =
            connection.prepareStatement(
                "SELECT table_schema FROM information_schema.tables WHERE table_name = 'ENTITIES'")) {
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          schemas.add(resultSet.getString(1));
        }
      }
    }
    return schemas;
  }

  @Test
  void bootstrapsIntoConfiguredSchema() throws SQLException {
    DataSource dataSource = createDataSource();
    DatasourceOperations ops =
        new DatasourceOperations(dataSource, configWithSchema(Optional.of("custom_polaris")));

    // Before bootstrap the schema does not exist; selecting it as the session schema fails with
    // H2's "schema not found", which must be treated like a missing relation.
    assertThat(JdbcBasePersistenceImpl.entityTableExists(ops)).isFalse();

    ops.executeScript(openSchemaScript());

    // Unqualified queries resolve against the configured schema.
    assertThat(JdbcBasePersistenceImpl.loadSchemaVersion(ops, false)).isEqualTo(4);
    // H2 case-folds the unquoted identifier to uppercase.
    assertThat(schemasContainingEntitiesTable(dataSource)).containsExactly("CUSTOM_POLARIS");
  }

  @Test
  void bootstrapsIntoDefaultSchemaWhenUnset() throws SQLException {
    DataSource dataSource = createDataSource();
    DatasourceOperations ops =
        new DatasourceOperations(dataSource, configWithSchema(Optional.empty()));

    assertThat(JdbcBasePersistenceImpl.entityTableExists(ops)).isFalse();

    ops.executeScript(openSchemaScript());

    assertThat(JdbcBasePersistenceImpl.loadSchemaVersion(ops, false)).isEqualTo(4);
    assertThat(schemasContainingEntitiesTable(dataSource))
        .containsExactly(RelationalJdbcConfiguration.DEFAULT_SCHEMA_NAME);
  }
}
