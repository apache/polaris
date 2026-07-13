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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.core.persistence.bootstrap.BootstrapOptions;
import org.apache.polaris.core.persistence.bootstrap.ImmutableBootstrapOptions;
import org.apache.polaris.core.persistence.bootstrap.ImmutableSchemaOptions;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcSchemaManagerTest {
  private DataSource dataSource;
  private DatasourceOperations datasourceOperations;
  private JdbcSchemaManager schemaManager;

  @BeforeEach
  void setUp() {
    dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:test_schema_manager_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    datasourceOperations =
        new DatasourceOperations(
            dataSource, SimpleRelationalJdbcConfiguration.forDatabaseType(DatabaseType.H2));
    schemaManager = new JdbcSchemaManager(datasourceOperations);
  }

  @Test
  void bootstrapCoreOnlyDoesNotCreateLineageComponent() {
    Map<JdbcSchemaComponent, Integer> versions = schemaManager.bootstrap(bootstrapOptions());

    assertThat(versions).containsEntry(JdbcSchemaComponent.METASTORE, 4);
    assertThat(versions).doesNotContainKey(JdbcSchemaComponent.LINEAGE);
    assertThat(
            JdbcBasePersistenceImpl.loadSchemaVersion(
                datasourceOperations, JdbcSchemaComponent.METASTORE, false))
        .isEqualTo(4);
    assertThat(
            JdbcBasePersistenceImpl.loadSchemaVersion(
                datasourceOperations, JdbcSchemaComponent.LINEAGE, true))
        .isZero();
    assertThat(
            JdbcBasePersistenceImpl.loadSchemaVersion(
                datasourceOperations, JdbcSchemaComponent.LINEAGE, false))
        .isZero();
    assertThatLineageTableDoesNotExist();
  }

  @Test
  void bootstrapCanAddLineageV1ComponentAfterCoreV4Bootstrap() throws SQLException {
    schemaManager.bootstrap(bootstrapOptions());

    Map<JdbcSchemaComponent, Integer> versions =
        schemaManager.bootstrap(bootstrapOptionsWithLineagePersistence());

    assertCoreV4AndLineageV1(versions);
    assertThat(
            JdbcBasePersistenceImpl.loadSchemaVersion(
                datasourceOperations, JdbcSchemaComponent.METASTORE, false))
        .isEqualTo(4);
    assertThat(
            JdbcBasePersistenceImpl.loadSchemaVersion(
                datasourceOperations, JdbcSchemaComponent.LINEAGE, false))
        .isEqualTo(1);
    assertThatLineageTableExists();
  }

  @Test
  void bootstrapCanAddExplicitlyRequestedLineageV1WithCoreV4() throws SQLException {
    Map<JdbcSchemaComponent, Integer> versions =
        schemaManager.bootstrap(bootstrapOptionsWithComponentVersion("lineage", 1));

    assertCoreV4AndLineageV1(versions);
    assertThat(
            JdbcBasePersistenceImpl.loadSchemaVersion(
                datasourceOperations, JdbcSchemaComponent.METASTORE, false))
        .isEqualTo(4);
    assertThat(
            JdbcBasePersistenceImpl.loadSchemaVersion(
                datasourceOperations, JdbcSchemaComponent.LINEAGE, false))
        .isEqualTo(1);
    assertThatLineageTableExists();
  }

  @Test
  void bootstrapRejectsUnknownRequestedComponent() {
    assertThatThrownBy(
            () -> schemaManager.bootstrap(bootstrapOptionsWithComponentVersion("unknown", 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown JDBC schema component: unknown");
  }

  @Test
  void bootstrapRejectsInvalidComponentVersionBeforeExecutingAnyScripts() {
    assertThatThrownBy(
            () -> schemaManager.bootstrap(bootstrapOptionsWithComponentVersion("lineage", 2)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid lineage schema version 2");

    assertThatMetastoreTableDoesNotExist();
    assertThatLineageTableDoesNotExist();
  }

  @Test
  void bootstrapRejectsUnsupportedComponentVersionChange() throws SQLException {
    schemaManager.bootstrap(bootstrapOptionsWithLineagePersistence());
    updateVersion(JdbcSchemaComponent.LINEAGE, 2);

    assertThatThrownBy(() -> schemaManager.bootstrap(bootstrapOptionsWithLineagePersistence()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot upgrade JDBC schema component lineage from version 2");
  }

  private static BootstrapOptions bootstrapOptions() {
    return ImmutableBootstrapOptions.builder()
        .realms(List.of("TEST_REALM"))
        .rootCredentialsSet(RootCredentialsSet.EMPTY)
        .schemaOptions(ImmutableSchemaOptions.builder().build())
        .build();
  }

  private static BootstrapOptions bootstrapOptionsWithLineagePersistence() {
    return ImmutableBootstrapOptions.builder()
        .from(bootstrapOptions())
        .lineagePersistenceEnabled(true)
        .build();
  }

  private static BootstrapOptions bootstrapOptionsWithComponentVersion(
      String componentName, int version) {
    return ImmutableBootstrapOptions.builder()
        .realms(List.of("TEST_REALM"))
        .rootCredentialsSet(RootCredentialsSet.EMPTY)
        .schemaOptions(
            ImmutableSchemaOptions.builder()
                .schemaComponentVersions(Map.of(componentName, version))
                .build())
        .build();
  }

  private static void assertCoreV4AndLineageV1(Map<JdbcSchemaComponent, Integer> versions) {
    assertThat(versions)
        .containsEntry(JdbcSchemaComponent.METASTORE, 4)
        .containsEntry(JdbcSchemaComponent.LINEAGE, 1);
  }

  private void updateVersion(JdbcSchemaComponent component, int version) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement =
            connection.prepareStatement(
                "UPDATE POLARIS_SCHEMA.version SET version_value = ? WHERE version_key = ?")) {
      statement.setInt(1, version);
      statement.setString(2, component.versionKey());
      statement.executeUpdate();
    }
  }

  private void assertThatLineageTableExists() throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement =
            connection.prepareStatement("SELECT COUNT(*) FROM POLARIS_SCHEMA.lineage_datasets")) {
      assertThat(statement.executeQuery().next()).isTrue();
    }
  }

  private void assertThatLineageTableDoesNotExist() {
    assertThatThrownBy(
            () -> {
              try (Connection connection = dataSource.getConnection();
                  PreparedStatement statement =
                      connection.prepareStatement(
                          "SELECT COUNT(*) FROM POLARIS_SCHEMA.lineage_datasets")) {
                statement.executeQuery();
              }
            })
        .isInstanceOf(SQLException.class)
        .satisfies(
            throwable ->
                assertThat(datasourceOperations.isRelationDoesNotExist((SQLException) throwable))
                    .isTrue());
  }

  private void assertThatMetastoreTableDoesNotExist() {
    assertThatThrownBy(
            () -> {
              try (Connection connection = dataSource.getConnection();
                  PreparedStatement statement =
                      connection.prepareStatement("SELECT COUNT(*) FROM POLARIS_SCHEMA.version")) {
                statement.executeQuery();
              }
            })
        .isInstanceOf(SQLException.class)
        .satisfies(
            throwable ->
                assertThat(datasourceOperations.isRelationDoesNotExist((SQLException) throwable))
                    .isTrue());
  }
}
