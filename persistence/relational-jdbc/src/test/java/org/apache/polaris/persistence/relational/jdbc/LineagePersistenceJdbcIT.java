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

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.lineage.LineageColumnEdgeRecord;
import org.apache.polaris.core.persistence.lineage.LineageDatasetRecord;
import org.apache.polaris.core.persistence.lineage.LineageEdgeRecord;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.test.commons.CockroachRelationalJdbcLifeCycleManagement;
import org.apache.polaris.test.commons.PostgresRelationalJdbcLifeCycleManagement;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.cockroachdb.CockroachContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** JDBC integration tests for OpenLineage graph persistence on non-H2 databases. */
@Testcontainers
class LineagePersistenceJdbcIT {
  private static final String POSTGRES_REALM_ID = "POSTGRES_REALM";
  private static final String COCKROACH_REALM_ID = "COCKROACH_REALM";

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>(
          containerSpecHelper("postgres", PostgresRelationalJdbcLifeCycleManagement.class)
              .dockerImageName(null)
              .asCompatibleSubstituteFor("postgres"));

  @Container
  private static final CockroachContainer COCKROACH =
      new CockroachContainer(
          containerSpecHelper("cockroachdb", CockroachRelationalJdbcLifeCycleManagement.class)
              .dockerImageName(null)
              .asCompatibleSubstituteFor("cockroachdb/cockroach"));

  @Test
  void postgresLineageUpsertsInsertAndUpdate() throws Exception {
    runLineageCrudSmoke(
        dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
        new TestJdbcConfiguration(Optional.empty()),
        "postgres/schema-v5.sql",
        POSTGRES_REALM_ID);
  }

  @Test
  void cockroachLineageUpsertsInsertAndUpdate() throws Exception {
    runLineageCrudSmoke(
        dataSource(COCKROACH.getJdbcUrl(), COCKROACH.getUsername(), COCKROACH.getPassword()),
        new TestJdbcConfiguration(Optional.of(DatabaseType.COCKROACHDB.getDisplayName())),
        "cockroachdb/schema-v5.sql",
        COCKROACH_REALM_ID);
  }

  private static void runLineageCrudSmoke(
      DataSource dataSource,
      RelationalJdbcConfiguration config,
      String schemaResource,
      String realmId)
      throws Exception {
    DatasourceOperations datasourceOperations = new DatasourceOperations(dataSource, config);
    ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
    try (InputStream schemaStream = classLoader.getResourceAsStream(schemaResource)) {
      datasourceOperations.executeScript(schemaStream);
    }
    assertEquals(5, JdbcBasePersistenceImpl.loadSchemaVersion(datasourceOperations, false));

    JdbcBasePersistenceImpl lineagePersistence =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            PrincipalSecretsGenerator.RANDOM_SECRETS,
            storageProvider(),
            realmId,
            5);

    lineagePersistence.upsertLineageDataset(
        LineageDatasetRecord.builder()
            .datasetId(10L)
            .catalog("polaris")
            .namespace("analytics")
            .name("orders")
            .createdAt(100L)
            .updatedAt(100L)
            .build());
    lineagePersistence.upsertLineageDataset(
        LineageDatasetRecord.builder()
            .datasetId(999L)
            .catalog("polaris-prod")
            .namespace("analytics")
            .name("orders")
            .polarisEntityId(42L)
            .createdAt(200L)
            .updatedAt(250L)
            .build());
    assertSingleLong(
        dataSource,
        "SELECT COUNT(*) FROM POLARIS_SCHEMA.lineage_datasets WHERE realm_id = ?",
        realmId,
        1L);
    assertSingleLong(
        dataSource,
        "SELECT dataset_id FROM POLARIS_SCHEMA.lineage_datasets WHERE realm_id = ?",
        realmId,
        10L);
    assertSingleLong(
        dataSource,
        "SELECT updated_at FROM POLARIS_SCHEMA.lineage_datasets WHERE realm_id = ?",
        realmId,
        250L);
    lineagePersistence.upsertLineageDataset(
        LineageDatasetRecord.builder()
            .datasetId(20L)
            .catalog("polaris")
            .namespace("analytics")
            .name("orders_daily")
            .createdAt(100L)
            .updatedAt(100L)
            .build());

    lineagePersistence.upsertLineageEdge(
        LineageEdgeRecord.builder()
            .sourceDatasetId(10L)
            .targetDatasetId(20L)
            .lastEventAt(200L)
            .build());
    lineagePersistence.upsertLineageEdge(
        LineageEdgeRecord.builder()
            .sourceDatasetId(10L)
            .targetDatasetId(20L)
            .lastEventAt(100L)
            .build());
    assertSingleLong(
        dataSource,
        "SELECT last_event_at FROM POLARIS_SCHEMA.lineage_edges WHERE realm_id = ?",
        realmId,
        200L);

    lineagePersistence.upsertLineageColumnEdge(
        LineageColumnEdgeRecord.builder()
            .sourceDatasetId(10L)
            .sourceField("price")
            .targetDatasetId(20L)
            .targetField("total")
            .lastEventAt(300L)
            .build());
    lineagePersistence.upsertLineageColumnEdge(
        LineageColumnEdgeRecord.builder()
            .sourceDatasetId(10L)
            .sourceField("price")
            .targetDatasetId(20L)
            .targetField("total")
            .lastEventAt(100L)
            .build());
    assertSingleLong(
        dataSource,
        "SELECT last_event_at FROM POLARIS_SCHEMA.lineage_column_edges WHERE realm_id = ?",
        realmId,
        300L);
  }

  private static DataSource dataSource(String jdbcUrl, String username, String password) {
    PGSimpleDataSource dataSource = new PGSimpleDataSource();
    dataSource.setURL(jdbcUrl);
    dataSource.setUser(username);
    dataSource.setPassword(password);
    return dataSource;
  }

  private static void assertSingleLong(
      DataSource dataSource, String sql, String realmId, long expected) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, realmId);
      try (ResultSet rs = statement.executeQuery()) {
        rs.next();
        assertEquals(expected, rs.getLong(1));
      }
    }
  }

  private static PolarisStorageIntegrationProvider storageProvider() {
    return new PolarisStorageIntegrationProvider() {
      @Override
      public <T extends PolarisStorageConfigurationInfo>
          PolarisStorageIntegration<T> getStorageIntegrationForConfig(
              PolarisStorageConfigurationInfo config) {
        return null;
      }
    };
  }

  private static class TestJdbcConfiguration implements RelationalJdbcConfiguration {
    private final Optional<String> databaseType;

    private TestJdbcConfiguration(Optional<String> databaseType) {
      this.databaseType = databaseType;
    }

    @Override
    public Optional<Integer> maxRetries() {
      return Optional.of(3);
    }

    @Override
    public Optional<Long> maxDurationInMs() {
      return Optional.of(5000L);
    }

    @Override
    public Optional<Long> initialDelayInMs() {
      return Optional.of(100L);
    }

    @Override
    public Optional<String> databaseType() {
      return databaseType;
    }
  }
}
