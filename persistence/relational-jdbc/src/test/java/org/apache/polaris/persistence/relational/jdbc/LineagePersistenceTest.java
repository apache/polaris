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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.lineage.LineageColumnEdgeRecord;
import org.apache.polaris.core.persistence.lineage.LineageDatasetRecord;
import org.apache.polaris.core.persistence.lineage.LineageEdgeRecord;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** JDBC tests for OpenLineage graph persistence. */
class LineagePersistenceTest {
  private static final String REALM_ID = "TEST_REALM";

  private JdbcBasePersistenceImpl lineagePersistence;
  private DataSource dataSource;

  @BeforeEach
  void setUp() throws SQLException {
    dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:test_lineage_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1", "sa", "");

    DatasourceOperations datasourceOperations =
        new DatasourceOperations(dataSource, new TestJdbcConfiguration());

    ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
    InputStream schemaStream = classLoader.getResourceAsStream("h2/schema-v5.sql");
    datasourceOperations.executeScript(schemaStream);

    PolarisDiagnostics diagnostics = new PolarisDefaultDiagServiceImpl();
    lineagePersistence =
        new JdbcBasePersistenceImpl(
            diagnostics,
            datasourceOperations,
            PrincipalSecretsGenerator.RANDOM_SECRETS,
            storageProvider(),
            REALM_ID,
            5);
  }

  @Test
  void upsertLineageDatasetInsertsAndUpdatesByOpenLineageIdentity() throws SQLException {
    LineageDatasetRecord first =
        LineageDatasetRecord.builder()
            .datasetId(10L)
            .catalog("polaris")
            .namespace("analytics")
            .name("orders")
            .createdAt(100L)
            .updatedAt(100L)
            .build();
    LineageDatasetRecord second =
        LineageDatasetRecord.builder()
            .datasetId(999L)
            .catalog("polaris-prod")
            .namespace("analytics")
            .name("orders")
            .polarisEntityId(42L)
            .createdAt(200L)
            .updatedAt(250L)
            .build();

    lineagePersistence.upsertLineageDataset(first);
    lineagePersistence.upsertLineageDataset(second);

    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement =
            connection.prepareStatement(
                "SELECT dataset_id, catalog, polaris_entity_id, created_at, updated_at "
                    + "FROM POLARIS_SCHEMA.lineage_datasets WHERE realm_id = ?")) {
      statement.setString(1, REALM_ID);
      try (ResultSet rs = statement.executeQuery()) {
        rs.next();
        assertEquals(10L, rs.getLong("dataset_id"));
        assertEquals("polaris-prod", rs.getString("catalog"));
        assertEquals(42L, rs.getLong("polaris_entity_id"));
        assertEquals(100L, rs.getLong("created_at"));
        assertEquals(250L, rs.getLong("updated_at"));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  void upsertLineageEdgeUpdatesLastEventAt() throws SQLException {
    upsertSourceAndTargetDatasets();

    lineagePersistence.upsertLineageEdge(
        LineageEdgeRecord.builder()
            .sourceDatasetId(10L)
            .targetDatasetId(20L)
            .lastEventAt(100L)
            .build());
    lineagePersistence.upsertLineageEdge(
        LineageEdgeRecord.builder()
            .sourceDatasetId(10L)
            .targetDatasetId(20L)
            .lastEventAt(200L)
            .build());

    assertSingleLong(
        "SELECT last_event_at FROM POLARIS_SCHEMA.lineage_edges WHERE realm_id = ?", 200L);
  }

  @Test
  void upsertLineageEdgeDoesNotMoveLastEventAtBackward() throws SQLException {
    upsertSourceAndTargetDatasets();

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
        "SELECT last_event_at FROM POLARIS_SCHEMA.lineage_edges WHERE realm_id = ?", 200L);
  }

  @Test
  void upsertLineageColumnEdgeUpdatesLastEventAt() throws SQLException {
    upsertSourceAndTargetDatasets();

    lineagePersistence.upsertLineageColumnEdge(
        LineageColumnEdgeRecord.builder()
            .sourceDatasetId(10L)
            .sourceField("price")
            .targetDatasetId(20L)
            .targetField("total")
            .lastEventAt(100L)
            .build());
    lineagePersistence.upsertLineageColumnEdge(
        LineageColumnEdgeRecord.builder()
            .sourceDatasetId(10L)
            .sourceField("price")
            .targetDatasetId(20L)
            .targetField("total")
            .lastEventAt(300L)
            .build());

    assertSingleLong(
        "SELECT last_event_at FROM POLARIS_SCHEMA.lineage_column_edges WHERE realm_id = ?", 300L);
  }

  @Test
  void upsertLineageColumnEdgeDoesNotMoveLastEventAtBackward() throws SQLException {
    upsertSourceAndTargetDatasets();

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
        "SELECT last_event_at FROM POLARIS_SCHEMA.lineage_column_edges WHERE realm_id = ?", 300L);
  }

  @Test
  void lineageUpsertsFailForSchemaBeforeV5() throws SQLException {
    DataSource v4DataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:test_lineage_v4_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1", "sa", "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(v4DataSource, new TestJdbcConfiguration());

    ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
    InputStream schemaStream = classLoader.getResourceAsStream("h2/schema-v4.sql");
    datasourceOperations.executeScript(schemaStream);

    JdbcBasePersistenceImpl v4LineagePersistence =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            PrincipalSecretsGenerator.RANDOM_SECRETS,
            storageProvider(),
            REALM_ID,
            4);

    // Schema v4 intentionally has no lineage tables. JDBC lineage writes must fail clearly instead
    // of silently dropping data.
    assertThrows(
        IllegalStateException.class,
        () ->
            v4LineagePersistence.upsertLineageDataset(
                LineageDatasetRecord.builder()
                    .datasetId(10L)
                    .catalog("polaris")
                    .namespace("analytics")
                    .name("orders")
                    .createdAt(100L)
                    .updatedAt(100L)
                    .build()));
    assertThrows(
        IllegalStateException.class,
        () ->
            v4LineagePersistence.upsertLineageEdge(
                LineageEdgeRecord.builder()
                    .sourceDatasetId(10L)
                    .targetDatasetId(20L)
                    .lastEventAt(100L)
                    .build()));
    assertThrows(
        IllegalStateException.class,
        () ->
            v4LineagePersistence.upsertLineageColumnEdge(
                LineageColumnEdgeRecord.builder()
                    .sourceDatasetId(10L)
                    .sourceField("price")
                    .targetDatasetId(20L)
                    .targetField("total")
                    .lastEventAt(100L)
                    .build()));
  }

  @Test
  void deleteAllRemovesLineageRowsForRealm() throws SQLException {
    upsertSourceAndTargetDatasets();
    lineagePersistence.upsertLineageEdge(
        LineageEdgeRecord.builder()
            .sourceDatasetId(10L)
            .targetDatasetId(20L)
            .lastEventAt(200L)
            .build());
    lineagePersistence.upsertLineageColumnEdge(
        LineageColumnEdgeRecord.builder()
            .sourceDatasetId(10L)
            .sourceField("price")
            .targetDatasetId(20L)
            .targetField("total")
            .lastEventAt(300L)
            .build());

    lineagePersistence.deleteAll(new PolarisCallContext(() -> REALM_ID, lineagePersistence));

    assertSingleLong(
        "SELECT COUNT(*) FROM POLARIS_SCHEMA.lineage_column_edges WHERE realm_id = ?", 0L);
    assertSingleLong("SELECT COUNT(*) FROM POLARIS_SCHEMA.lineage_edges WHERE realm_id = ?", 0L);
    assertSingleLong("SELECT COUNT(*) FROM POLARIS_SCHEMA.lineage_datasets WHERE realm_id = ?", 0L);
  }

  private void upsertSourceAndTargetDatasets() {
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
            .datasetId(20L)
            .catalog("polaris")
            .namespace("analytics")
            .name("orders_daily")
            .createdAt(100L)
            .updatedAt(100L)
            .build());
  }

  private void assertSingleLong(String sql, long expected) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, REALM_ID);
      try (ResultSet rs = statement.executeQuery()) {
        rs.next();
        assertEquals(expected, rs.getLong(1));
        assertFalse(rs.next());
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
    @Override
    public Optional<Integer> maxRetries() {
      return Optional.of(1);
    }

    @Override
    public Optional<Long> maxDurationInMs() {
      return Optional.of(100L);
    }

    @Override
    public Optional<Long> initialDelayInMs() {
      return Optional.of(10L);
    }

    @Override
    public Optional<String> databaseType() {
      return Optional.empty();
    }
  }
}
