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
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.lineage.LineageColumnEdge;
import org.apache.polaris.core.lineage.LineageDataset;
import org.apache.polaris.core.lineage.LineageDirection;
import org.apache.polaris.core.lineage.LineageEdge;
import org.apache.polaris.core.lineage.LineageFieldReference;
import org.apache.polaris.core.lineage.LineageGranularity;
import org.apache.polaris.core.lineage.LineageGraph;
import org.apache.polaris.core.lineage.LineageQueryRequest;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** JDBC tests for OpenLineage graph persistence. */
class LineagePersistenceTest {
  private static final String REALM_ID = "TEST_REALM";
  private static final RealmContext REALM_CONTEXT = () -> REALM_ID;
  private static final Instant EVENT_TIME = Instant.parse("2026-01-01T00:00:00Z");

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
    LineageDataset first = new LineageDataset("polaris", "analytics", "orders");
    LineageDataset second =
        new LineageDataset("polaris-prod", "analytics", "orders", OptionalLong.of(42L));

    lineagePersistence.upsertDatasets(REALM_CONTEXT, List.of(first));
    lineagePersistence.upsertDatasets(REALM_CONTEXT, List.of(second));

    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement =
            connection.prepareStatement(
                "SELECT catalog, polaris_entity_id "
                    + "FROM POLARIS_SCHEMA.lineage_datasets WHERE realm_id = ?")) {
      statement.setString(1, REALM_ID);
      try (ResultSet rs = statement.executeQuery()) {
        rs.next();
        assertEquals("polaris-prod", rs.getString("catalog"));
        assertEquals(42L, rs.getLong("polaris_entity_id"));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  void upsertLineageEdgeUpdatesLastEventAt() throws SQLException {
    upsertSourceAndTargetDatasets();

    lineagePersistence.replaceDatasetEdges(
        REALM_CONTEXT,
        List.of(targetDataset()),
        List.of(new LineageEdge(sourceDataset(), targetDataset())),
        EVENT_TIME);
    lineagePersistence.replaceDatasetEdges(
        REALM_CONTEXT,
        List.of(targetDataset()),
        List.of(new LineageEdge(sourceDataset(), targetDataset())),
        EVENT_TIME.plusMillis(100));

    assertSingleLong(
        "SELECT last_event_at FROM POLARIS_SCHEMA.lineage_edges WHERE realm_id = ?",
        EVENT_TIME.plusMillis(100).toEpochMilli());
  }

  @Test
  void upsertLineageEdgeDoesNotMoveLastEventAtBackward() throws SQLException {
    upsertSourceAndTargetDatasets();

    lineagePersistence.replaceDatasetEdges(
        REALM_CONTEXT,
        List.of(targetDataset()),
        List.of(new LineageEdge(sourceDataset(), targetDataset())),
        EVENT_TIME.plusMillis(100));
    lineagePersistence.replaceDatasetEdges(
        REALM_CONTEXT,
        List.of(targetDataset()),
        List.of(new LineageEdge(sourceDataset(), targetDataset())),
        EVENT_TIME);

    assertSingleLong(
        "SELECT last_event_at FROM POLARIS_SCHEMA.lineage_edges WHERE realm_id = ?",
        EVENT_TIME.plusMillis(100).toEpochMilli());
  }

  @Test
  void replaceLineageEdgesReplacesOlderUpstreamsForTargetDataset() throws SQLException {
    upsertSourceAlternativeAndTargetDatasets();

    lineagePersistence.replaceDatasetEdges(
        REALM_CONTEXT,
        List.of(targetDataset()),
        List.of(new LineageEdge(sourceDataset(), targetDataset())),
        EVENT_TIME);
    lineagePersistence.upsertColumnEdges(REALM_CONTEXT, List.of(columnEdge()), EVENT_TIME);

    lineagePersistence.replaceDatasetEdges(
        REALM_CONTEXT,
        List.of(targetDataset()),
        List.of(new LineageEdge(alternativeSourceDataset(), targetDataset())),
        EVENT_TIME.plusMillis(100));

    LineageGraph graph =
        lineagePersistence.loadLineage(
            REALM_CONTEXT,
            new LineageQueryRequest(
                "dataset:polaris:analytics.orders_daily",
                LineageDirection.UPSTREAM,
                LineageGranularity.COLUMN));

    assertEquals(1, graph.upstream().size());
    assertEquals("dataset:polaris:analytics.customers", graph.upstream().get(0).id());
    assertEquals(0, graph.upstream().get(0).fieldMappings().size());
    assertSingleLong("SELECT COUNT(*) FROM POLARIS_SCHEMA.lineage_edges WHERE realm_id = ?", 1L);
    assertSingleLong(
        "SELECT COUNT(*) FROM POLARIS_SCHEMA.lineage_column_edges WHERE realm_id = ?", 0L);
  }

  @Test
  void replaceLineageEdgesDoesNotReplaceNewerSnapshotWithOlderEvent() throws SQLException {
    upsertSourceAlternativeAndTargetDatasets();

    lineagePersistence.replaceDatasetEdges(
        REALM_CONTEXT,
        List.of(targetDataset()),
        List.of(new LineageEdge(alternativeSourceDataset(), targetDataset())),
        EVENT_TIME.plusMillis(100));

    lineagePersistence.replaceDatasetEdges(
        REALM_CONTEXT,
        List.of(targetDataset()),
        List.of(new LineageEdge(sourceDataset(), targetDataset())),
        EVENT_TIME);
    lineagePersistence.upsertColumnEdges(REALM_CONTEXT, List.of(columnEdge()), EVENT_TIME);

    LineageGraph graph =
        lineagePersistence.loadLineage(
            REALM_CONTEXT,
            new LineageQueryRequest(
                "dataset:polaris:analytics.orders_daily",
                LineageDirection.UPSTREAM,
                LineageGranularity.DATASET));

    assertEquals(1, graph.upstream().size());
    assertEquals("dataset:polaris:analytics.customers", graph.upstream().get(0).id());
    assertSingleLong("SELECT COUNT(*) FROM POLARIS_SCHEMA.lineage_edges WHERE realm_id = ?", 1L);
  }

  @Test
  void replaceLineageEdgesClearsUpstreamsForTargetDatasetWhenSnapshotHasNoInputs()
      throws SQLException {
    upsertSourceAndTargetDatasets();

    lineagePersistence.replaceDatasetEdges(
        REALM_CONTEXT,
        List.of(targetDataset()),
        List.of(new LineageEdge(sourceDataset(), targetDataset())),
        EVENT_TIME);
    lineagePersistence.upsertColumnEdges(REALM_CONTEXT, List.of(columnEdge()), EVENT_TIME);

    lineagePersistence.replaceDatasetEdges(
        REALM_CONTEXT, List.of(targetDataset()), List.of(), EVENT_TIME.plusMillis(100));

    lineagePersistence.replaceDatasetEdges(
        REALM_CONTEXT,
        List.of(targetDataset()),
        List.of(new LineageEdge(sourceDataset(), targetDataset())),
        EVENT_TIME);

    LineageGraph graph =
        lineagePersistence.loadLineage(
            REALM_CONTEXT,
            new LineageQueryRequest(
                "dataset:polaris:analytics.orders_daily",
                LineageDirection.UPSTREAM,
                LineageGranularity.COLUMN));

    assertEquals(0, graph.upstream().size());
    assertSingleLong("SELECT COUNT(*) FROM POLARIS_SCHEMA.lineage_edges WHERE realm_id = ?", 0L);
    assertSingleLong(
        "SELECT COUNT(*) FROM POLARIS_SCHEMA.lineage_column_edges WHERE realm_id = ?", 0L);
    assertSingleLong(
        "SELECT last_lineage_event_at FROM POLARIS_SCHEMA.lineage_datasets "
            + "WHERE realm_id = ? AND name = 'orders_daily'",
        EVENT_TIME.plusMillis(100).toEpochMilli());
  }

  @Test
  void upsertLineageColumnEdgeUpdatesLastEventAt() throws SQLException {
    upsertSourceAndTargetDatasets();

    lineagePersistence.upsertColumnEdges(REALM_CONTEXT, List.of(columnEdge()), EVENT_TIME);
    lineagePersistence.upsertColumnEdges(
        REALM_CONTEXT, List.of(columnEdge()), EVENT_TIME.plusMillis(200));

    assertSingleLong(
        "SELECT last_event_at FROM POLARIS_SCHEMA.lineage_column_edges WHERE realm_id = ?",
        EVENT_TIME.plusMillis(200).toEpochMilli());
  }

  @Test
  void upsertLineageColumnEdgeDoesNotMoveLastEventAtBackward() throws SQLException {
    upsertSourceAndTargetDatasets();

    lineagePersistence.upsertColumnEdges(
        REALM_CONTEXT, List.of(columnEdge()), EVENT_TIME.plusMillis(200));
    lineagePersistence.upsertColumnEdges(REALM_CONTEXT, List.of(columnEdge()), EVENT_TIME);

    assertSingleLong(
        "SELECT last_event_at FROM POLARIS_SCHEMA.lineage_column_edges WHERE realm_id = ?",
        EVENT_TIME.plusMillis(200).toEpochMilli());
  }

  @Test
  void loadLineageReturnsDepthOneGraphWithColumnMappings() {
    upsertSourceAndTargetDatasets();
    lineagePersistence.replaceDatasetEdges(
        REALM_CONTEXT,
        List.of(targetDataset()),
        List.of(new LineageEdge(sourceDataset(), targetDataset())),
        EVENT_TIME);
    lineagePersistence.upsertColumnEdges(REALM_CONTEXT, List.of(columnEdge()), EVENT_TIME);

    LineageGraph graph =
        lineagePersistence.loadLineage(
            REALM_CONTEXT,
            new LineageQueryRequest(
                "dataset:polaris:analytics.orders_daily",
                LineageDirection.UPSTREAM,
                LineageGranularity.COLUMN));

    assertEquals("dataset:polaris:analytics.orders_daily", graph.node().id());
    assertEquals(1, graph.upstream().size());
    assertEquals("dataset:polaris:analytics.orders", graph.upstream().get(0).id());
    assertEquals(1, graph.upstream().get(0).fieldMappings().size());
    assertEquals("price", graph.upstream().get(0).fieldMappings().get(0).sourceField());
    assertEquals("total", graph.upstream().get(0).fieldMappings().get(0).targetField());
    assertEquals(0, graph.downstream().size());
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
        () -> v4LineagePersistence.upsertDatasets(REALM_CONTEXT, List.of(sourceDataset())));
    assertThrows(
        IllegalStateException.class,
        () ->
            v4LineagePersistence.replaceDatasetEdges(
                REALM_CONTEXT,
                List.of(targetDataset()),
                List.of(new LineageEdge(sourceDataset(), targetDataset())),
                EVENT_TIME));
    assertThrows(
        IllegalStateException.class,
        () ->
            v4LineagePersistence.upsertColumnEdges(
                REALM_CONTEXT, List.of(columnEdge()), EVENT_TIME));
  }

  @Test
  void deleteAllRemovesLineageRowsForRealm() throws SQLException {
    upsertSourceAndTargetDatasets();
    lineagePersistence.replaceDatasetEdges(
        REALM_CONTEXT,
        List.of(targetDataset()),
        List.of(new LineageEdge(sourceDataset(), targetDataset())),
        EVENT_TIME);
    lineagePersistence.upsertColumnEdges(REALM_CONTEXT, List.of(columnEdge()), EVENT_TIME);

    lineagePersistence.deleteAll(new PolarisCallContext(() -> REALM_ID, lineagePersistence));

    assertSingleLong(
        "SELECT COUNT(*) FROM POLARIS_SCHEMA.lineage_column_edges WHERE realm_id = ?", 0L);
    assertSingleLong("SELECT COUNT(*) FROM POLARIS_SCHEMA.lineage_edges WHERE realm_id = ?", 0L);
    assertSingleLong("SELECT COUNT(*) FROM POLARIS_SCHEMA.lineage_datasets WHERE realm_id = ?", 0L);
  }

  private void upsertSourceAndTargetDatasets() {
    lineagePersistence.upsertDatasets(REALM_CONTEXT, List.of(sourceDataset(), targetDataset()));
  }

  private void upsertSourceAlternativeAndTargetDatasets() {
    lineagePersistence.upsertDatasets(
        REALM_CONTEXT, List.of(sourceDataset(), alternativeSourceDataset(), targetDataset()));
  }

  private static LineageDataset sourceDataset() {
    return new LineageDataset("polaris", "analytics", "orders");
  }

  private static LineageDataset alternativeSourceDataset() {
    return new LineageDataset("polaris", "analytics", "customers");
  }

  private static LineageDataset targetDataset() {
    return new LineageDataset("polaris", "analytics", "orders_daily");
  }

  private static LineageColumnEdge columnEdge() {
    return new LineageColumnEdge(
        new LineageFieldReference(sourceDataset(), "price"),
        new LineageFieldReference(targetDataset(), "total"));
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
      public PolarisStorageIntegration getStorageIntegration(
          List<PolarisEntity> resolvedEntityPath) {
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
