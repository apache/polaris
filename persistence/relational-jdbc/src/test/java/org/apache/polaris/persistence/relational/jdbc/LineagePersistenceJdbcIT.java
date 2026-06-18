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
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.lineage.LineageColumnEdge;
import org.apache.polaris.core.lineage.LineageDataset;
import org.apache.polaris.core.lineage.LineageEdge;
import org.apache.polaris.core.lineage.LineageFieldReference;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
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
  private static final Instant EVENT_TIME = Instant.parse("2026-01-01T00:00:00Z");

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
    RealmContext realmContext = () -> realmId;
    LineageDataset source = new LineageDataset("polaris", "analytics", "orders");
    LineageDataset sourceUpdate =
        new LineageDataset("polaris-prod", "analytics", "orders", OptionalLong.of(42L));
    LineageDataset target = new LineageDataset("polaris", "analytics", "orders_daily");

    lineagePersistence.upsertDatasets(realmContext, List.of(source));
    lineagePersistence.upsertDatasets(realmContext, List.of(sourceUpdate));
    assertSingleLong(
        dataSource,
        "SELECT COUNT(*) FROM POLARIS_SCHEMA.lineage_datasets WHERE realm_id = ?",
        realmId,
        1L);
    lineagePersistence.upsertDatasets(realmContext, List.of(target));

    lineagePersistence.upsertDatasetEdges(
        realmContext, List.of(new LineageEdge(source, target)), EVENT_TIME.plusMillis(100));
    lineagePersistence.upsertDatasetEdges(
        realmContext, List.of(new LineageEdge(source, target)), EVENT_TIME);
    assertSingleLong(
        dataSource,
        "SELECT last_event_at FROM POLARIS_SCHEMA.lineage_edges WHERE realm_id = ?",
        realmId,
        EVENT_TIME.plusMillis(100).toEpochMilli());

    LineageColumnEdge columnEdge =
        new LineageColumnEdge(
            new LineageFieldReference(source, "price"), new LineageFieldReference(target, "total"));
    lineagePersistence.upsertColumnEdges(
        realmContext, List.of(columnEdge), EVENT_TIME.plusMillis(200));
    lineagePersistence.upsertColumnEdges(realmContext, List.of(columnEdge), EVENT_TIME);
    assertSingleLong(
        dataSource,
        "SELECT last_event_at FROM POLARIS_SCHEMA.lineage_column_edges WHERE realm_id = ?",
        realmId,
        EVENT_TIME.plusMillis(200).toEpochMilli());
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
      public PolarisStorageIntegration getStorageIntegration(
          List<PolarisEntity> resolvedEntityPath) {
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
