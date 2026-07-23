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

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class JdbcBasePersistenceImplTest {

  private static final RealmContext REALM_CONTEXT = () -> "REALM";

  private static final long SECURABLE_CATALOG_ID = 1L;
  private static final long SECURABLE_ID = 2L;
  private static final long GRANTEE_CATALOG_ID = 3L;
  private static final long GRANTEE_ID = 4L;
  private static final int PRIVILEGE_CODE = 21;
  private static final int TIME_SERIES_ROWS = 5;

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5})
  void writeToGrantRecordsIsIdempotent(int schemaVersion) throws SQLException {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:grant_idempotency_v"
                + schemaVersion
                + "_"
                + System.nanoTime()
                + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    try (InputStream scriptStream = DatabaseType.H2.openInitScriptResource(schemaVersion)) {
      datasourceOperations.executeScript(scriptStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    JdbcBasePersistenceImpl basePersistence =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            RANDOM_SECRETS,
            REALM_CONTEXT.getRealmIdentifier(),
            schemaVersion);
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, basePersistence);

    PolarisGrantRecord grant =
        new PolarisGrantRecord(
            SECURABLE_CATALOG_ID, SECURABLE_ID, GRANTEE_CATALOG_ID, GRANTEE_ID, PRIVILEGE_CODE);

    assertThatCode(() -> basePersistence.writeToGrantRecords(callCtx, grant))
        .doesNotThrowAnyException();
    assertThatCode(() -> basePersistence.writeToGrantRecords(callCtx, grant))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "23502", // NOT NULL violation
        "23503", // FK violation
        "23513", // CHECK violation
        "08006", // connection failure
        "40001" // serialization failure
      })
  void writeToGrantRecords_IdempotencyCheck_OnlyCatchesPKUniqueness(String sqlStateCode)
      throws SQLException {
    SQLException nonUniqueViolation = new SQLException(sqlStateCode, sqlStateCode);

    DatasourceOperations datasourceOperations = Mockito.mock(DatasourceOperations.class);
    when(datasourceOperations.getDatabaseType()).thenReturn(DatabaseType.H2);
    doThrow(nonUniqueViolation)
        .when(datasourceOperations)
        .executeUpdate(any(QueryGenerator.PreparedQuery.class));
    doCallRealMethod()
        .when(datasourceOperations)
        .isUniquenessConstraintViolation(any(SQLException.class));

    int schemaVersion = 4;
    JdbcBasePersistenceImpl basePersistence =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            RANDOM_SECRETS,
            REALM_CONTEXT.getRealmIdentifier(),
            schemaVersion);
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, basePersistence);

    PolarisGrantRecord grant =
        new PolarisGrantRecord(
            SECURABLE_CATALOG_ID, SECURABLE_ID, GRANTEE_CATALOG_ID, GRANTEE_ID, PRIVILEGE_CODE);

    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> basePersistence.writeToGrantRecords(callCtx, grant))
        .withMessageContaining("Failed to write to grant records")
        .withCause(nonUniqueViolation);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void lookupEntityVersionsPreservesOrderAndNullsForMissing(int schemaVersion)
      throws SQLException, IOException {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:lookup_versions_v"
                + schemaVersion
                + "_"
                + System.nanoTime()
                + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    DatasourceOperations real = new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    try (InputStream script = DatabaseType.H2.openInitScriptResource(schemaVersion)) {
      real.executeScript(script);
    }
    // Spy so we can assert on the SQL actually issued, without blocking the real call.
    DatasourceOperations spy = Mockito.spy(real);
    doCallRealMethod().when(spy).executeSelect(any(), any());

    JdbcBasePersistenceImpl impl =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            spy,
            RANDOM_SECRETS,
            REALM_CONTEXT.getRealmIdentifier(),
            schemaVersion);
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, impl);

    PolarisBaseEntity e1 =
        new PolarisBaseEntity.Builder()
            .id(101L)
            .catalogId(0L)
            .parentId(0L)
            .typeCode(PolarisEntityType.PRINCIPAL.getCode())
            .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
            .name("e1")
            .entityVersion(1)
            .grantRecordsVersion(2)
            .createTimestamp(System.currentTimeMillis())
            .build();
    PolarisBaseEntity e2 =
        new PolarisBaseEntity.Builder()
            .id(102L)
            .catalogId(0L)
            .parentId(0L)
            .typeCode(PolarisEntityType.PRINCIPAL.getCode())
            .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
            .name("e2")
            .entityVersion(3)
            .grantRecordsVersion(4)
            .createTimestamp(System.currentTimeMillis())
            .build();
    impl.writeEntity(callCtx, e1, false, null);
    impl.writeEntity(callCtx, e2, false, null);

    PolarisEntityId id1 = new PolarisEntityId(0L, 101L);
    PolarisEntityId missing = new PolarisEntityId(0L, 999L);
    PolarisEntityId id2 = new PolarisEntityId(0L, 102L);
    List<PolarisChangeTrackingVersions> result =
        impl.lookupEntityVersions(callCtx, List.of(id2, missing, id1));

    // Behavioural: order preserved, null for missing.
    assertThat(result).hasSize(3);
    assertThat(result.get(0).entityVersion()).isEqualTo(e2.getEntityVersion());
    assertThat(result.get(0).grantRecordsVersion()).isEqualTo(e2.getGrantRecordsVersion());
    assertThat(result.get(1)).isNull();
    assertThat(result.get(2).entityVersion()).isEqualTo(e1.getEntityVersion());
    assertThat(result.get(2).grantRecordsVersion()).isEqualTo(e1.getGrantRecordsVersion());

    // Perf: the SQL that actually executed must not fetch the large JSON property columns.
    ArgumentCaptor<QueryGenerator.PreparedQuery> captor =
        ArgumentCaptor.forClass(QueryGenerator.PreparedQuery.class);
    verify(spy).executeSelect(captor.capture(), any());
    assertThat(captor.getValue().sql()).doesNotContain("properties");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void hasChildrenUsesExistsQueryNotFullScan(int schemaVersion) throws SQLException, IOException {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:has_children_v"
                + schemaVersion
                + "_"
                + System.nanoTime()
                + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    DatasourceOperations real = new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    try (InputStream script = DatabaseType.H2.openInitScriptResource(schemaVersion)) {
      real.executeScript(script);
    }
    DatasourceOperations spy = Mockito.spy(real);
    doCallRealMethod().when(spy).executeSelect(any(), any());

    JdbcBasePersistenceImpl impl =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            spy,
            RANDOM_SECRETS,
            REALM_CONTEXT.getRealmIdentifier(),
            schemaVersion);
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, impl);

    impl.hasChildren(callCtx, null, 0L, 0L);

    ArgumentCaptor<QueryGenerator.PreparedQuery> captor =
        ArgumentCaptor.forClass(QueryGenerator.PreparedQuery.class);
    verify(spy).executeSelect(captor.capture(), any());
    String sql = captor.getValue().sql();
    assertThat(sql).contains("LIMIT 1");
    assertThat(sql).doesNotContain("properties");
    assertThat(sql).doesNotContain("internal_properties");
  }

  @ParameterizedTest
  @ValueSource(ints = {4, 5}) // v4: realm_purge absent (guard skips enqueue); v5: real path
  void deleteAllPurgesMetastoreMetadataButNotTimeSeriesDataForCurrentRealmOnly(int schemaVersion)
      throws SQLException, IOException {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:delete_all_v"
                + schemaVersion
                + "_"
                + System.nanoTime()
                + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
            "sa",
            "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    try (InputStream script = DatabaseType.H2.openInitScriptResource(schemaVersion)) {
      datasourceOperations.executeScript(script);
    }

    RealmContext otherRealm = () -> "OTHER_REALM";

    // Populate every realm-scoped table for both the target realm and another realm.
    populateAllTables(datasourceOperations, REALM_CONTEXT, schemaVersion);
    populateAllTables(datasourceOperations, otherRealm, schemaVersion);

    List<String> tables = expectedRealmScopedTables(schemaVersion);
    for (String table : tables) {
      assertThat(countRows(dataSource, table, REALM_CONTEXT.getRealmIdentifier()))
          .as("rows in %s for target realm before deleteAll", table)
          .isPositive();
    }

    JdbcBasePersistenceImpl impl =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            RANDOM_SECRETS,
            REALM_CONTEXT.getRealmIdentifier(),
            schemaVersion);
    impl.deleteAll(new PolarisCallContext(REALM_CONTEXT, impl));

    // First four tables are metastore metadata (purged); the rest are time-series data (left).
    List<String> metastoreTables = tables.subList(0, 4);
    List<String> timeSeriesTables = tables.subList(4, tables.size());

    for (String table : metastoreTables) {
      assertThat(countRows(dataSource, table, REALM_CONTEXT.getRealmIdentifier()))
          .as("metastore rows in %s for target realm after deleteAll", table)
          .isZero();
      assertThat(countRows(dataSource, table, otherRealm.getRealmIdentifier()))
          .as("rows in %s for other realm must be untouched", table)
          .isPositive();
    }

    for (String table : timeSeriesTables) {
      assertThat(countRows(dataSource, table, REALM_CONTEXT.getRealmIdentifier()))
          .as("time-series rows in %s must survive deleteAll (purged asynchronously)", table)
          .isPositive();
      assertThat(countRows(dataSource, table, otherRealm.getRealmIdentifier()))
          .as("rows in %s for other realm must be untouched", table)
          .isPositive();
    }
  }

  private static List<String> expectedRealmScopedTables(int schemaVersion) {
    List<String> tables =
        new ArrayList<>(
            List.of(
                "entities",
                "grant_records",
                "principal_authentication_data",
                "policy_mapping_record"));
    if (schemaVersion >= 3) {
      tables.add("events");
    }
    if (schemaVersion >= 4) {
      tables.add("scan_metrics_report");
      tables.add("commit_metrics_report");
    }
    return tables;
  }

  private static void populateAllTables(
      DatasourceOperations datasourceOperations, RealmContext realm, int schemaVersion) {
    JdbcBasePersistenceImpl impl =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            RANDOM_SECRETS,
            realm.getRealmIdentifier(),
            schemaVersion);
    PolarisCallContext callCtx = new PolarisCallContext(realm, impl);

    PolarisBaseEntity entity =
        new PolarisBaseEntity.Builder()
            .id(1L)
            .catalogId(0L)
            .parentId(0L)
            .typeCode(PolarisEntityType.PRINCIPAL.getCode())
            .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
            .name("entity-" + realm.getRealmIdentifier())
            .entityVersion(1)
            .grantRecordsVersion(1)
            .createTimestamp(System.currentTimeMillis())
            .build();
    impl.writeEntity(callCtx, entity, false, null);

    impl.writeToGrantRecords(
        callCtx,
        new PolarisGrantRecord(
            SECURABLE_CATALOG_ID, SECURABLE_ID, GRANTEE_CATALOG_ID, GRANTEE_ID, PRIVILEGE_CODE));

    impl.generateNewPrincipalSecrets(callCtx, "principal-" + realm.getRealmIdentifier(), 1L);

    impl.writeToPolicyMappingRecords(
        callCtx,
        new PolarisPolicyMappingRecord(
            1L, 2L, 3L, 4L, PredefinedPolicyTypes.DATA_COMPACTION.getCode(), "{}"));

    if (schemaVersion >= 3) {
      for (int i = 0; i < TIME_SERIES_ROWS; i++) {
        impl.writeEvents(
            List.of(
                new EventEntity(
                    "catalog-1",
                    UUID.randomUUID().toString(),
                    "req-" + i,
                    "TEST_EVENT",
                    System.currentTimeMillis(),
                    "principal-1",
                    EventEntity.ResourceType.CATALOG,
                    "resource-1")));
      }
    }

    if (schemaVersion < 4) {
      return;
    }

    // Metrics report tables live in the relational-jdbc schema, but their write API moved to the
    // metrics extension (#5068), so rows are inserted directly here.
    String realmId = realm.getRealmIdentifier();
    for (int i = 0; i < TIME_SERIES_ROWS; i++) {
      insertRow(
          datasourceOperations,
          "scan_metrics_report",
          "(report_id, realm_id, catalog_id, table_id, timestamp_ms) VALUES (?, ?, ?, ?, ?)",
          List.of(UUID.randomUUID().toString(), realmId, 1L, 2L, 1L));
      insertRow(
          datasourceOperations,
          "commit_metrics_report",
          "(report_id, realm_id, catalog_id, table_id, timestamp_ms, snapshot_id, operation)"
              + " VALUES (?, ?, ?, ?, ?, ?, ?)",
          List.of(UUID.randomUUID().toString(), realmId, 1L, 2L, 1L, 3L, "append"));
    }
  }

  private static void insertRow(
      DatasourceOperations ops, String table, String columnsAndValues, List<Object> params) {
    try {
      ops.executeUpdate(
          new QueryGenerator.PreparedQuery(
              "INSERT INTO "
                  + QueryGenerator.getFullyQualifiedTableName(table)
                  + " "
                  + columnsAndValues,
              params));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static long countRows(JdbcConnectionPool dataSource, String table, String realmId)
      throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement =
            connection.prepareStatement(
                "SELECT COUNT(*) FROM POLARIS_SCHEMA." + table + " WHERE realm_id = ?")) {
      statement.setString(1, realmId);
      try (ResultSet rs = statement.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  private static final class TestJdbcConfiguration implements RelationalJdbcConfiguration {
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

    @Override
    public Optional<String> databaseType() {
      return Optional.of("h2");
    }
  }
}
