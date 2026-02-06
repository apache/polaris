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

import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.persistence.relational.jdbc.models.ImmutableModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ImmutableModelScanMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Integration tests for metrics report persistence using JdbcBasePersistenceImpl. Tests the full
 * flow of writing scan and commit metrics reports to the database.
 */
class MetricsReportPersistenceTest {

  private JdbcBasePersistenceImpl persistence;
  private DatasourceOperations datasourceOperations;

  @BeforeEach
  void setUp() throws SQLException {
    DataSource dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:test_metrics_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1", "sa", "");

    datasourceOperations = new DatasourceOperations(dataSource, new TestJdbcConfiguration());

    // Execute schema v4 for entity tables
    ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
    InputStream schemaStream = classLoader.getResourceAsStream("h2/schema-v4.sql");
    datasourceOperations.executeScript(schemaStream);

    // Execute metrics schema v1 for metrics tables
    InputStream metricsSchemaStream = classLoader.getResourceAsStream("h2/schema-metrics-v1.sql");
    datasourceOperations.executeScript(metricsSchemaStream);

    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    RealmContext realmContext = () -> "TEST_REALM";

    persistence =
        new JdbcBasePersistenceImpl(
            diagServices,
            datasourceOperations,
            RANDOM_SECRETS,
            Mockito.mock(),
            realmContext.getRealmIdentifier(),
            4);
  }

  @Test
  void testWriteScanMetricsReport() {
    ModelScanMetricsReport report =
        ImmutableModelScanMetricsReport.builder()
            .reportId(UUID.randomUUID().toString())
            .realmId("TEST_REALM")
            .catalogId(12345L)
            .namespace("db.schema")
            .tableId(67890L)
            .timestampMs(System.currentTimeMillis())
            .snapshotId(12345L)
            .schemaId(1)
            .filterExpression("id > 100")
            .resultDataFiles(10L)
            .resultDeleteFiles(2L)
            .totalFileSizeBytes(1024000L)
            .totalDataManifests(5L)
            .totalDeleteManifests(1L)
            .scannedDataManifests(3L)
            .scannedDeleteManifests(1L)
            .skippedDataManifests(2L)
            .skippedDeleteManifests(0L)
            .skippedDataFiles(5L)
            .skippedDeleteFiles(0L)
            .totalPlanningDurationMs(150L)
            .equalityDeleteFiles(1L)
            .positionalDeleteFiles(1L)
            .indexedDeleteFiles(0L)
            .totalDeleteFileSizeBytes(10240L)
            .principalName("test-user")
            .requestId("req-123")
            .otelTraceId("trace-abc")
            .otelSpanId("span-xyz")
            .reportTraceId("report-trace-1")
            .build();

    // Should not throw
    persistence.writeScanMetricsReport(report);
  }

  @Test
  void testWriteCommitMetricsReport() {
    ModelCommitMetricsReport report =
        ImmutableModelCommitMetricsReport.builder()
            .reportId(UUID.randomUUID().toString())
            .realmId("TEST_REALM")
            .catalogId(12345L)
            .namespace("db.schema")
            .tableId(67890L)
            .timestampMs(System.currentTimeMillis())
            .snapshotId(12345L)
            .sequenceNumber(1L)
            .operation("append")
            .addedDataFiles(5L)
            .removedDataFiles(0L)
            .totalDataFiles(100L)
            .addedDeleteFiles(0L)
            .removedDeleteFiles(0L)
            .totalDeleteFiles(2L)
            .addedEqualityDeleteFiles(0L)
            .removedEqualityDeleteFiles(0L)
            .addedPositionalDeleteFiles(0L)
            .removedPositionalDeleteFiles(0L)
            .addedRecords(1000L)
            .removedRecords(0L)
            .totalRecords(50000L)
            .addedFileSizeBytes(102400L)
            .removedFileSizeBytes(0L)
            .totalFileSizeBytes(5120000L)
            .totalDurationMs(250L)
            .attempts(1)
            .principalName("test-user")
            .requestId("req-456")
            .otelTraceId("trace-def")
            .otelSpanId("span-uvw")
            .reportTraceId("report-trace-2")
            .build();

    // Should not throw
    persistence.writeCommitMetricsReport(report);
  }

  @Test
  void testWriteMultipleScanReports() {
    for (int i = 0; i < 10; i++) {
      ModelScanMetricsReport report =
          ImmutableModelScanMetricsReport.builder()
              .reportId(UUID.randomUUID().toString())
              .realmId("TEST_REALM")
              .catalogId(12345L)
              .namespace("db.schema")
              .tableId(100L + i)
              .timestampMs(System.currentTimeMillis())
              .resultDataFiles((long) (i * 10))
              .resultDeleteFiles(0L)
              .totalFileSizeBytes((long) (i * 1024))
              .totalDataManifests(1L)
              .totalDeleteManifests(0L)
              .scannedDataManifests(1L)
              .scannedDeleteManifests(0L)
              .skippedDataManifests(0L)
              .skippedDeleteManifests(0L)
              .skippedDataFiles(0L)
              .skippedDeleteFiles(0L)
              .totalPlanningDurationMs((long) (i * 10))
              .equalityDeleteFiles(0L)
              .positionalDeleteFiles(0L)
              .indexedDeleteFiles(0L)
              .totalDeleteFileSizeBytes(0L)
              .build();

      persistence.writeScanMetricsReport(report);
    }
  }

  @Test
  void testWriteReportWithNullOptionalFields() {
    ModelScanMetricsReport report =
        ImmutableModelScanMetricsReport.builder()
            .reportId(UUID.randomUUID().toString())
            .realmId("TEST_REALM")
            .catalogId(12345L)
            .namespace("db")
            .tableId(99999L)
            .timestampMs(System.currentTimeMillis())
            // All optional fields left as null
            .resultDataFiles(1L)
            .resultDeleteFiles(0L)
            .totalFileSizeBytes(100L)
            .totalDataManifests(1L)
            .totalDeleteManifests(0L)
            .scannedDataManifests(1L)
            .scannedDeleteManifests(0L)
            .skippedDataManifests(0L)
            .skippedDeleteManifests(0L)
            .skippedDataFiles(0L)
            .skippedDeleteFiles(0L)
            .totalPlanningDurationMs(10L)
            .equalityDeleteFiles(0L)
            .positionalDeleteFiles(0L)
            .indexedDeleteFiles(0L)
            .totalDeleteFileSizeBytes(0L)
            .build();

    // Should not throw even with null optional fields
    persistence.writeScanMetricsReport(report);
  }

  @Test
  void testQueryScanMetricsReportsByTable() {
    long baseTime = System.currentTimeMillis();

    // Write multiple reports for the same table
    for (int i = 0; i < 5; i++) {
      ModelScanMetricsReport report =
          ImmutableModelScanMetricsReport.builder()
              .reportId(UUID.randomUUID().toString())
              .realmId("TEST_REALM")
              .catalogId(12345L)
              .namespace("db.schema")
              .tableId(88888L)
              .timestampMs(baseTime + i * 1000)
              .resultDataFiles((long) i)
              .resultDeleteFiles(0L)
              .totalFileSizeBytes(100L)
              .totalDataManifests(1L)
              .totalDeleteManifests(0L)
              .scannedDataManifests(1L)
              .scannedDeleteManifests(0L)
              .skippedDataManifests(0L)
              .skippedDeleteManifests(0L)
              .skippedDataFiles(0L)
              .skippedDeleteFiles(0L)
              .totalPlanningDurationMs(10L)
              .equalityDeleteFiles(0L)
              .positionalDeleteFiles(0L)
              .indexedDeleteFiles(0L)
              .totalDeleteFileSizeBytes(0L)
              .build();
      persistence.writeScanMetricsReport(report);
    }

    // Query all reports for the table
    var results = persistence.queryScanMetricsReports(12345L, 88888L, null, null, null, 10);
    assertThat(results).hasSize(5);

    // Query with time range
    var rangeResults =
        persistence.queryScanMetricsReports(
            12345L, 88888L, baseTime + 1000, baseTime + 4000, null, 10);
    assertThat(rangeResults).hasSize(3);

    // Query with limit
    var limitedResults = persistence.queryScanMetricsReports(12345L, 88888L, null, null, null, 2);
    assertThat(limitedResults).hasSize(2);
  }

  @Test
  void testQueryScanMetricsReportsByTraceId() {
    String traceId = "test-trace-" + UUID.randomUUID();

    // Write a report with trace ID
    ModelScanMetricsReport report =
        ImmutableModelScanMetricsReport.builder()
            .reportId(UUID.randomUUID().toString())
            .realmId("TEST_REALM")
            .catalogId(12345L)
            .namespace("db")
            .tableId(77777L)
            .timestampMs(System.currentTimeMillis())
            .otelTraceId(traceId)
            .resultDataFiles(1L)
            .resultDeleteFiles(0L)
            .totalFileSizeBytes(100L)
            .totalDataManifests(1L)
            .totalDeleteManifests(0L)
            .scannedDataManifests(1L)
            .scannedDeleteManifests(0L)
            .skippedDataManifests(0L)
            .skippedDeleteManifests(0L)
            .skippedDataFiles(0L)
            .skippedDeleteFiles(0L)
            .totalPlanningDurationMs(10L)
            .equalityDeleteFiles(0L)
            .positionalDeleteFiles(0L)
            .indexedDeleteFiles(0L)
            .totalDeleteFileSizeBytes(0L)
            .build();
    persistence.writeScanMetricsReport(report);

    // Query by trace ID
    var results = persistence.queryScanMetricsReportsByTraceId(traceId);
    assertThat(results).hasSize(1);
    assertThat(results.get(0).getOtelTraceId()).isEqualTo(traceId);
  }

  @Test
  void testDeleteOldScanMetricsReports() {
    // Create reports with different timestamps
    long now = System.currentTimeMillis();
    long oneHourAgo = now - 3600_000;
    long twoDaysAgo = now - 2 * 24 * 3600_000;

    // Create an old report (2 days ago)
    ModelScanMetricsReport oldReport =
        ImmutableModelScanMetricsReport.builder()
            .reportId("old-report-" + UUID.randomUUID())
            .realmId("TEST_REALM")
            .catalogId(11111L)
            .namespace("test_namespace")
            .tableId(67890L)
            .timestampMs(twoDaysAgo)
            .resultDataFiles(10L)
            .resultDeleteFiles(0L)
            .totalFileSizeBytes(1000L)
            .totalDataManifests(1L)
            .totalDeleteManifests(0L)
            .scannedDataManifests(1L)
            .scannedDeleteManifests(0L)
            .skippedDataManifests(0L)
            .skippedDeleteManifests(0L)
            .skippedDataFiles(0L)
            .skippedDeleteFiles(0L)
            .totalPlanningDurationMs(10L)
            .equalityDeleteFiles(0L)
            .positionalDeleteFiles(0L)
            .indexedDeleteFiles(0L)
            .totalDeleteFileSizeBytes(0L)
            .build();
    persistence.writeScanMetricsReport(oldReport);

    // Create a recent report (1 hour ago)
    ModelScanMetricsReport recentReport =
        ImmutableModelScanMetricsReport.builder()
            .reportId("recent-report-" + UUID.randomUUID())
            .realmId("TEST_REALM")
            .catalogId(11111L)
            .namespace("test_namespace")
            .tableId(67890L)
            .timestampMs(oneHourAgo)
            .resultDataFiles(10L)
            .resultDeleteFiles(0L)
            .totalFileSizeBytes(1000L)
            .totalDataManifests(1L)
            .totalDeleteManifests(0L)
            .scannedDataManifests(1L)
            .scannedDeleteManifests(0L)
            .skippedDataManifests(0L)
            .skippedDeleteManifests(0L)
            .skippedDataFiles(0L)
            .skippedDeleteFiles(0L)
            .totalPlanningDurationMs(10L)
            .equalityDeleteFiles(0L)
            .positionalDeleteFiles(0L)
            .indexedDeleteFiles(0L)
            .totalDeleteFileSizeBytes(0L)
            .build();
    persistence.writeScanMetricsReport(recentReport);

    // Delete reports older than 1 day
    long oneDayAgo = now - 24 * 3600_000;
    int deleted = persistence.deleteScanMetricsReportsOlderThan(oneDayAgo);

    // Should have deleted the old report
    assertThat(deleted).isEqualTo(1);

    // Query to verify only recent report remains
    var results = persistence.queryScanMetricsReports(11111L, 67890L, null, null, null, 10);
    assertThat(results).hasSize(1);
    assertThat(results.get(0).getReportId()).isEqualTo(recentReport.getReportId());
  }

  @Test
  void testDeleteOldCommitMetricsReports() {
    // Create reports with different timestamps
    long now = System.currentTimeMillis();
    long oneHourAgo = now - 3600_000;
    long twoDaysAgo = now - 2 * 24 * 3600_000;

    // Create an old report (2 days ago)
    ModelCommitMetricsReport oldReport =
        ImmutableModelCommitMetricsReport.builder()
            .reportId("old-commit-" + UUID.randomUUID())
            .realmId("TEST_REALM")
            .catalogId(11111L)
            .namespace("test_namespace")
            .tableId(67890L)
            .timestampMs(twoDaysAgo)
            .snapshotId(100L)
            .sequenceNumber(1L)
            .operation("append")
            .addedDataFiles(5L)
            .removedDataFiles(0L)
            .totalDataFiles(5L)
            .addedDeleteFiles(0L)
            .removedDeleteFiles(0L)
            .totalDeleteFiles(0L)
            .addedEqualityDeleteFiles(0L)
            .removedEqualityDeleteFiles(0L)
            .addedPositionalDeleteFiles(0L)
            .removedPositionalDeleteFiles(0L)
            .addedRecords(1000L)
            .removedRecords(0L)
            .totalRecords(1000L)
            .addedFileSizeBytes(10000L)
            .removedFileSizeBytes(0L)
            .totalFileSizeBytes(10000L)
            .totalDurationMs(50L)
            .attempts(1)
            .build();
    persistence.writeCommitMetricsReport(oldReport);

    // Create a recent report (1 hour ago)
    ModelCommitMetricsReport recentReport =
        ImmutableModelCommitMetricsReport.builder()
            .reportId("recent-commit-" + UUID.randomUUID())
            .realmId("TEST_REALM")
            .catalogId(11111L)
            .namespace("test_namespace")
            .tableId(67890L)
            .timestampMs(oneHourAgo)
            .snapshotId(101L)
            .sequenceNumber(2L)
            .operation("append")
            .addedDataFiles(3L)
            .removedDataFiles(0L)
            .totalDataFiles(8L)
            .addedDeleteFiles(0L)
            .removedDeleteFiles(0L)
            .totalDeleteFiles(0L)
            .addedEqualityDeleteFiles(0L)
            .removedEqualityDeleteFiles(0L)
            .addedPositionalDeleteFiles(0L)
            .removedPositionalDeleteFiles(0L)
            .addedRecords(500L)
            .removedRecords(0L)
            .totalRecords(1500L)
            .addedFileSizeBytes(5000L)
            .removedFileSizeBytes(0L)
            .totalFileSizeBytes(15000L)
            .totalDurationMs(30L)
            .attempts(1)
            .build();
    persistence.writeCommitMetricsReport(recentReport);

    // Delete reports older than 1 day
    long oneDayAgo = now - 24 * 3600_000;
    int deleted = persistence.deleteCommitMetricsReportsOlderThan(oneDayAgo);

    // Should have deleted the old report
    assertThat(deleted).isEqualTo(1);

    // Query to verify only recent report remains
    var results = persistence.queryCommitMetricsReports(11111L, 67890L, null, null, null, 10);
    assertThat(results).hasSize(1);
    assertThat(results.get(0).getReportId()).isEqualTo(recentReport.getReportId());
  }

  // ==================== Schema Version < 4 Tests ====================
  // These tests verify graceful degradation when metrics tables don't exist

  @Test
  void testSupportsMetricsPersistence_SchemaV4() {
    assertThat(persistence.supportsMetricsPersistence()).isTrue();
  }

  @Test
  void testSupportsMetricsPersistence_SchemaV3() {
    JdbcBasePersistenceImpl v3Persistence = createPersistenceWithSchemaVersion(3);
    assertThat(v3Persistence.supportsMetricsPersistence()).isFalse();
  }

  @Test
  void testSupportsMetricsPersistence_SchemaV1() {
    JdbcBasePersistenceImpl v1Persistence = createPersistenceWithSchemaVersion(1);
    assertThat(v1Persistence.supportsMetricsPersistence()).isFalse();
  }

  @Test
  void testWriteScanMetricsReport_OlderSchema_IsNoOp() {
    JdbcBasePersistenceImpl v3Persistence = createPersistenceWithSchemaVersion(3);

    ModelScanMetricsReport report =
        ImmutableModelScanMetricsReport.builder()
            .reportId(UUID.randomUUID().toString())
            .realmId("TEST_REALM")
            .catalogId(12345L)
            .namespace("db")
            .tableId(67890L)
            .timestampMs(System.currentTimeMillis())
            .resultDataFiles(1L)
            .resultDeleteFiles(0L)
            .totalFileSizeBytes(100L)
            .totalDataManifests(1L)
            .totalDeleteManifests(0L)
            .scannedDataManifests(1L)
            .scannedDeleteManifests(0L)
            .skippedDataManifests(0L)
            .skippedDeleteManifests(0L)
            .skippedDataFiles(0L)
            .skippedDeleteFiles(0L)
            .totalPlanningDurationMs(10L)
            .equalityDeleteFiles(0L)
            .positionalDeleteFiles(0L)
            .indexedDeleteFiles(0L)
            .totalDeleteFileSizeBytes(0L)
            .build();

    // Should not throw - silently ignored on older schemas
    v3Persistence.writeScanMetricsReport(report);
  }

  @Test
  void testWriteCommitMetricsReport_OlderSchema_IsNoOp() {
    JdbcBasePersistenceImpl v3Persistence = createPersistenceWithSchemaVersion(3);

    ModelCommitMetricsReport report =
        ImmutableModelCommitMetricsReport.builder()
            .reportId(UUID.randomUUID().toString())
            .realmId("TEST_REALM")
            .catalogId(12345L)
            .namespace("db")
            .tableId(67890L)
            .timestampMs(System.currentTimeMillis())
            .snapshotId(12345L)
            .operation("append")
            .addedDataFiles(1L)
            .removedDataFiles(0L)
            .totalDataFiles(1L)
            .addedDeleteFiles(0L)
            .removedDeleteFiles(0L)
            .totalDeleteFiles(0L)
            .addedEqualityDeleteFiles(0L)
            .removedEqualityDeleteFiles(0L)
            .addedPositionalDeleteFiles(0L)
            .removedPositionalDeleteFiles(0L)
            .addedRecords(100L)
            .removedRecords(0L)
            .totalRecords(100L)
            .addedFileSizeBytes(1000L)
            .removedFileSizeBytes(0L)
            .totalFileSizeBytes(1000L)
            .totalDurationMs(50L)
            .attempts(1)
            .build();

    // Should not throw - silently ignored on older schemas
    v3Persistence.writeCommitMetricsReport(report);
  }

  @Test
  void testQueryScanMetricsReports_OlderSchema_ReturnsEmptyList() {
    JdbcBasePersistenceImpl v3Persistence = createPersistenceWithSchemaVersion(3);

    var results = v3Persistence.queryScanMetricsReports(12345L, 67890L, null, null, null, 10);

    assertThat(results).isEmpty();
  }

  @Test
  void testQueryCommitMetricsReports_OlderSchema_ReturnsEmptyList() {
    JdbcBasePersistenceImpl v3Persistence = createPersistenceWithSchemaVersion(3);

    var results = v3Persistence.queryCommitMetricsReports(12345L, 67890L, null, null, null, 10);

    assertThat(results).isEmpty();
  }

  @Test
  void testQueryScanMetricsReportsByTraceId_OlderSchema_ReturnsEmptyList() {
    JdbcBasePersistenceImpl v3Persistence = createPersistenceWithSchemaVersion(3);

    var results = v3Persistence.queryScanMetricsReportsByTraceId("trace-123");

    assertThat(results).isEmpty();
  }

  @Test
  void testQueryCommitMetricsReportsByTraceId_OlderSchema_ReturnsEmptyList() {
    JdbcBasePersistenceImpl v3Persistence = createPersistenceWithSchemaVersion(3);

    var results = v3Persistence.queryCommitMetricsReportsByTraceId("trace-123");

    assertThat(results).isEmpty();
  }

  @Test
  void testDeleteScanMetricsReportsOlderThan_OlderSchema_ReturnsZero() {
    JdbcBasePersistenceImpl v3Persistence = createPersistenceWithSchemaVersion(3);

    int deleted = v3Persistence.deleteScanMetricsReportsOlderThan(System.currentTimeMillis());

    assertThat(deleted).isEqualTo(0);
  }

  @Test
  void testDeleteCommitMetricsReportsOlderThan_OlderSchema_ReturnsZero() {
    JdbcBasePersistenceImpl v3Persistence = createPersistenceWithSchemaVersion(3);

    int deleted = v3Persistence.deleteCommitMetricsReportsOlderThan(System.currentTimeMillis());

    assertThat(deleted).isEqualTo(0);
  }

  @Test
  void testDeleteAllMetricsReportsOlderThan_OlderSchema_ReturnsZero() {
    JdbcBasePersistenceImpl v3Persistence = createPersistenceWithSchemaVersion(3);

    int deleted = v3Persistence.deleteAllMetricsReportsOlderThan(System.currentTimeMillis());

    assertThat(deleted).isEqualTo(0);
  }

  @Test
  void testWriteScanMetricsReportWithRoles() {
    ModelScanMetricsReport report =
        ImmutableModelScanMetricsReport.builder()
            .reportId(UUID.randomUUID().toString())
            .realmId("TEST_REALM")
            .catalogId(12345L)
            .namespace("db.schema")
            .tableId(67890L)
            .timestampMs(System.currentTimeMillis())
            .snapshotId(12345L)
            .schemaId(1)
            .resultDataFiles(10L)
            .resultDeleteFiles(2L)
            .totalFileSizeBytes(1024000L)
            .totalDataManifests(5L)
            .totalDeleteManifests(1L)
            .scannedDataManifests(3L)
            .scannedDeleteManifests(1L)
            .skippedDataManifests(2L)
            .skippedDeleteManifests(0L)
            .skippedDataFiles(5L)
            .skippedDeleteFiles(0L)
            .totalPlanningDurationMs(150L)
            .equalityDeleteFiles(1L)
            .positionalDeleteFiles(1L)
            .indexedDeleteFiles(0L)
            .totalDeleteFileSizeBytes(10240L)
            .principalName("test-user")
            .requestId("req-123")
            .otelTraceId("trace-abc")
            .otelSpanId("span-xyz")
            .reportTraceId("report-trace-roles")
            .roles(Set.of("admin", "data_engineer", "analyst"))
            .build();

    // Should not throw - roles are serialized as JSON array in principal_role_ids column
    persistence.writeScanMetricsReport(report);
  }

  @Test
  void testWriteCommitMetricsReportWithRoles() {
    ModelCommitMetricsReport report =
        ImmutableModelCommitMetricsReport.builder()
            .reportId(UUID.randomUUID().toString())
            .realmId("TEST_REALM")
            .catalogId(12345L)
            .namespace("db.schema")
            .tableId(67890L)
            .timestampMs(System.currentTimeMillis())
            .snapshotId(12345L)
            .sequenceNumber(1L)
            .operation("append")
            .addedDataFiles(5L)
            .removedDataFiles(0L)
            .totalDataFiles(100L)
            .addedDeleteFiles(0L)
            .removedDeleteFiles(0L)
            .totalDeleteFiles(2L)
            .addedEqualityDeleteFiles(0L)
            .removedEqualityDeleteFiles(0L)
            .addedPositionalDeleteFiles(0L)
            .removedPositionalDeleteFiles(0L)
            .addedRecords(1000L)
            .removedRecords(0L)
            .totalRecords(50000L)
            .addedFileSizeBytes(102400L)
            .removedFileSizeBytes(0L)
            .totalFileSizeBytes(5120000L)
            .totalDurationMs(250L)
            .attempts(1)
            .principalName("test-user")
            .requestId("req-456")
            .otelTraceId("trace-def")
            .otelSpanId("span-uvw")
            .reportTraceId("report-trace-roles")
            .roles(Set.of("admin", "data_engineer"))
            .build();

    // Should not throw - roles are serialized as JSON array in principal_role_ids column
    persistence.writeCommitMetricsReport(report);
  }

  @Test
  void testScanMetricsReportRolesAreReadBack() {
    String reportId = UUID.randomUUID().toString();
    String otelTraceId = "otel-trace-roles-read-" + UUID.randomUUID();
    Set<String> expectedRoles = Set.of("admin", "data_engineer", "analyst");

    ModelScanMetricsReport report =
        ImmutableModelScanMetricsReport.builder()
            .reportId(reportId)
            .realmId("TEST_REALM")
            .catalogId(12345L)
            .namespace("db.schema")
            .tableId(67890L)
            .timestampMs(System.currentTimeMillis())
            .snapshotId(12345L)
            .schemaId(1)
            .resultDataFiles(10L)
            .resultDeleteFiles(2L)
            .totalFileSizeBytes(1024000L)
            .totalDataManifests(5L)
            .totalDeleteManifests(1L)
            .scannedDataManifests(3L)
            .scannedDeleteManifests(1L)
            .skippedDataManifests(2L)
            .skippedDeleteManifests(0L)
            .skippedDataFiles(5L)
            .skippedDeleteFiles(0L)
            .totalPlanningDurationMs(150L)
            .equalityDeleteFiles(1L)
            .positionalDeleteFiles(1L)
            .indexedDeleteFiles(0L)
            .totalDeleteFileSizeBytes(10240L)
            .principalName("test-user")
            .requestId("req-123")
            .otelTraceId(otelTraceId)
            .otelSpanId("span-xyz")
            .reportTraceId("report-trace-123")
            .roles(expectedRoles)
            .build();

    persistence.writeScanMetricsReport(report);

    // Query by otel trace ID and verify roles are returned
    List<ModelScanMetricsReport> results =
        persistence.queryScanMetricsReportsByTraceId(otelTraceId);

    assertThat(results).hasSize(1);
    assertThat(results.get(0).getReportId()).isEqualTo(reportId);
    assertThat(results.get(0).getRoles()).containsExactlyInAnyOrderElementsOf(expectedRoles);
  }

  @Test
  void testCommitMetricsReportRolesAreReadBack() {
    String reportId = UUID.randomUUID().toString();
    String otelTraceId = "otel-trace-commit-roles-read-" + UUID.randomUUID();
    Set<String> expectedRoles = Set.of("admin", "data_engineer");

    ModelCommitMetricsReport report =
        ImmutableModelCommitMetricsReport.builder()
            .reportId(reportId)
            .realmId("TEST_REALM")
            .catalogId(12345L)
            .namespace("db.schema")
            .tableId(67890L)
            .timestampMs(System.currentTimeMillis())
            .snapshotId(12345L)
            .sequenceNumber(1L)
            .operation("append")
            .addedDataFiles(5L)
            .removedDataFiles(0L)
            .totalDataFiles(100L)
            .addedDeleteFiles(0L)
            .removedDeleteFiles(0L)
            .totalDeleteFiles(2L)
            .addedEqualityDeleteFiles(0L)
            .removedEqualityDeleteFiles(0L)
            .addedPositionalDeleteFiles(0L)
            .removedPositionalDeleteFiles(0L)
            .addedRecords(1000L)
            .removedRecords(0L)
            .totalRecords(50000L)
            .addedFileSizeBytes(102400L)
            .removedFileSizeBytes(0L)
            .totalFileSizeBytes(5120000L)
            .totalDurationMs(250L)
            .attempts(1)
            .principalName("test-user")
            .requestId("req-456")
            .otelTraceId(otelTraceId)
            .otelSpanId("span-uvw")
            .reportTraceId("report-trace-456")
            .roles(expectedRoles)
            .build();

    persistence.writeCommitMetricsReport(report);

    // Query by otel trace ID and verify roles are returned
    List<ModelCommitMetricsReport> results =
        persistence.queryCommitMetricsReportsByTraceId(otelTraceId);

    assertThat(results).hasSize(1);
    assertThat(results.get(0).getReportId()).isEqualTo(reportId);
    assertThat(results.get(0).getRoles()).containsExactlyInAnyOrderElementsOf(expectedRoles);
  }

  @Test
  void testScanMetricsReportWithEmptyRoles() {
    String reportId = UUID.randomUUID().toString();
    String otelTraceId = "otel-trace-empty-roles-" + UUID.randomUUID();

    ModelScanMetricsReport report =
        ImmutableModelScanMetricsReport.builder()
            .reportId(reportId)
            .realmId("TEST_REALM")
            .catalogId(12345L)
            .namespace("db.schema")
            .tableId(67890L)
            .timestampMs(System.currentTimeMillis())
            .snapshotId(12345L)
            .schemaId(1)
            .resultDataFiles(10L)
            .resultDeleteFiles(2L)
            .totalFileSizeBytes(1024000L)
            .totalDataManifests(5L)
            .totalDeleteManifests(1L)
            .scannedDataManifests(3L)
            .scannedDeleteManifests(1L)
            .skippedDataManifests(2L)
            .skippedDeleteManifests(0L)
            .skippedDataFiles(5L)
            .skippedDeleteFiles(0L)
            .totalPlanningDurationMs(150L)
            .equalityDeleteFiles(1L)
            .positionalDeleteFiles(1L)
            .indexedDeleteFiles(0L)
            .totalDeleteFileSizeBytes(10240L)
            .principalName("test-user")
            .requestId("req-123")
            .otelTraceId(otelTraceId)
            .otelSpanId("span-xyz")
            .reportTraceId("report-trace-empty")
            // No roles set - uses default empty set
            .build();

    persistence.writeScanMetricsReport(report);

    // Query by otel trace ID and verify empty roles
    List<ModelScanMetricsReport> results =
        persistence.queryScanMetricsReportsByTraceId(otelTraceId);

    assertThat(results).hasSize(1);
    assertThat(results.get(0).getRoles()).isEmpty();
  }

  @Test
  void testScanMetricsReportRolesViaTimeRangeQuery() {
    String reportId = UUID.randomUUID().toString();
    long timestamp = System.currentTimeMillis();
    Set<String> expectedRoles = Set.of("role1", "role2");

    ModelScanMetricsReport report =
        ImmutableModelScanMetricsReport.builder()
            .reportId(reportId)
            .realmId("TEST_REALM")
            .catalogId(22222L)
            .namespace("db.schema")
            .tableId(66666L)
            .timestampMs(timestamp)
            .snapshotId(12345L)
            .schemaId(1)
            .resultDataFiles(10L)
            .resultDeleteFiles(2L)
            .totalFileSizeBytes(1024000L)
            .totalDataManifests(5L)
            .totalDeleteManifests(1L)
            .scannedDataManifests(3L)
            .scannedDeleteManifests(1L)
            .skippedDataManifests(2L)
            .skippedDeleteManifests(0L)
            .skippedDataFiles(5L)
            .skippedDeleteFiles(0L)
            .totalPlanningDurationMs(150L)
            .equalityDeleteFiles(1L)
            .positionalDeleteFiles(1L)
            .indexedDeleteFiles(0L)
            .totalDeleteFileSizeBytes(10240L)
            .principalName("test-user")
            .requestId("req-123")
            .otelTraceId("trace-abc")
            .otelSpanId("span-xyz")
            .reportTraceId("report-trace-time-query")
            .roles(expectedRoles)
            .build();

    persistence.writeScanMetricsReport(report);

    // Query by time range and verify roles are returned
    List<ModelScanMetricsReport> results =
        persistence.queryScanMetricsReports(
            22222L, 66666L, timestamp - 1000, timestamp + 1000, null, 100);

    assertThat(results).hasSize(1);
    assertThat(results.get(0).getReportId()).isEqualTo(reportId);
    assertThat(results.get(0).getRoles()).containsExactlyInAnyOrderElementsOf(expectedRoles);
  }

  /**
   * Creates a JdbcBasePersistenceImpl with the specified schema version. This uses the same
   * datasource but with a different reported schema version to test graceful degradation.
   */
  private JdbcBasePersistenceImpl createPersistenceWithSchemaVersion(int schemaVersion) {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    return new JdbcBasePersistenceImpl(
        diagServices,
        datasourceOperations,
        RANDOM_SECRETS,
        Mockito.mock(),
        "TEST_REALM",
        schemaVersion);
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
  }
}
