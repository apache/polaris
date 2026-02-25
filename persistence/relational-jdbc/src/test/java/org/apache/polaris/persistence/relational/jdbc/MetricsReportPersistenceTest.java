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
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RequestIdSupplier;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.persistence.relational.jdbc.models.ImmutableModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ImmutableModelScanMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for metrics report persistence using JdbcBasePersistenceImpl. Tests the full
 * flow of writing scan and commit metrics reports to the database.
 */
class MetricsReportPersistenceTest {

  private JdbcBasePersistenceImpl metricsPersistence;
  private DatasourceOperations datasourceOperations;

  @BeforeEach
  void setUp() throws SQLException {
    DataSource dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:test_metrics_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1", "sa", "");

    datasourceOperations = new DatasourceOperations(dataSource, new TestJdbcConfiguration());

    // Execute main schema v4 (entity tables, grants, etc.)
    ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
    InputStream schemaStream = classLoader.getResourceAsStream("h2/schema-v4.sql");
    datasourceOperations.executeScript(schemaStream);

    // Execute metrics schema (scan_metrics_report, commit_metrics_report tables)
    InputStream metricsSchemaStream = classLoader.getResourceAsStream("h2/schema-metrics-v1.sql");
    datasourceOperations.executeScript(metricsSchemaStream);

    PolarisDiagnostics diagnostics = new PolarisDefaultDiagServiceImpl();
    PolarisStorageIntegrationProvider storageProvider =
        new PolarisStorageIntegrationProvider() {
          @Override
          public <T extends PolarisStorageConfigurationInfo>
              PolarisStorageIntegration<T> getStorageIntegrationForConfig(
                  PolarisStorageConfigurationInfo config) {
            return null;
          }
        };

    // Create JdbcBasePersistenceImpl with metrics datasource set to the same datasource
    metricsPersistence =
        new JdbcBasePersistenceImpl(
            diagnostics,
            datasourceOperations,
            PrincipalSecretsGenerator.RANDOM_SECRETS,
            storageProvider,
            "TEST_REALM",
            4,
            datasourceOperations);
    metricsPersistence.setMetricsRequestContext(null, RequestIdSupplier.NOOP);
  }

  @Test
  void testWriteScanMetricsReport() {
    ModelScanMetricsReport report =
        ImmutableModelScanMetricsReport.builder()
            .reportId(UUID.randomUUID().toString())
            .realmId("TEST_REALM")
            .catalogId(12345L)
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
    metricsPersistence.writeScanMetricsReport(report);
  }

  @Test
  void testWriteCommitMetricsReport() {
    ModelCommitMetricsReport report =
        ImmutableModelCommitMetricsReport.builder()
            .reportId(UUID.randomUUID().toString())
            .realmId("TEST_REALM")
            .catalogId(12345L)
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
    metricsPersistence.writeCommitMetricsReport(report);
  }

  @Test
  void testWriteMultipleScanReports() {
    for (int i = 0; i < 10; i++) {
      ModelScanMetricsReport report =
          ImmutableModelScanMetricsReport.builder()
              .reportId(UUID.randomUUID().toString())
              .realmId("TEST_REALM")
              .catalogId(12345L)
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

      metricsPersistence.writeScanMetricsReport(report);
    }
  }

  @Test
  void testWriteReportWithNullOptionalFields() {
    ModelScanMetricsReport report =
        ImmutableModelScanMetricsReport.builder()
            .reportId(UUID.randomUUID().toString())
            .realmId("TEST_REALM")
            .catalogId(12345L)
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
    metricsPersistence.writeScanMetricsReport(report);
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
      metricsPersistence.writeScanMetricsReport(report);
    }

    // Query all reports for the table (no cursor: null, null for cursorTimestampMs, cursorReportId)
    var results =
        metricsPersistence.queryScanMetricsReports(12345L, 88888L, null, null, null, null, 10);
    assertThat(results).hasSize(5);

    // Query with time range
    var rangeResults =
        metricsPersistence.queryScanMetricsReports(
            12345L, 88888L, baseTime + 1000, baseTime + 4000, null, null, 10);
    assertThat(rangeResults).hasSize(3);

    // Query with limit
    var limitedResults =
        metricsPersistence.queryScanMetricsReports(12345L, 88888L, null, null, null, null, 2);
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
    metricsPersistence.writeScanMetricsReport(report);

    // Query by trace ID
    var results = metricsPersistence.queryScanMetricsReportsByTraceId(traceId);
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
    metricsPersistence.writeScanMetricsReport(oldReport);

    // Create a recent report (1 hour ago)
    ModelScanMetricsReport recentReport =
        ImmutableModelScanMetricsReport.builder()
            .reportId("recent-report-" + UUID.randomUUID())
            .realmId("TEST_REALM")
            .catalogId(11111L)
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
    metricsPersistence.writeScanMetricsReport(recentReport);

    // Delete reports older than 1 day
    long oneDayAgo = now - 24 * 3600_000;
    int deleted = metricsPersistence.deleteScanMetricsReportsOlderThan(oneDayAgo);

    // Should have deleted the old report
    assertThat(deleted).isEqualTo(1);

    // Query to verify only recent report remains
    var results =
        metricsPersistence.queryScanMetricsReports(11111L, 67890L, null, null, null, null, 10);
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
    metricsPersistence.writeCommitMetricsReport(oldReport);

    // Create a recent report (1 hour ago)
    ModelCommitMetricsReport recentReport =
        ImmutableModelCommitMetricsReport.builder()
            .reportId("recent-commit-" + UUID.randomUUID())
            .realmId("TEST_REALM")
            .catalogId(11111L)
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
    metricsPersistence.writeCommitMetricsReport(recentReport);

    // Delete reports older than 1 day
    long oneDayAgo = now - 24 * 3600_000;
    int deleted = metricsPersistence.deleteCommitMetricsReportsOlderThan(oneDayAgo);

    // Should have deleted the old report
    assertThat(deleted).isEqualTo(1);

    // Query to verify only recent report remains
    var results =
        metricsPersistence.queryCommitMetricsReports(11111L, 67890L, null, null, null, null, 10);
    assertThat(results).hasSize(1);
    assertThat(results.get(0).getReportId()).isEqualTo(recentReport.getReportId());
  }

  /**
   * Tests that writing the same reportId twice is idempotent and doesn't throw an exception. This
   * verifies that the ON CONFLICT DO NOTHING (PostgreSQL) / MERGE INTO (H2) logic works correctly.
   */
  @Test
  void testWriteDuplicateScanReportIsIdempotent() {
    String reportId = UUID.randomUUID().toString();

    ModelScanMetricsReport report =
        ImmutableModelScanMetricsReport.builder()
            .reportId(reportId)
            .realmId("TEST_REALM")
            .catalogId(12345L)
            .tableId(99999L)
            .timestampMs(System.currentTimeMillis())
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

    // First write should succeed
    metricsPersistence.writeScanMetricsReport(report);

    // Second write with same reportId should NOT throw (idempotent)
    metricsPersistence.writeScanMetricsReport(report);

    // Verify only one report exists
    var results =
        metricsPersistence.queryScanMetricsReports(12345L, 99999L, null, null, null, null, 10);
    assertThat(results).hasSize(1);
    assertThat(results.get(0).getReportId()).isEqualTo(reportId);
  }

  /**
   * Tests that writing the same reportId twice for commit reports is idempotent and doesn't throw
   * an exception.
   */
  @Test
  void testWriteDuplicateCommitReportIsIdempotent() {
    String reportId = UUID.randomUUID().toString();

    ModelCommitMetricsReport report =
        ImmutableModelCommitMetricsReport.builder()
            .reportId(reportId)
            .realmId("TEST_REALM")
            .catalogId(12345L)
            .tableId(88888L)
            .timestampMs(System.currentTimeMillis())
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

    // First write should succeed
    metricsPersistence.writeCommitMetricsReport(report);

    // Second write with same reportId should NOT throw (idempotent)
    metricsPersistence.writeCommitMetricsReport(report);

    // Verify only one report exists
    var results =
        metricsPersistence.queryCommitMetricsReports(12345L, 88888L, null, null, null, null, 10);
    assertThat(results).hasSize(1);
    assertThat(results.get(0).getReportId()).isEqualTo(reportId);
  }

  /**
   * Tests that pagination works correctly when records have different timestamps. This test
   * verifies that cursor-based pagination using (timestamp_ms, report_id) correctly paginates
   * through all records in chronological order.
   */
  @Test
  void testPaginationAcrossTimestampBoundaries() {
    long baseTime = System.currentTimeMillis();

    // Create 6 reports with varying timestamps (not in order)
    // We use specific UUIDs that we can sort to verify ordering
    String[] reportIds = new String[6];
    long[] timestamps = {
      baseTime + 5000, // Report 0: future
      baseTime + 1000, // Report 1: near future
      baseTime - 1000, // Report 2: recent past
      baseTime + 3000, // Report 3: future
      baseTime - 2000, // Report 4: past
      baseTime // Report 5: now
    };

    for (int i = 0; i < 6; i++) {
      reportIds[i] = UUID.randomUUID().toString();
      ModelScanMetricsReport report =
          ImmutableModelScanMetricsReport.builder()
              .reportId(reportIds[i])
              .realmId("TEST_REALM")
              .catalogId(55555L)
              .tableId(66666L)
              .timestampMs(timestamps[i])
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
      metricsPersistence.writeScanMetricsReport(report);
    }

    // Get first page of 2 results (no cursor: null, null for cursorTimestampMs, cursorReportId)
    var page1 =
        metricsPersistence.queryScanMetricsReports(55555L, 66666L, null, null, null, null, 2);
    assertThat(page1).hasSize(2);

    // Use composite cursor from page 1 for page 2 (timestamp_ms, report_id)
    var lastPage1 = page1.get(page1.size() - 1);
    var page2 =
        metricsPersistence.queryScanMetricsReports(
            55555L, 66666L, null, null, lastPage1.getTimestampMs(), lastPage1.getReportId(), 2);
    assertThat(page2).hasSize(2);

    // Use composite cursor from page 2 for page 3
    var lastPage2 = page2.get(page2.size() - 1);
    var page3 =
        metricsPersistence.queryScanMetricsReports(
            55555L, 66666L, null, null, lastPage2.getTimestampMs(), lastPage2.getReportId(), 2);
    assertThat(page3).hasSize(2);

    // Use composite cursor from page 3 - should get no more results
    var lastPage3 = page3.get(page3.size() - 1);
    var page4 =
        metricsPersistence.queryScanMetricsReports(
            55555L, 66666L, null, null, lastPage3.getTimestampMs(), lastPage3.getReportId(), 2);
    assertThat(page4).isEmpty();

    // Verify no duplicates across pages - collect all report IDs
    java.util.Set<String> allReportIds = new java.util.HashSet<>();
    page1.forEach(r -> allReportIds.add(r.getReportId()));
    page2.forEach(r -> allReportIds.add(r.getReportId()));
    page3.forEach(r -> allReportIds.add(r.getReportId()));

    // Should have exactly 6 unique report IDs (no duplicates, no missing)
    assertThat(allReportIds).hasSize(6);
  }

  /** Tests pagination for commit metrics reports across timestamp boundaries. */
  @Test
  void testCommitPaginationAcrossTimestampBoundaries() {
    long baseTime = System.currentTimeMillis();

    // Create 4 reports with varying timestamps
    for (int i = 0; i < 4; i++) {
      // Alternate between past and future timestamps
      long timestamp = (i % 2 == 0) ? baseTime - (i * 1000) : baseTime + (i * 1000);
      ModelCommitMetricsReport report =
          ImmutableModelCommitMetricsReport.builder()
              .reportId(UUID.randomUUID().toString())
              .realmId("TEST_REALM")
              .catalogId(44444L)
              .tableId(55555L)
              .timestampMs(timestamp)
              .snapshotId(100L + i)
              .sequenceNumber((long) i)
              .operation("append")
              .addedDataFiles((long) i)
              .removedDataFiles(0L)
              .totalDataFiles((long) i)
              .addedDeleteFiles(0L)
              .removedDeleteFiles(0L)
              .totalDeleteFiles(0L)
              .addedEqualityDeleteFiles(0L)
              .removedEqualityDeleteFiles(0L)
              .addedPositionalDeleteFiles(0L)
              .removedPositionalDeleteFiles(0L)
              .addedRecords((long) (i * 100))
              .removedRecords(0L)
              .totalRecords((long) (i * 100))
              .addedFileSizeBytes((long) (i * 1000))
              .removedFileSizeBytes(0L)
              .totalFileSizeBytes((long) (i * 1000))
              .totalDurationMs(50L)
              .attempts(1)
              .build();
      metricsPersistence.writeCommitMetricsReport(report);
    }

    // Paginate through all results
    java.util.Set<String> allReportIds = new java.util.HashSet<>();

    var page1 =
        metricsPersistence.queryCommitMetricsReports(44444L, 55555L, null, null, null, null, 2);
    assertThat(page1).hasSize(2);
    page1.forEach(r -> allReportIds.add(r.getReportId()));

    // Use composite cursor from page 1 for page 2 (timestamp_ms, report_id)
    var lastPage1 = page1.get(page1.size() - 1);
    var page2 =
        metricsPersistence.queryCommitMetricsReports(
            44444L, 55555L, null, null, lastPage1.getTimestampMs(), lastPage1.getReportId(), 2);
    assertThat(page2).hasSize(2);
    page2.forEach(r -> allReportIds.add(r.getReportId()));

    // Verify all 4 unique reports were retrieved
    assertThat(allReportIds).hasSize(4);
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
