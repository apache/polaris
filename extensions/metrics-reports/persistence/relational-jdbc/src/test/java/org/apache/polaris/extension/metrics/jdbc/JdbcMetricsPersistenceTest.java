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
package org.apache.polaris.extension.metrics.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.extension.metrics.spi.MetricsQuerySpi;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for metrics report persistence using {@link JdbcMetricsPersistence}. Tests the
 * SPI-level write operations for scan and commit metrics reports.
 */
class JdbcMetricsPersistenceTest {

  private JdbcMetricsPersistence metricsPersistence;

  @BeforeEach
  void setUp() throws SQLException {
    DataSource dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:test_metrics_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1", "sa", "");

    DatasourceOperations datasourceOperations =
        new DatasourceOperations(dataSource, new TestJdbcConfiguration());

    // Execute main schema v4 (includes metrics tables)
    ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
    InputStream schemaStream = classLoader.getResourceAsStream("h2/schema-v5.sql");
    datasourceOperations.executeScript(schemaStream);

    RealmContext realmContext = () -> "TEST_REALM";
    metricsPersistence = new JdbcMetricsPersistence(datasourceOperations, realmContext);
  }

  @Test
  void testWriteScanReport() {
    ScanMetricsRecord record =
        ScanMetricsRecord.builder()
            .reportId(UUID.randomUUID().toString())
            .catalogId(12345L)
            .tableId(67890L)
            .timestamp(Instant.now())
            .principalName("test-user")
            .requestId("req-123")
            .otelTraceId("trace-abc")
            .otelSpanId("span-xyz")
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
            .build();

    // Should not throw - uses SPI method
    metricsPersistence.writeScanReport(record);
  }

  @Test
  void testWriteCommitReport() {
    CommitMetricsRecord record =
        CommitMetricsRecord.builder()
            .reportId(UUID.randomUUID().toString())
            .catalogId(12345L)
            .tableId(67890L)
            .timestamp(Instant.now())
            .principalName("test-user")
            .requestId("req-456")
            .otelTraceId("trace-def")
            .otelSpanId("span-uvw")
            .snapshotId(12345L)
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
            .attempts(1)
            .build();

    // Should not throw - uses SPI method
    metricsPersistence.writeCommitReport(record);
  }

  @Test
  void testWriteMultipleScanReports() {
    for (int i = 0; i < 10; i++) {
      ScanMetricsRecord record =
          ScanMetricsRecord.builder()
              .reportId(UUID.randomUUID().toString())
              .catalogId(12345L)
              .tableId(100L + i)
              .timestamp(Instant.now())
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

      metricsPersistence.writeScanReport(record);
    }
  }

  @Test
  void testRoundTripInDatasourceSelectedSchema() throws SQLException {
    // The datasource selects a non-default schema (the H2 equivalent of the PostgreSQL driver's
    // currentSchema setting), and the latest bootstrap script creates its tables there. Reads must
    // resolve against the same schema as writes.
    DataSource dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:test_metrics_schema_"
                + UUID.randomUUID()
                + ";DB_CLOSE_DELAY=-1"
                + ";INIT=CREATE SCHEMA IF NOT EXISTS custom_polaris\\;SET SCHEMA custom_polaris",
            "sa",
            "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    datasourceOperations.executeScript(
        DatabaseType.H2.openInitScriptResource(DatabaseType.H2.getLatestSchemaVersion()));
    JdbcMetricsPersistence persistence =
        new JdbcMetricsPersistence(datasourceOperations, () -> "TEST_REALM");

    ScanMetricsRecord scan =
        ScanMetricsRecord.builder()
            .reportId(UUID.randomUUID().toString())
            .catalogId(1L)
            .tableId(2L)
            .timestamp(Instant.now())
            .resultDataFiles(0L)
            .resultDeleteFiles(0L)
            .totalFileSizeBytes(0L)
            .totalDataManifests(0L)
            .totalDeleteManifests(0L)
            .scannedDataManifests(0L)
            .scannedDeleteManifests(0L)
            .skippedDataManifests(0L)
            .skippedDeleteManifests(0L)
            .skippedDataFiles(0L)
            .skippedDeleteFiles(0L)
            .totalPlanningDurationMs(0L)
            .equalityDeleteFiles(0L)
            .positionalDeleteFiles(0L)
            .indexedDeleteFiles(0L)
            .totalDeleteFileSizeBytes(0L)
            .build();
    CommitMetricsRecord commit =
        CommitMetricsRecord.builder()
            .reportId(UUID.randomUUID().toString())
            .catalogId(1L)
            .tableId(2L)
            .timestamp(Instant.now())
            .snapshotId(3L)
            .operation("append")
            .addedDataFiles(0L)
            .removedDataFiles(0L)
            .totalDataFiles(0L)
            .addedDeleteFiles(0L)
            .removedDeleteFiles(0L)
            .totalDeleteFiles(0L)
            .addedEqualityDeleteFiles(0L)
            .removedEqualityDeleteFiles(0L)
            .addedPositionalDeleteFiles(0L)
            .removedPositionalDeleteFiles(0L)
            .addedRecords(0L)
            .removedRecords(0L)
            .totalRecords(0L)
            .addedFileSizeBytes(0L)
            .removedFileSizeBytes(0L)
            .totalFileSizeBytes(0L)
            .attempts(1)
            .build();
    persistence.writeScanReport(scan);
    persistence.writeCommitReport(commit);

    MetricsQuerySpi.QueryResult scans =
        persistence.listReports(
            MetricsQuerySpi.MetricType.SCAN,
            1L,
            List.of(2L),
            null,
            null,
            null,
            PageToken.fromLimit(10));
    MetricsQuerySpi.QueryResult commits =
        persistence.listReports(
            MetricsQuerySpi.MetricType.COMMIT,
            1L,
            List.of(2L),
            null,
            null,
            null,
            PageToken.fromLimit(10));

    assertThat(((MetricsQuerySpi.ScanResult) scans).reports().items())
        .extracting(ScanMetricsRecord::reportId)
        .containsExactly(scan.reportId());
    assertThat(((MetricsQuerySpi.CommitResult) commits).reports().items())
        .extracting(CommitMetricsRecord::reportId)
        .containsExactly(commit.reportId());
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
