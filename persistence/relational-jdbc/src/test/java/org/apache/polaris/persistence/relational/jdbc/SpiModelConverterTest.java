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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
import org.junit.jupiter.api.Test;

/** Unit tests for metrics model conversion methods (SPI record -> JDBC model). */
public class SpiModelConverterTest {

  private static final String TEST_REPORT_ID = "report-123";
  private static final String TEST_REALM_ID = "realm-1";
  private static final long TEST_CATALOG_ID = 12345L;
  private static final long TEST_TABLE_ID = 67890L;
  private static final Instant TEST_TIMESTAMP = Instant.ofEpochMilli(1704067200000L);
  private static final long TEST_TIMESTAMP_MS = 1704067200000L;

  @Test
  void testFromScanRecord() {
    ScanMetricsRecord record = createTestScanRecord();

    ModelScanMetricsReport model = ModelScanMetricsReport.fromRecord(record, TEST_REALM_ID);

    assertThat(model.getReportId()).isEqualTo(TEST_REPORT_ID);
    assertThat(model.getRealmId()).isEqualTo(TEST_REALM_ID);
    assertThat(model.getCatalogId()).isEqualTo(TEST_CATALOG_ID);
    assertThat(model.getTableId()).isEqualTo(TEST_TABLE_ID);
    assertThat(model.getTimestampMs()).isEqualTo(TEST_TIMESTAMP_MS);
    assertThat(model.getSnapshotId()).isEqualTo(123456789L);
    assertThat(model.getSchemaId()).isEqualTo(1);
    assertThat(model.getFilterExpression()).isEqualTo("id > 100");
    assertThat(model.getProjectedFieldIds()).isEqualTo("1,2,3");
    assertThat(model.getProjectedFieldNames()).isEqualTo("id,name,value");
    assertThat(model.getResultDataFiles()).isEqualTo(10L);
    assertThat(model.getResultDeleteFiles()).isEqualTo(2L);
    assertThat(model.getTotalFileSizeBytes()).isEqualTo(1024000L);
    assertThat(model.getMetadata()).isEqualTo("{\"custom\":\"value\"}");
  }

  @Test
  void testFromCommitRecord() {
    CommitMetricsRecord record = createTestCommitRecord();

    ModelCommitMetricsReport model = ModelCommitMetricsReport.fromRecord(record, TEST_REALM_ID);

    assertThat(model.getReportId()).isEqualTo(TEST_REPORT_ID);
    assertThat(model.getRealmId()).isEqualTo(TEST_REALM_ID);
    assertThat(model.getCatalogId()).isEqualTo(TEST_CATALOG_ID);
    assertThat(model.getTableId()).isEqualTo(TEST_TABLE_ID);
    assertThat(model.getTimestampMs()).isEqualTo(TEST_TIMESTAMP_MS);
    assertThat(model.getSnapshotId()).isEqualTo(987654321L);
    assertThat(model.getSequenceNumber()).isEqualTo(5L);
    assertThat(model.getOperation()).isEqualTo("append");
    assertThat(model.getAddedDataFiles()).isEqualTo(10L);
    assertThat(model.getRemovedDataFiles()).isEqualTo(2L);
    assertThat(model.getTotalDataFiles()).isEqualTo(100L);
    assertThat(model.getAttempts()).isEqualTo(1);
  }

  @Test
  void testNullOptionalFields() {
    ScanMetricsRecord record =
        ScanMetricsRecord.builder()
            .reportId(TEST_REPORT_ID)
            .catalogId(TEST_CATALOG_ID)
            .tableId(TEST_TABLE_ID)
            .timestamp(TEST_TIMESTAMP)
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

    ModelScanMetricsReport model = ModelScanMetricsReport.fromRecord(record, TEST_REALM_ID);
    assertThat(model.getSnapshotId()).isNull();
    assertThat(model.getSchemaId()).isNull();
    assertThat(model.getFilterExpression()).isNull();
    assertThat(model.getProjectedFieldIds()).isNull();
    assertThat(model.getProjectedFieldNames()).isNull();
  }

  @Test
  void testEmptyMetadata() {
    ScanMetricsRecord record =
        ScanMetricsRecord.builder()
            .reportId(TEST_REPORT_ID)
            .catalogId(TEST_CATALOG_ID)
            .tableId(TEST_TABLE_ID)
            .timestamp(TEST_TIMESTAMP)
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

    ModelScanMetricsReport model = ModelScanMetricsReport.fromRecord(record, TEST_REALM_ID);
    assertThat(model.getMetadata()).isEqualTo("{}");
  }

  private ScanMetricsRecord createTestScanRecord() {
    return ScanMetricsRecord.builder()
        .reportId(TEST_REPORT_ID)
        .catalogId(TEST_CATALOG_ID)
        .tableId(TEST_TABLE_ID)
        .timestamp(TEST_TIMESTAMP)
        .snapshotId(123456789L)
        .schemaId(1)
        .filterExpression("id > 100")
        .projectedFieldIds(List.of(1, 2, 3))
        .projectedFieldNames(List.of("id", "name", "value"))
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
        .totalDeleteFileSizeBytes(2048L)
        .metadata(Map.of("custom", "value"))
        .build();
  }

  private CommitMetricsRecord createTestCommitRecord() {
    return CommitMetricsRecord.builder()
        .reportId(TEST_REPORT_ID)
        .catalogId(TEST_CATALOG_ID)
        .tableId(TEST_TABLE_ID)
        .timestamp(TEST_TIMESTAMP)
        .snapshotId(987654321L)
        .sequenceNumber(5L)
        .operation("append")
        .addedDataFiles(10L)
        .removedDataFiles(2L)
        .totalDataFiles(100L)
        .addedDeleteFiles(1L)
        .removedDeleteFiles(0L)
        .totalDeleteFiles(5L)
        .addedEqualityDeleteFiles(0L)
        .removedEqualityDeleteFiles(0L)
        .addedPositionalDeleteFiles(1L)
        .removedPositionalDeleteFiles(0L)
        .addedRecords(1000L)
        .removedRecords(50L)
        .totalRecords(50000L)
        .addedFileSizeBytes(102400L)
        .removedFileSizeBytes(5120L)
        .totalFileSizeBytes(5120000L)
        .totalDurationMs(250L)
        .attempts(1)
        .metadata(Map.of("custom", "value"))
        .build();
  }
}
