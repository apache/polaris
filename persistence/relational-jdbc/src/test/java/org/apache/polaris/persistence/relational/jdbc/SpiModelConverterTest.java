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
import java.util.Optional;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ImmutableModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ImmutableModelScanMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link SpiModelConverter}. */
public class SpiModelConverterTest {

  private static final String TEST_REPORT_ID = "report-123";
  private static final String TEST_REALM_ID = "realm-1";
  private static final long TEST_CATALOG_ID = 12345L;
  private static final long TEST_TABLE_ID = 67890L;
  private static final Instant TEST_TIMESTAMP = Instant.ofEpochMilli(1704067200000L);
  private static final long TEST_TIMESTAMP_MS = 1704067200000L;

  // === Scan Metrics Test ===

  @Test
  void testToModelScanReport() {
    ScanMetricsRecord record = createTestScanRecord();

    ModelScanMetricsReport model = SpiModelConverter.toModelScanReport(record, TEST_REALM_ID);

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
  void testToScanMetricsRecord() {
    ModelScanMetricsReport model = createTestModelScanReport();

    ScanMetricsRecord record = SpiModelConverter.toScanMetricsRecord(model);

    assertThat(record.reportId()).isEqualTo(TEST_REPORT_ID);
    assertThat(record.catalogId()).isEqualTo(TEST_CATALOG_ID);
    assertThat(record.tableId()).isEqualTo(TEST_TABLE_ID);
    assertThat(record.timestamp()).isEqualTo(TEST_TIMESTAMP);
    assertThat(record.snapshotId()).isEqualTo(Optional.of(123456789L));
    assertThat(record.schemaId()).isEqualTo(Optional.of(1));
    assertThat(record.filterExpression()).isEqualTo(Optional.of("id > 100"));
    assertThat(record.projectedFieldIds()).containsExactly(1, 2, 3);
    assertThat(record.projectedFieldNames()).containsExactly("id", "name", "value");
    assertThat(record.resultDataFiles()).isEqualTo(10L);
    assertThat(record.metadata()).containsEntry("custom", "value");
  }

  @Test
  void testScanRecordRoundTrip() {
    ScanMetricsRecord original = createTestScanRecord();

    ModelScanMetricsReport model = SpiModelConverter.toModelScanReport(original, TEST_REALM_ID);
    ScanMetricsRecord roundTripped = SpiModelConverter.toScanMetricsRecord(model);

    assertThat(roundTripped.reportId()).isEqualTo(original.reportId());
    assertThat(roundTripped.catalogId()).isEqualTo(original.catalogId());
    assertThat(roundTripped.tableId()).isEqualTo(original.tableId());
    assertThat(roundTripped.timestamp()).isEqualTo(original.timestamp());
    assertThat(roundTripped.resultDataFiles()).isEqualTo(original.resultDataFiles());
  }

  // === Commit Metrics Test ===

  @Test
  void testToModelCommitReport() {
    CommitMetricsRecord record = createTestCommitRecord();

    ModelCommitMetricsReport model = SpiModelConverter.toModelCommitReport(record, TEST_REALM_ID);

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
  void testToCommitMetricsRecord() {
    ModelCommitMetricsReport model = createTestModelCommitReport();

    CommitMetricsRecord record = SpiModelConverter.toCommitMetricsRecord(model);

    assertThat(record.reportId()).isEqualTo(TEST_REPORT_ID);
    assertThat(record.catalogId()).isEqualTo(TEST_CATALOG_ID);
    assertThat(record.tableId()).isEqualTo(TEST_TABLE_ID);
    assertThat(record.timestamp()).isEqualTo(TEST_TIMESTAMP);
    assertThat(record.snapshotId()).isEqualTo(987654321L);
    assertThat(record.sequenceNumber()).isEqualTo(Optional.of(5L));
    assertThat(record.operation()).isEqualTo("append");
    assertThat(record.addedDataFiles()).isEqualTo(10L);
    assertThat(record.attempts()).isEqualTo(1);
  }

  @Test
  void testCommitRecordRoundTrip() {
    CommitMetricsRecord original = createTestCommitRecord();

    ModelCommitMetricsReport model = SpiModelConverter.toModelCommitReport(original, TEST_REALM_ID);
    CommitMetricsRecord roundTripped = SpiModelConverter.toCommitMetricsRecord(model);

    assertThat(roundTripped.reportId()).isEqualTo(original.reportId());
    assertThat(roundTripped.catalogId()).isEqualTo(original.catalogId());
    assertThat(roundTripped.tableId()).isEqualTo(original.tableId());
    assertThat(roundTripped.timestamp()).isEqualTo(original.timestamp());
    assertThat(roundTripped.snapshotId()).isEqualTo(original.snapshotId());
    assertThat(roundTripped.operation()).isEqualTo(original.operation());
  }

  // === Edge Cases ===

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

    ModelScanMetricsReport model = SpiModelConverter.toModelScanReport(record, TEST_REALM_ID);
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

    ModelScanMetricsReport model = SpiModelConverter.toModelScanReport(record, TEST_REALM_ID);
    assertThat(model.getMetadata()).isEqualTo("{}");
  }

  // === Helper Methods ===

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

  private ModelScanMetricsReport createTestModelScanReport() {
    return ImmutableModelScanMetricsReport.builder()
        .reportId(TEST_REPORT_ID)
        .realmId(TEST_REALM_ID)
        .catalogId(TEST_CATALOG_ID)
        .tableId(TEST_TABLE_ID)
        .timestampMs(TEST_TIMESTAMP_MS)
        .snapshotId(123456789L)
        .schemaId(1)
        .filterExpression("id > 100")
        .projectedFieldIds("1,2,3")
        .projectedFieldNames("id,name,value")
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
        .metadata("{\"custom\":\"value\"}")
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

  private ModelCommitMetricsReport createTestModelCommitReport() {
    return ImmutableModelCommitMetricsReport.builder()
        .reportId(TEST_REPORT_ID)
        .realmId(TEST_REALM_ID)
        .catalogId(TEST_CATALOG_ID)
        .tableId(TEST_TABLE_ID)
        .timestampMs(TEST_TIMESTAMP_MS)
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
        .metadata("{\"custom\":\"value\"}")
        .build();
  }
}
