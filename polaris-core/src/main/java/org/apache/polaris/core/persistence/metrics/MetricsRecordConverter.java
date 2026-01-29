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
package org.apache.polaris.core.persistence.metrics;

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.TimerResult;

/**
 * Utility class for converting Iceberg metrics reports to SPI record types.
 *
 * <p>This converter extracts all relevant metrics from Iceberg's {@link ScanReport} and {@link
 * CommitReport} and combines them with context information to create persistence-ready records.
 */
public final class MetricsRecordConverter {

  private MetricsRecordConverter() {
    // Utility class
  }

  /**
   * Converts an Iceberg ScanReport to a ScanMetricsRecord.
   *
   * @param scanReport the Iceberg scan report
   * @param tableName the table name
   * @param context the metrics context containing realm, catalog, and request information
   * @return the scan metrics record ready for persistence
   */
  public static ScanMetricsRecord fromScanReport(
      ScanReport scanReport, String tableName, MetricsContext context) {
    ScanMetricsResult metrics = scanReport.scanMetrics();

    return ScanMetricsRecord.builder()
        .reportId(UUID.randomUUID().toString())
        .realmId(context.realmId())
        .catalogId(context.catalogId())
        .catalogName(context.catalogName())
        .namespace(context.namespace())
        .tableName(tableName)
        .timestamp(Instant.now())
        .reportTraceId(getMetadataValue(scanReport.metadata(), "trace-id"))
        .snapshotId(Optional.of(scanReport.snapshotId()))
        .schemaId(Optional.of(scanReport.schemaId()))
        .filterExpression(
            scanReport.filter() != null
                ? Optional.of(scanReport.filter().toString())
                : Optional.empty())
        .projectedFieldIds(
            scanReport.projectedFieldIds() != null
                ? scanReport.projectedFieldIds()
                : Collections.emptyList())
        .projectedFieldNames(
            scanReport.projectedFieldNames() != null
                ? scanReport.projectedFieldNames()
                : Collections.emptyList())
        .resultDataFiles(getCounterValue(metrics.resultDataFiles()))
        .resultDeleteFiles(getCounterValue(metrics.resultDeleteFiles()))
        .totalFileSizeBytes(getCounterValue(metrics.totalFileSizeInBytes()))
        .totalDataManifests(getCounterValue(metrics.totalDataManifests()))
        .totalDeleteManifests(getCounterValue(metrics.totalDeleteManifests()))
        .scannedDataManifests(getCounterValue(metrics.scannedDataManifests()))
        .scannedDeleteManifests(getCounterValue(metrics.scannedDeleteManifests()))
        .skippedDataManifests(getCounterValue(metrics.skippedDataManifests()))
        .skippedDeleteManifests(getCounterValue(metrics.skippedDeleteManifests()))
        .skippedDataFiles(getCounterValue(metrics.skippedDataFiles()))
        .skippedDeleteFiles(getCounterValue(metrics.skippedDeleteFiles()))
        .totalPlanningDurationMs(getTimerValueMs(metrics.totalPlanningDuration()))
        .equalityDeleteFiles(getCounterValue(metrics.equalityDeleteFiles()))
        .positionalDeleteFiles(getCounterValue(metrics.positionalDeleteFiles()))
        .indexedDeleteFiles(getCounterValue(metrics.indexedDeleteFiles()))
        .totalDeleteFileSizeBytes(getCounterValue(metrics.totalDeleteFileSizeInBytes()))
        .metadata(Collections.emptyMap())
        .build();
  }

  /**
   * Converts an Iceberg CommitReport to a CommitMetricsRecord.
   *
   * @param commitReport the Iceberg commit report
   * @param tableName the table name
   * @param context the metrics context containing realm, catalog, and request information
   * @return the commit metrics record ready for persistence
   */
  public static CommitMetricsRecord fromCommitReport(
      CommitReport commitReport, String tableName, MetricsContext context) {
    CommitMetricsResult metrics = commitReport.commitMetrics();

    return CommitMetricsRecord.builder()
        .reportId(UUID.randomUUID().toString())
        .realmId(context.realmId())
        .catalogId(context.catalogId())
        .catalogName(context.catalogName())
        .namespace(context.namespace())
        .tableName(tableName)
        .timestamp(Instant.now())
        .reportTraceId(getMetadataValue(commitReport.metadata(), "trace-id"))
        .snapshotId(commitReport.snapshotId())
        .sequenceNumber(Optional.of(commitReport.sequenceNumber()))
        .operation(commitReport.operation())
        .addedDataFiles(getCounterValue(metrics.addedDataFiles()))
        .removedDataFiles(getCounterValue(metrics.removedDataFiles()))
        .totalDataFiles(getCounterValue(metrics.totalDataFiles()))
        .addedDeleteFiles(getCounterValue(metrics.addedDeleteFiles()))
        .removedDeleteFiles(getCounterValue(metrics.removedDeleteFiles()))
        .totalDeleteFiles(getCounterValue(metrics.totalDeleteFiles()))
        .addedEqualityDeleteFiles(getCounterValue(metrics.addedEqualityDeleteFiles()))
        .removedEqualityDeleteFiles(getCounterValue(metrics.removedEqualityDeleteFiles()))
        .addedPositionalDeleteFiles(getCounterValue(metrics.addedPositionalDeleteFiles()))
        .removedPositionalDeleteFiles(getCounterValue(metrics.removedPositionalDeleteFiles()))
        .addedRecords(getCounterValue(metrics.addedRecords()))
        .removedRecords(getCounterValue(metrics.removedRecords()))
        .totalRecords(getCounterValue(metrics.totalRecords()))
        .addedFileSizeBytes(getCounterValue(metrics.addedFilesSizeInBytes()))
        .removedFileSizeBytes(getCounterValue(metrics.removedFilesSizeInBytes()))
        .totalFileSizeBytes(getCounterValue(metrics.totalFilesSizeInBytes()))
        .totalDurationMs(getTimerValueMsOpt(metrics.totalDuration()))
        .attempts(getCounterValueInt(metrics.attempts()))
        .metadata(Collections.emptyMap())
        .build();
  }

  private static long getCounterValue(CounterResult counter) {
    if (counter == null) {
      return 0L;
    }
    return counter.value();
  }

  private static int getCounterValueInt(CounterResult counter) {
    if (counter == null) {
      return 0;
    }
    return (int) counter.value();
  }

  private static long getTimerValueMs(TimerResult timer) {
    if (timer == null || timer.totalDuration() == null) {
      return 0L;
    }
    return timer.totalDuration().toMillis();
  }

  private static Optional<Long> getTimerValueMsOpt(TimerResult timer) {
    if (timer == null || timer.totalDuration() == null) {
      return Optional.empty();
    }
    return Optional.of(timer.totalDuration().toMillis());
  }

  private static Optional<String> getMetadataValue(
      java.util.Map<String, String> metadata, String key) {
    if (metadata == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(metadata.get(key));
  }
}
