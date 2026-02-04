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
package org.apache.polaris.core.metrics.iceberg;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.TimerResult;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;

/**
 * Converts Iceberg metrics reports to SPI record types using a fluent builder API.
 *
 * <p>This converter extracts all relevant metrics from Iceberg's {@link ScanReport} and {@link
 * CommitReport} and combines them with context information to create persistence-ready records.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ScanMetricsRecord record = MetricsRecordConverter.forScanReport(scanReport)
 *     .catalogId(catalog.getId())
 *     .tableIdentifier(TableIdentifier.of(namespace, tableName))
 *     .build();
 * }</pre>
 */
public final class MetricsRecordConverter {

  private MetricsRecordConverter() {
    // Utility class
  }

  /**
   * Creates a builder for converting a ScanReport to a ScanMetricsRecord.
   *
   * @param scanReport the Iceberg scan report
   * @return builder for configuring the conversion
   */
  public static ScanReportBuilder forScanReport(ScanReport scanReport) {
    return new ScanReportBuilder(scanReport);
  }

  /**
   * Creates a builder for converting a CommitReport to a CommitMetricsRecord.
   *
   * @param commitReport the Iceberg commit report
   * @return builder for configuring the conversion
   */
  public static CommitReportBuilder forCommitReport(CommitReport commitReport) {
    return new CommitReportBuilder(commitReport);
  }

  /**
   * Converts a TableIdentifier namespace to a list of levels.
   *
   * @param tableIdentifier the Iceberg table identifier
   * @return namespace as a list of levels
   */
  private static List<String> namespaceToList(TableIdentifier tableIdentifier) {
    return Arrays.asList(tableIdentifier.namespace().levels());
  }

  /** Builder for converting ScanReport to ScanMetricsRecord. */
  public static final class ScanReportBuilder {
    private final ScanReport scanReport;
    private long catalogId;
    private TableIdentifier tableIdentifier;

    private ScanReportBuilder(ScanReport scanReport) {
      this.scanReport = scanReport;
    }

    public ScanReportBuilder catalogId(long catalogId) {
      this.catalogId = catalogId;
      return this;
    }

    /**
     * Sets the table identifier including namespace and table name.
     *
     * <p>The namespace and table name will be extracted from the TableIdentifier and stored as
     * separate primitive fields in the SPI record.
     *
     * @param tableIdentifier the Iceberg table identifier
     * @return this builder
     */
    public ScanReportBuilder tableIdentifier(TableIdentifier tableIdentifier) {
      this.tableIdentifier = tableIdentifier;
      return this;
    }

    public ScanMetricsRecord build() {
      ScanMetricsResult metrics = scanReport.scanMetrics();
      Map<String, String> reportMetadata =
          scanReport.metadata() != null ? scanReport.metadata() : Collections.emptyMap();

      return ScanMetricsRecord.builder()
          .reportId(UUID.randomUUID().toString())
          .catalogId(catalogId)
          .namespace(namespaceToList(tableIdentifier))
          .tableName(tableIdentifier.name())
          .timestamp(Instant.now())
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
          .metadata(reportMetadata)
          .build();
    }
  }

  /** Builder for converting CommitReport to CommitMetricsRecord. */
  public static final class CommitReportBuilder {
    private final CommitReport commitReport;
    private long catalogId;
    private TableIdentifier tableIdentifier;

    private CommitReportBuilder(CommitReport commitReport) {
      this.commitReport = commitReport;
    }

    public CommitReportBuilder catalogId(long catalogId) {
      this.catalogId = catalogId;
      return this;
    }

    /**
     * Sets the table identifier including namespace and table name.
     *
     * <p>The namespace and table name will be extracted from the TableIdentifier and stored as
     * separate primitive fields in the SPI record.
     *
     * @param tableIdentifier the Iceberg table identifier
     * @return this builder
     */
    public CommitReportBuilder tableIdentifier(TableIdentifier tableIdentifier) {
      this.tableIdentifier = tableIdentifier;
      return this;
    }

    public CommitMetricsRecord build() {
      CommitMetricsResult metrics = commitReport.commitMetrics();
      Map<String, String> reportMetadata =
          commitReport.metadata() != null ? commitReport.metadata() : Collections.emptyMap();

      return CommitMetricsRecord.builder()
          .reportId(UUID.randomUUID().toString())
          .catalogId(catalogId)
          .namespace(namespaceToList(tableIdentifier))
          .tableName(tableIdentifier.name())
          .timestamp(Instant.now())
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
          .metadata(reportMetadata)
          .build();
    }
  }

  // === Helper Methods ===

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
}
