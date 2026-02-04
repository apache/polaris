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
package org.apache.polaris.persistence.relational.jdbc.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.TimerResult;

/**
 * Converter utility class for transforming Iceberg metrics reports into persistence model classes.
 */
public final class MetricsReportConverter {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private MetricsReportConverter() {
    // Utility class
  }

  /**
   * Converts an Iceberg ScanReport to a ModelScanMetricsReport.
   *
   * @param scanReport the Iceberg scan report
   * @param realmId the realm ID for multi-tenancy
   * @param catalogId the catalog ID
   * @param tableId the table entity ID
   * @param namespace the namespace (dot-separated)
   * @param principalName the principal who initiated the scan (optional)
   * @param requestId the request ID (optional)
   * @param otelTraceId OpenTelemetry trace ID (optional)
   * @param otelSpanId OpenTelemetry span ID (optional)
   * @return the converted ModelScanMetricsReport
   */
  public static ModelScanMetricsReport fromScanReport(
      ScanReport scanReport,
      String realmId,
      long catalogId,
      long tableId,
      String namespace,
      @Nullable String principalName,
      @Nullable String requestId,
      @Nullable String otelTraceId,
      @Nullable String otelSpanId) {

    String reportId = UUID.randomUUID().toString();
    long timestampMs = System.currentTimeMillis();

    ScanMetricsResult metrics = scanReport.scanMetrics();

    ImmutableModelScanMetricsReport.Builder builder =
        ImmutableModelScanMetricsReport.builder()
            .reportId(reportId)
            .realmId(realmId)
            .catalogId(catalogId)
            .namespace(namespace)
            .tableId(tableId)
            .timestampMs(timestampMs)
            .principalName(principalName)
            .requestId(requestId)
            .otelTraceId(otelTraceId)
            .otelSpanId(otelSpanId)
            .snapshotId(scanReport.snapshotId())
            .schemaId(scanReport.schemaId())
            .filterExpression(scanReport.filter() != null ? scanReport.filter().toString() : null)
            .projectedFieldIds(formatIntegerList(scanReport.projectedFieldIds()))
            .projectedFieldNames(formatStringList(scanReport.projectedFieldNames()));

    // Extract metrics values
    if (metrics != null) {
      builder
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
          .totalDeleteFileSizeBytes(getCounterValue(metrics.totalDeleteFileSizeInBytes()));
    } else {
      builder
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
          .totalDeleteFileSizeBytes(0L);
    }

    // Store additional metadata as JSON
    Map<String, String> metadata = scanReport.metadata();
    if (metadata != null && !metadata.isEmpty()) {
      builder.metadata(toJson(metadata));
    }

    return builder.build();
  }

  /**
   * Converts an Iceberg CommitReport to a ModelCommitMetricsReport.
   *
   * @param commitReport the Iceberg commit report
   * @param realmId the realm ID for multi-tenancy
   * @param catalogId the catalog ID
   * @param tableId the table entity ID
   * @param namespace the namespace (dot-separated)
   * @param principalName the principal who initiated the commit (optional)
   * @param requestId the request ID (optional)
   * @param otelTraceId OpenTelemetry trace ID (optional)
   * @param otelSpanId OpenTelemetry span ID (optional)
   * @return the converted ModelCommitMetricsReport
   */
  public static ModelCommitMetricsReport fromCommitReport(
      CommitReport commitReport,
      String realmId,
      long catalogId,
      long tableId,
      String namespace,
      @Nullable String principalName,
      @Nullable String requestId,
      @Nullable String otelTraceId,
      @Nullable String otelSpanId) {

    String reportId = UUID.randomUUID().toString();
    long timestampMs = System.currentTimeMillis();

    CommitMetricsResult metrics = commitReport.commitMetrics();

    ImmutableModelCommitMetricsReport.Builder builder =
        ImmutableModelCommitMetricsReport.builder()
            .reportId(reportId)
            .realmId(realmId)
            .catalogId(catalogId)
            .namespace(namespace)
            .tableId(tableId)
            .timestampMs(timestampMs)
            .principalName(principalName)
            .requestId(requestId)
            .otelTraceId(otelTraceId)
            .otelSpanId(otelSpanId)
            .snapshotId(commitReport.snapshotId())
            .sequenceNumber(commitReport.sequenceNumber())
            .operation(commitReport.operation() != null ? commitReport.operation() : "UNKNOWN");

    // Extract metrics values
    if (metrics != null) {
      builder
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
          .totalDurationMs(getTimerValueMs(metrics.totalDuration()))
          .attempts(getCounterValueInt(metrics.attempts()));
    } else {
      builder
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
          .totalDurationMs(0L)
          .attempts(1);
    }

    // Store additional metadata as JSON
    Map<String, String> metadata = commitReport.metadata();
    if (metadata != null && !metadata.isEmpty()) {
      builder.metadata(toJson(metadata));
    }

    return builder.build();
  }

  private static long getCounterValue(@Nullable CounterResult counter) {
    return counter != null ? counter.value() : 0L;
  }

  private static int getCounterValueInt(@Nullable CounterResult counter) {
    return counter != null ? (int) counter.value() : 1;
  }

  private static long getTimerValueMs(@Nullable TimerResult timer) {
    return timer != null && timer.totalDuration() != null ? timer.totalDuration().toMillis() : 0L;
  }

  private static String formatIntegerList(@Nullable List<Integer> list) {
    if (list == null || list.isEmpty()) {
      return null;
    }
    return list.stream().map(String::valueOf).collect(Collectors.joining(","));
  }

  private static String formatStringList(@Nullable List<String> list) {
    if (list == null || list.isEmpty()) {
      return null;
    }
    return String.join(",", list);
  }

  private static String toJson(Map<String, String> map) {
    try {
      return OBJECT_MAPPER.writeValueAsString(map);
    } catch (JsonProcessingException e) {
      return "{}";
    }
  }
}
