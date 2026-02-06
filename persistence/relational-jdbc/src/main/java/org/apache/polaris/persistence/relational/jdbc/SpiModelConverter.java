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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ImmutableModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ImmutableModelScanMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;

/**
 * Converter between SPI metrics records and JDBC model classes.
 *
 * <p>This utility class provides methods to convert between the backend-agnostic SPI types ({@link
 * ScanMetricsRecord}, {@link CommitMetricsRecord}) and the JDBC-specific model types ({@link
 * ModelScanMetricsReport}, {@link ModelCommitMetricsReport}).
 *
 * <p>Key conversions handled:
 *
 * <ul>
 *   <li>catalogId: long (SPI) ↔ long (Model)
 *   <li>timestamp: Instant (SPI) ↔ long milliseconds (Model)
 *   <li>metadata: Map&lt;String, String&gt; (SPI) ↔ JSON string (Model)
 *   <li>projectedFieldIds/Names: List (SPI) ↔ comma-separated string (Model)
 * </ul>
 */
public final class SpiModelConverter {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private SpiModelConverter() {
    // Utility class
  }

  /**
   * Converts a ScanMetricsRecord (SPI) to ModelScanMetricsReport (JDBC).
   *
   * @param record the SPI record
   * @param realmId the realm ID for multi-tenancy
   * @return the JDBC model
   */
  public static ModelScanMetricsReport toModelScanReport(ScanMetricsRecord record, String realmId) {
    return ImmutableModelScanMetricsReport.builder()
        .reportId(record.reportId())
        .realmId(realmId)
        .catalogId(record.catalogId())
        .tableId(record.tableId())
        .timestampMs(record.timestamp().toEpochMilli())
        .snapshotId(record.snapshotId().orElse(null))
        .schemaId(record.schemaId().orElse(null))
        .filterExpression(record.filterExpression().orElse(null))
        .projectedFieldIds(toCommaSeparated(record.projectedFieldIds()))
        .projectedFieldNames(toCommaSeparated(record.projectedFieldNames()))
        .resultDataFiles(record.resultDataFiles())
        .resultDeleteFiles(record.resultDeleteFiles())
        .totalFileSizeBytes(record.totalFileSizeBytes())
        .totalDataManifests(record.totalDataManifests())
        .totalDeleteManifests(record.totalDeleteManifests())
        .scannedDataManifests(record.scannedDataManifests())
        .scannedDeleteManifests(record.scannedDeleteManifests())
        .skippedDataManifests(record.skippedDataManifests())
        .skippedDeleteManifests(record.skippedDeleteManifests())
        .skippedDataFiles(record.skippedDataFiles())
        .skippedDeleteFiles(record.skippedDeleteFiles())
        .totalPlanningDurationMs(record.totalPlanningDurationMs())
        .equalityDeleteFiles(record.equalityDeleteFiles())
        .positionalDeleteFiles(record.positionalDeleteFiles())
        .indexedDeleteFiles(record.indexedDeleteFiles())
        .totalDeleteFileSizeBytes(record.totalDeleteFileSizeBytes())
        .metadata(toJsonString(record.metadata()))
        .build();
  }

  /**
   * Converts a CommitMetricsRecord (SPI) to ModelCommitMetricsReport (JDBC).
   *
   * @param record the SPI record
   * @param realmId the realm ID for multi-tenancy
   * @return the JDBC model
   */
  public static ModelCommitMetricsReport toModelCommitReport(
      CommitMetricsRecord record, String realmId) {
    return ImmutableModelCommitMetricsReport.builder()
        .reportId(record.reportId())
        .realmId(realmId)
        .catalogId(record.catalogId())
        .tableId(record.tableId())
        .timestampMs(record.timestamp().toEpochMilli())
        .snapshotId(record.snapshotId())
        .sequenceNumber(record.sequenceNumber().orElse(null))
        .operation(record.operation())
        .addedDataFiles(record.addedDataFiles())
        .removedDataFiles(record.removedDataFiles())
        .totalDataFiles(record.totalDataFiles())
        .addedDeleteFiles(record.addedDeleteFiles())
        .removedDeleteFiles(record.removedDeleteFiles())
        .totalDeleteFiles(record.totalDeleteFiles())
        .addedEqualityDeleteFiles(record.addedEqualityDeleteFiles())
        .removedEqualityDeleteFiles(record.removedEqualityDeleteFiles())
        .addedPositionalDeleteFiles(record.addedPositionalDeleteFiles())
        .removedPositionalDeleteFiles(record.removedPositionalDeleteFiles())
        .addedRecords(record.addedRecords())
        .removedRecords(record.removedRecords())
        .totalRecords(record.totalRecords())
        .addedFileSizeBytes(record.addedFileSizeBytes())
        .removedFileSizeBytes(record.removedFileSizeBytes())
        .totalFileSizeBytes(record.totalFileSizeBytes())
        .totalDurationMs(record.totalDurationMs().orElse(0L))
        .attempts(record.attempts())
        .metadata(toJsonString(record.metadata()))
        .build();
  }

  /**
   * Converts a ModelScanMetricsReport (JDBC) to ScanMetricsRecord (SPI).
   *
   * @param model the JDBC model
   * @return the SPI record
   */
  public static ScanMetricsRecord toScanMetricsRecord(ModelScanMetricsReport model) {
    return ScanMetricsRecord.builder()
        .reportId(model.getReportId())
        .catalogId(model.getCatalogId())
        .tableId(model.getTableId())
        .timestamp(Instant.ofEpochMilli(model.getTimestampMs()))
        .snapshotId(Optional.ofNullable(model.getSnapshotId()))
        .schemaId(Optional.ofNullable(model.getSchemaId()))
        .filterExpression(Optional.ofNullable(model.getFilterExpression()))
        .projectedFieldIds(parseIntList(model.getProjectedFieldIds()))
        .projectedFieldNames(parseStringList(model.getProjectedFieldNames()))
        .resultDataFiles(model.getResultDataFiles())
        .resultDeleteFiles(model.getResultDeleteFiles())
        .totalFileSizeBytes(model.getTotalFileSizeBytes())
        .totalDataManifests(model.getTotalDataManifests())
        .totalDeleteManifests(model.getTotalDeleteManifests())
        .scannedDataManifests(model.getScannedDataManifests())
        .scannedDeleteManifests(model.getScannedDeleteManifests())
        .skippedDataManifests(model.getSkippedDataManifests())
        .skippedDeleteManifests(model.getSkippedDeleteManifests())
        .skippedDataFiles(model.getSkippedDataFiles())
        .skippedDeleteFiles(model.getSkippedDeleteFiles())
        .totalPlanningDurationMs(model.getTotalPlanningDurationMs())
        .equalityDeleteFiles(model.getEqualityDeleteFiles())
        .positionalDeleteFiles(model.getPositionalDeleteFiles())
        .indexedDeleteFiles(model.getIndexedDeleteFiles())
        .totalDeleteFileSizeBytes(model.getTotalDeleteFileSizeBytes())
        .metadata(parseMetadataJson(model.getMetadata()))
        .build();
  }

  /**
   * Converts a ModelCommitMetricsReport (JDBC) to CommitMetricsRecord (SPI).
   *
   * @param model the JDBC model
   * @return the SPI record
   */
  public static CommitMetricsRecord toCommitMetricsRecord(ModelCommitMetricsReport model) {
    return CommitMetricsRecord.builder()
        .reportId(model.getReportId())
        .catalogId(model.getCatalogId())
        .tableId(model.getTableId())
        .timestamp(Instant.ofEpochMilli(model.getTimestampMs()))
        .snapshotId(model.getSnapshotId())
        .sequenceNumber(Optional.ofNullable(model.getSequenceNumber()))
        .operation(model.getOperation())
        .addedDataFiles(model.getAddedDataFiles())
        .removedDataFiles(model.getRemovedDataFiles())
        .totalDataFiles(model.getTotalDataFiles())
        .addedDeleteFiles(model.getAddedDeleteFiles())
        .removedDeleteFiles(model.getRemovedDeleteFiles())
        .totalDeleteFiles(model.getTotalDeleteFiles())
        .addedEqualityDeleteFiles(model.getAddedEqualityDeleteFiles())
        .removedEqualityDeleteFiles(model.getRemovedEqualityDeleteFiles())
        .addedPositionalDeleteFiles(model.getAddedPositionalDeleteFiles())
        .removedPositionalDeleteFiles(model.getRemovedPositionalDeleteFiles())
        .addedRecords(model.getAddedRecords())
        .removedRecords(model.getRemovedRecords())
        .totalRecords(model.getTotalRecords())
        .addedFileSizeBytes(model.getAddedFileSizeBytes())
        .removedFileSizeBytes(model.getRemovedFileSizeBytes())
        .totalFileSizeBytes(model.getTotalFileSizeBytes())
        .totalDurationMs(
            model.getTotalDurationMs() > 0
                ? Optional.of(model.getTotalDurationMs())
                : Optional.empty())
        .attempts(model.getAttempts())
        .metadata(parseMetadataJson(model.getMetadata()))
        .build();
  }

  // === Helper Methods ===

  private static String toCommaSeparated(List<?> list) {
    if (list == null || list.isEmpty()) {
      return null;
    }
    return list.stream().map(Object::toString).collect(Collectors.joining(","));
  }

  private static List<Integer> parseIntList(String commaSeparated) {
    if (commaSeparated == null || commaSeparated.isEmpty()) {
      return Collections.emptyList();
    }
    return java.util.Arrays.stream(commaSeparated.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(Integer::parseInt)
        .collect(Collectors.toList());
  }

  private static List<String> parseStringList(String commaSeparated) {
    if (commaSeparated == null || commaSeparated.isEmpty()) {
      return Collections.emptyList();
    }
    return java.util.Arrays.stream(commaSeparated.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  private static String toJsonString(Map<String, String> map) {
    if (map == null || map.isEmpty()) {
      return "{}";
    }
    try {
      return OBJECT_MAPPER.writeValueAsString(map);
    } catch (JsonProcessingException e) {
      return "{}";
    }
  }

  private static Map<String, String> parseMetadataJson(String json) {
    if (json == null || json.isEmpty() || "{}".equals(json)) {
      return Collections.emptyMap();
    }
    try {
      return OBJECT_MAPPER.readValue(json, new TypeReference<Map<String, String>>() {});
    } catch (JsonProcessingException e) {
      return Collections.emptyMap();
    }
  }
}
