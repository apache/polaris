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

import jakarta.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;

/** Model class for scan_metrics_report table - stores scan metrics as first-class entities. */
@PolarisImmutable
public interface ModelScanMetricsReport extends Converter<ModelScanMetricsReport> {
  String TABLE_NAME = "SCAN_METRICS_REPORT";

  // Column names
  String REPORT_ID = "report_id";
  String REALM_ID = "realm_id";
  String CATALOG_ID = "catalog_id";
  String TABLE_ID_COL = "table_id";
  String TIMESTAMP_MS = "timestamp_ms";
  String PRINCIPAL_NAME = "principal_name";
  String REQUEST_ID = "request_id";
  String OTEL_TRACE_ID = "otel_trace_id";
  String OTEL_SPAN_ID = "otel_span_id";
  String REPORT_TRACE_ID = "report_trace_id";
  String SNAPSHOT_ID = "snapshot_id";
  String SCHEMA_ID = "schema_id";
  String FILTER_EXPRESSION = "filter_expression";
  String PROJECTED_FIELD_IDS = "projected_field_ids";
  String PROJECTED_FIELD_NAMES = "projected_field_names";
  String RESULT_DATA_FILES = "result_data_files";
  String RESULT_DELETE_FILES = "result_delete_files";
  String TOTAL_FILE_SIZE_BYTES = "total_file_size_bytes";
  String TOTAL_DATA_MANIFESTS = "total_data_manifests";
  String TOTAL_DELETE_MANIFESTS = "total_delete_manifests";
  String SCANNED_DATA_MANIFESTS = "scanned_data_manifests";
  String SCANNED_DELETE_MANIFESTS = "scanned_delete_manifests";
  String SKIPPED_DATA_MANIFESTS = "skipped_data_manifests";
  String SKIPPED_DELETE_MANIFESTS = "skipped_delete_manifests";
  String SKIPPED_DATA_FILES = "skipped_data_files";
  String SKIPPED_DELETE_FILES = "skipped_delete_files";
  String TOTAL_PLANNING_DURATION_MS = "total_planning_duration_ms";
  String EQUALITY_DELETE_FILES = "equality_delete_files";
  String POSITIONAL_DELETE_FILES = "positional_delete_files";
  String INDEXED_DELETE_FILES = "indexed_delete_files";
  String TOTAL_DELETE_FILE_SIZE_BYTES = "total_delete_file_size_bytes";
  String METADATA = "metadata";

  List<String> ALL_COLUMNS =
      List.of(
          REPORT_ID,
          REALM_ID,
          CATALOG_ID,
          TABLE_ID_COL,
          TIMESTAMP_MS,
          PRINCIPAL_NAME,
          REQUEST_ID,
          OTEL_TRACE_ID,
          OTEL_SPAN_ID,
          REPORT_TRACE_ID,
          SNAPSHOT_ID,
          SCHEMA_ID,
          FILTER_EXPRESSION,
          PROJECTED_FIELD_IDS,
          PROJECTED_FIELD_NAMES,
          RESULT_DATA_FILES,
          RESULT_DELETE_FILES,
          TOTAL_FILE_SIZE_BYTES,
          TOTAL_DATA_MANIFESTS,
          TOTAL_DELETE_MANIFESTS,
          SCANNED_DATA_MANIFESTS,
          SCANNED_DELETE_MANIFESTS,
          SKIPPED_DATA_MANIFESTS,
          SKIPPED_DELETE_MANIFESTS,
          SKIPPED_DATA_FILES,
          SKIPPED_DELETE_FILES,
          TOTAL_PLANNING_DURATION_MS,
          EQUALITY_DELETE_FILES,
          POSITIONAL_DELETE_FILES,
          INDEXED_DELETE_FILES,
          TOTAL_DELETE_FILE_SIZE_BYTES,
          METADATA);

  // Getters
  String getReportId();

  String getRealmId();

  long getCatalogId();

  long getTableId();

  long getTimestampMs();

  @Nullable
  String getPrincipalName();

  @Nullable
  String getRequestId();

  @Nullable
  String getOtelTraceId();

  @Nullable
  String getOtelSpanId();

  @Nullable
  String getReportTraceId();

  @Nullable
  Long getSnapshotId();

  @Nullable
  Integer getSchemaId();

  @Nullable
  String getFilterExpression();

  @Nullable
  String getProjectedFieldIds();

  @Nullable
  String getProjectedFieldNames();

  long getResultDataFiles();

  long getResultDeleteFiles();

  long getTotalFileSizeBytes();

  long getTotalDataManifests();

  long getTotalDeleteManifests();

  long getScannedDataManifests();

  long getScannedDeleteManifests();

  long getSkippedDataManifests();

  long getSkippedDeleteManifests();

  long getSkippedDataFiles();

  long getSkippedDeleteFiles();

  long getTotalPlanningDurationMs();

  long getEqualityDeleteFiles();

  long getPositionalDeleteFiles();

  long getIndexedDeleteFiles();

  long getTotalDeleteFileSizeBytes();

  @Nullable
  String getMetadata();

  @Override
  default ModelScanMetricsReport fromResultSet(ResultSet rs) throws SQLException {
    return ImmutableModelScanMetricsReport.builder()
        .reportId(rs.getString(REPORT_ID))
        .realmId(rs.getString(REALM_ID))
        .catalogId(rs.getLong(CATALOG_ID))
        .tableId(rs.getLong(TABLE_ID_COL))
        .timestampMs(rs.getLong(TIMESTAMP_MS))
        .principalName(rs.getString(PRINCIPAL_NAME))
        .requestId(rs.getString(REQUEST_ID))
        .otelTraceId(rs.getString(OTEL_TRACE_ID))
        .otelSpanId(rs.getString(OTEL_SPAN_ID))
        .reportTraceId(rs.getString(REPORT_TRACE_ID))
        .snapshotId(rs.getObject(SNAPSHOT_ID, Long.class))
        .schemaId(rs.getObject(SCHEMA_ID, Integer.class))
        .filterExpression(rs.getString(FILTER_EXPRESSION))
        .projectedFieldIds(rs.getString(PROJECTED_FIELD_IDS))
        .projectedFieldNames(rs.getString(PROJECTED_FIELD_NAMES))
        .resultDataFiles(rs.getLong(RESULT_DATA_FILES))
        .resultDeleteFiles(rs.getLong(RESULT_DELETE_FILES))
        .totalFileSizeBytes(rs.getLong(TOTAL_FILE_SIZE_BYTES))
        .totalDataManifests(rs.getLong(TOTAL_DATA_MANIFESTS))
        .totalDeleteManifests(rs.getLong(TOTAL_DELETE_MANIFESTS))
        .scannedDataManifests(rs.getLong(SCANNED_DATA_MANIFESTS))
        .scannedDeleteManifests(rs.getLong(SCANNED_DELETE_MANIFESTS))
        .skippedDataManifests(rs.getLong(SKIPPED_DATA_MANIFESTS))
        .skippedDeleteManifests(rs.getLong(SKIPPED_DELETE_MANIFESTS))
        .skippedDataFiles(rs.getLong(SKIPPED_DATA_FILES))
        .skippedDeleteFiles(rs.getLong(SKIPPED_DELETE_FILES))
        .totalPlanningDurationMs(rs.getLong(TOTAL_PLANNING_DURATION_MS))
        .equalityDeleteFiles(rs.getLong(EQUALITY_DELETE_FILES))
        .positionalDeleteFiles(rs.getLong(POSITIONAL_DELETE_FILES))
        .indexedDeleteFiles(rs.getLong(INDEXED_DELETE_FILES))
        .totalDeleteFileSizeBytes(rs.getLong(TOTAL_DELETE_FILE_SIZE_BYTES))
        .metadata(rs.getString(METADATA))
        .build();
  }

  @Override
  default Map<String, Object> toMap(DatabaseType databaseType) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put(REPORT_ID, getReportId());
    map.put(REALM_ID, getRealmId());
    map.put(CATALOG_ID, getCatalogId());
    map.put(TABLE_ID_COL, getTableId());
    map.put(TIMESTAMP_MS, getTimestampMs());
    map.put(PRINCIPAL_NAME, getPrincipalName());
    map.put(REQUEST_ID, getRequestId());
    map.put(OTEL_TRACE_ID, getOtelTraceId());
    map.put(OTEL_SPAN_ID, getOtelSpanId());
    map.put(REPORT_TRACE_ID, getReportTraceId());
    map.put(SNAPSHOT_ID, getSnapshotId());
    map.put(SCHEMA_ID, getSchemaId());
    map.put(FILTER_EXPRESSION, getFilterExpression());
    map.put(PROJECTED_FIELD_IDS, getProjectedFieldIds());
    map.put(PROJECTED_FIELD_NAMES, getProjectedFieldNames());
    map.put(RESULT_DATA_FILES, getResultDataFiles());
    map.put(RESULT_DELETE_FILES, getResultDeleteFiles());
    map.put(TOTAL_FILE_SIZE_BYTES, getTotalFileSizeBytes());
    map.put(TOTAL_DATA_MANIFESTS, getTotalDataManifests());
    map.put(TOTAL_DELETE_MANIFESTS, getTotalDeleteManifests());
    map.put(SCANNED_DATA_MANIFESTS, getScannedDataManifests());
    map.put(SCANNED_DELETE_MANIFESTS, getScannedDeleteManifests());
    map.put(SKIPPED_DATA_MANIFESTS, getSkippedDataManifests());
    map.put(SKIPPED_DELETE_MANIFESTS, getSkippedDeleteManifests());
    map.put(SKIPPED_DATA_FILES, getSkippedDataFiles());
    map.put(SKIPPED_DELETE_FILES, getSkippedDeleteFiles());
    map.put(TOTAL_PLANNING_DURATION_MS, getTotalPlanningDurationMs());
    map.put(EQUALITY_DELETE_FILES, getEqualityDeleteFiles());
    map.put(POSITIONAL_DELETE_FILES, getPositionalDeleteFiles());
    map.put(INDEXED_DELETE_FILES, getIndexedDeleteFiles());
    map.put(TOTAL_DELETE_FILE_SIZE_BYTES, getTotalDeleteFileSizeBytes());

    if (databaseType.equals(DatabaseType.POSTGRES)) {
      map.put(METADATA, toJsonbPGobject(getMetadata() != null ? getMetadata() : "{}"));
    } else {
      map.put(METADATA, getMetadata() != null ? getMetadata() : "{}");
    }
    return map;
  }

  /** Dummy instance to be used as a Converter when calling fromResultSet(). */
  ModelScanMetricsReport CONVERTER =
      ImmutableModelScanMetricsReport.builder()
          .reportId("")
          .realmId("")
          .catalogId(0L)
          .tableId(0L)
          .timestampMs(0L)
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
}
