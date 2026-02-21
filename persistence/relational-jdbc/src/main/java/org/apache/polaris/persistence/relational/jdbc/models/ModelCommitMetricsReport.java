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

/** Model class for commit_metrics_report table - stores commit metrics as first-class entities. */
@PolarisImmutable
public interface ModelCommitMetricsReport extends Converter<ModelCommitMetricsReport> {
  String TABLE_NAME = "COMMIT_METRICS_REPORT";

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
  String SEQUENCE_NUMBER = "sequence_number";
  String OPERATION = "operation";
  String ADDED_DATA_FILES = "added_data_files";
  String REMOVED_DATA_FILES = "removed_data_files";
  String TOTAL_DATA_FILES = "total_data_files";
  String ADDED_DELETE_FILES = "added_delete_files";
  String REMOVED_DELETE_FILES = "removed_delete_files";
  String TOTAL_DELETE_FILES = "total_delete_files";
  String ADDED_EQUALITY_DELETE_FILES = "added_equality_delete_files";
  String REMOVED_EQUALITY_DELETE_FILES = "removed_equality_delete_files";
  String ADDED_POSITIONAL_DELETE_FILES = "added_positional_delete_files";
  String REMOVED_POSITIONAL_DELETE_FILES = "removed_positional_delete_files";
  String ADDED_RECORDS = "added_records";
  String REMOVED_RECORDS = "removed_records";
  String TOTAL_RECORDS = "total_records";
  String ADDED_FILE_SIZE_BYTES = "added_file_size_bytes";
  String REMOVED_FILE_SIZE_BYTES = "removed_file_size_bytes";
  String TOTAL_FILE_SIZE_BYTES = "total_file_size_bytes";
  String TOTAL_DURATION_MS = "total_duration_ms";
  String ATTEMPTS = "attempts";
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
          SEQUENCE_NUMBER,
          OPERATION,
          ADDED_DATA_FILES,
          REMOVED_DATA_FILES,
          TOTAL_DATA_FILES,
          ADDED_DELETE_FILES,
          REMOVED_DELETE_FILES,
          TOTAL_DELETE_FILES,
          ADDED_EQUALITY_DELETE_FILES,
          REMOVED_EQUALITY_DELETE_FILES,
          ADDED_POSITIONAL_DELETE_FILES,
          REMOVED_POSITIONAL_DELETE_FILES,
          ADDED_RECORDS,
          REMOVED_RECORDS,
          TOTAL_RECORDS,
          ADDED_FILE_SIZE_BYTES,
          REMOVED_FILE_SIZE_BYTES,
          TOTAL_FILE_SIZE_BYTES,
          TOTAL_DURATION_MS,
          ATTEMPTS,
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

  long getSnapshotId();

  @Nullable
  Long getSequenceNumber();

  String getOperation();

  long getAddedDataFiles();

  long getRemovedDataFiles();

  long getTotalDataFiles();

  long getAddedDeleteFiles();

  long getRemovedDeleteFiles();

  long getTotalDeleteFiles();

  long getAddedEqualityDeleteFiles();

  long getRemovedEqualityDeleteFiles();

  long getAddedPositionalDeleteFiles();

  long getRemovedPositionalDeleteFiles();

  long getAddedRecords();

  long getRemovedRecords();

  long getTotalRecords();

  long getAddedFileSizeBytes();

  long getRemovedFileSizeBytes();

  long getTotalFileSizeBytes();

  long getTotalDurationMs();

  int getAttempts();

  @Nullable
  String getMetadata();

  @Override
  default ModelCommitMetricsReport fromResultSet(ResultSet rs) throws SQLException {
    return ImmutableModelCommitMetricsReport.builder()
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
        .snapshotId(rs.getLong(SNAPSHOT_ID))
        .sequenceNumber(rs.getObject(SEQUENCE_NUMBER, Long.class))
        .operation(rs.getString(OPERATION))
        .addedDataFiles(rs.getLong(ADDED_DATA_FILES))
        .removedDataFiles(rs.getLong(REMOVED_DATA_FILES))
        .totalDataFiles(rs.getLong(TOTAL_DATA_FILES))
        .addedDeleteFiles(rs.getLong(ADDED_DELETE_FILES))
        .removedDeleteFiles(rs.getLong(REMOVED_DELETE_FILES))
        .totalDeleteFiles(rs.getLong(TOTAL_DELETE_FILES))
        .addedEqualityDeleteFiles(rs.getLong(ADDED_EQUALITY_DELETE_FILES))
        .removedEqualityDeleteFiles(rs.getLong(REMOVED_EQUALITY_DELETE_FILES))
        .addedPositionalDeleteFiles(rs.getLong(ADDED_POSITIONAL_DELETE_FILES))
        .removedPositionalDeleteFiles(rs.getLong(REMOVED_POSITIONAL_DELETE_FILES))
        .addedRecords(rs.getLong(ADDED_RECORDS))
        .removedRecords(rs.getLong(REMOVED_RECORDS))
        .totalRecords(rs.getLong(TOTAL_RECORDS))
        .addedFileSizeBytes(rs.getLong(ADDED_FILE_SIZE_BYTES))
        .removedFileSizeBytes(rs.getLong(REMOVED_FILE_SIZE_BYTES))
        .totalFileSizeBytes(rs.getLong(TOTAL_FILE_SIZE_BYTES))
        .totalDurationMs(rs.getLong(TOTAL_DURATION_MS))
        .attempts(rs.getInt(ATTEMPTS))
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
    map.put(SEQUENCE_NUMBER, getSequenceNumber());
    map.put(OPERATION, getOperation());
    map.put(ADDED_DATA_FILES, getAddedDataFiles());
    map.put(REMOVED_DATA_FILES, getRemovedDataFiles());
    map.put(TOTAL_DATA_FILES, getTotalDataFiles());
    map.put(ADDED_DELETE_FILES, getAddedDeleteFiles());
    map.put(REMOVED_DELETE_FILES, getRemovedDeleteFiles());
    map.put(TOTAL_DELETE_FILES, getTotalDeleteFiles());
    map.put(ADDED_EQUALITY_DELETE_FILES, getAddedEqualityDeleteFiles());
    map.put(REMOVED_EQUALITY_DELETE_FILES, getRemovedEqualityDeleteFiles());
    map.put(ADDED_POSITIONAL_DELETE_FILES, getAddedPositionalDeleteFiles());
    map.put(REMOVED_POSITIONAL_DELETE_FILES, getRemovedPositionalDeleteFiles());
    map.put(ADDED_RECORDS, getAddedRecords());
    map.put(REMOVED_RECORDS, getRemovedRecords());
    map.put(TOTAL_RECORDS, getTotalRecords());
    map.put(ADDED_FILE_SIZE_BYTES, getAddedFileSizeBytes());
    map.put(REMOVED_FILE_SIZE_BYTES, getRemovedFileSizeBytes());
    map.put(TOTAL_FILE_SIZE_BYTES, getTotalFileSizeBytes());
    map.put(TOTAL_DURATION_MS, getTotalDurationMs());
    map.put(ATTEMPTS, getAttempts());

    if (databaseType.equals(DatabaseType.POSTGRES)) {
      map.put(METADATA, toJsonbPGobject(getMetadata() != null ? getMetadata() : "{}"));
    } else {
      map.put(METADATA, getMetadata() != null ? getMetadata() : "{}");
    }
    return map;
  }

  /** Dummy instance to be used as a Converter when calling fromResultSet(). */
  ModelCommitMetricsReport CONVERTER =
      ImmutableModelCommitMetricsReport.builder()
          .reportId("")
          .realmId("")
          .catalogId(0L)
          .tableId(0L)
          .timestampMs(0L)
          .snapshotId(0L)
          .operation("")
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
          .attempts(1)
          .build();
}
