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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;

/**
 * Converter for reading ModelScanMetricsReport from database result sets. This class is needed
 * because the Immutables-generated class cannot be instantiated without required fields.
 */
public class ModelScanMetricsReportConverter implements Converter<ModelScanMetricsReport> {

  @Override
  public ModelScanMetricsReport fromResultSet(ResultSet rs) throws SQLException {
    return ImmutableModelScanMetricsReport.builder()
        .reportId(rs.getString(ModelScanMetricsReport.REPORT_ID))
        .realmId(rs.getString(ModelScanMetricsReport.REALM_ID))
        .catalogId(rs.getLong(ModelScanMetricsReport.CATALOG_ID))
        .tableId(rs.getLong(ModelScanMetricsReport.TABLE_ID_COL))
        .timestampMs(rs.getLong(ModelScanMetricsReport.TIMESTAMP_MS))
        .principalName(rs.getString(ModelScanMetricsReport.PRINCIPAL_NAME))
        .requestId(rs.getString(ModelScanMetricsReport.REQUEST_ID))
        .otelTraceId(rs.getString(ModelScanMetricsReport.OTEL_TRACE_ID))
        .otelSpanId(rs.getString(ModelScanMetricsReport.OTEL_SPAN_ID))
        .reportTraceId(rs.getString(ModelScanMetricsReport.REPORT_TRACE_ID))
        .snapshotId(rs.getObject(ModelScanMetricsReport.SNAPSHOT_ID, Long.class))
        .schemaId(rs.getObject(ModelScanMetricsReport.SCHEMA_ID, Integer.class))
        .filterExpression(rs.getString(ModelScanMetricsReport.FILTER_EXPRESSION))
        .projectedFieldIds(rs.getString(ModelScanMetricsReport.PROJECTED_FIELD_IDS))
        .projectedFieldNames(rs.getString(ModelScanMetricsReport.PROJECTED_FIELD_NAMES))
        .resultDataFiles(rs.getLong(ModelScanMetricsReport.RESULT_DATA_FILES))
        .resultDeleteFiles(rs.getLong(ModelScanMetricsReport.RESULT_DELETE_FILES))
        .totalFileSizeBytes(rs.getLong(ModelScanMetricsReport.TOTAL_FILE_SIZE_BYTES))
        .totalDataManifests(rs.getLong(ModelScanMetricsReport.TOTAL_DATA_MANIFESTS))
        .totalDeleteManifests(rs.getLong(ModelScanMetricsReport.TOTAL_DELETE_MANIFESTS))
        .scannedDataManifests(rs.getLong(ModelScanMetricsReport.SCANNED_DATA_MANIFESTS))
        .scannedDeleteManifests(rs.getLong(ModelScanMetricsReport.SCANNED_DELETE_MANIFESTS))
        .skippedDataManifests(rs.getLong(ModelScanMetricsReport.SKIPPED_DATA_MANIFESTS))
        .skippedDeleteManifests(rs.getLong(ModelScanMetricsReport.SKIPPED_DELETE_MANIFESTS))
        .skippedDataFiles(rs.getLong(ModelScanMetricsReport.SKIPPED_DATA_FILES))
        .skippedDeleteFiles(rs.getLong(ModelScanMetricsReport.SKIPPED_DELETE_FILES))
        .totalPlanningDurationMs(rs.getLong(ModelScanMetricsReport.TOTAL_PLANNING_DURATION_MS))
        .equalityDeleteFiles(rs.getLong(ModelScanMetricsReport.EQUALITY_DELETE_FILES))
        .positionalDeleteFiles(rs.getLong(ModelScanMetricsReport.POSITIONAL_DELETE_FILES))
        .indexedDeleteFiles(rs.getLong(ModelScanMetricsReport.INDEXED_DELETE_FILES))
        .totalDeleteFileSizeBytes(rs.getLong(ModelScanMetricsReport.TOTAL_DELETE_FILE_SIZE_BYTES))
        .metadata(rs.getString(ModelScanMetricsReport.METADATA))
        .build();
  }

  @Override
  public Map<String, Object> toMap(DatabaseType databaseType) {
    throw new UnsupportedOperationException("Converter is read-only");
  }
}
