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
 * Converter for reading ModelCommitMetricsReport from database result sets. This class is needed
 * because the Immutables-generated class cannot be instantiated without required fields.
 */
public class ModelCommitMetricsReportConverter implements Converter<ModelCommitMetricsReport> {

  @Override
  public ModelCommitMetricsReport fromResultSet(ResultSet rs) throws SQLException {
    return ImmutableModelCommitMetricsReport.builder()
        .reportId(rs.getString(ModelCommitMetricsReport.REPORT_ID))
        .realmId(rs.getString(ModelCommitMetricsReport.REALM_ID))
        .catalogId(rs.getLong(ModelCommitMetricsReport.CATALOG_ID))
        .tableId(rs.getLong(ModelCommitMetricsReport.TABLE_ID_COL))
        .timestampMs(rs.getLong(ModelCommitMetricsReport.TIMESTAMP_MS))
        .principalName(rs.getString(ModelCommitMetricsReport.PRINCIPAL_NAME))
        .requestId(rs.getString(ModelCommitMetricsReport.REQUEST_ID))
        .otelTraceId(rs.getString(ModelCommitMetricsReport.OTEL_TRACE_ID))
        .otelSpanId(rs.getString(ModelCommitMetricsReport.OTEL_SPAN_ID))
        .reportTraceId(rs.getString(ModelCommitMetricsReport.REPORT_TRACE_ID))
        .snapshotId(rs.getObject(ModelCommitMetricsReport.SNAPSHOT_ID, Long.class))
        .sequenceNumber(rs.getObject(ModelCommitMetricsReport.SEQUENCE_NUMBER, Long.class))
        .operation(rs.getString(ModelCommitMetricsReport.OPERATION))
        .addedDataFiles(rs.getLong(ModelCommitMetricsReport.ADDED_DATA_FILES))
        .removedDataFiles(rs.getLong(ModelCommitMetricsReport.REMOVED_DATA_FILES))
        .totalDataFiles(rs.getLong(ModelCommitMetricsReport.TOTAL_DATA_FILES))
        .addedDeleteFiles(rs.getLong(ModelCommitMetricsReport.ADDED_DELETE_FILES))
        .removedDeleteFiles(rs.getLong(ModelCommitMetricsReport.REMOVED_DELETE_FILES))
        .totalDeleteFiles(rs.getLong(ModelCommitMetricsReport.TOTAL_DELETE_FILES))
        .addedEqualityDeleteFiles(rs.getLong(ModelCommitMetricsReport.ADDED_EQUALITY_DELETE_FILES))
        .removedEqualityDeleteFiles(
            rs.getLong(ModelCommitMetricsReport.REMOVED_EQUALITY_DELETE_FILES))
        .addedPositionalDeleteFiles(
            rs.getLong(ModelCommitMetricsReport.ADDED_POSITIONAL_DELETE_FILES))
        .removedPositionalDeleteFiles(
            rs.getLong(ModelCommitMetricsReport.REMOVED_POSITIONAL_DELETE_FILES))
        .addedRecords(rs.getLong(ModelCommitMetricsReport.ADDED_RECORDS))
        .removedRecords(rs.getLong(ModelCommitMetricsReport.REMOVED_RECORDS))
        .totalRecords(rs.getLong(ModelCommitMetricsReport.TOTAL_RECORDS))
        .addedFileSizeBytes(rs.getLong(ModelCommitMetricsReport.ADDED_FILE_SIZE_BYTES))
        .removedFileSizeBytes(rs.getLong(ModelCommitMetricsReport.REMOVED_FILE_SIZE_BYTES))
        .totalFileSizeBytes(rs.getLong(ModelCommitMetricsReport.TOTAL_FILE_SIZE_BYTES))
        .totalDurationMs(rs.getObject(ModelCommitMetricsReport.TOTAL_DURATION_MS, Long.class))
        .attempts(rs.getObject(ModelCommitMetricsReport.ATTEMPTS, Integer.class))
        .metadata(rs.getString(ModelCommitMetricsReport.METADATA))
        .build();
  }

  @Override
  public Map<String, Object> toMap(DatabaseType databaseType) {
    throw new UnsupportedOperationException("Converter is read-only");
  }
}
