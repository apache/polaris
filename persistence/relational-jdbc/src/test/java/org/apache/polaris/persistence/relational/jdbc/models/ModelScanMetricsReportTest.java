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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;

public class ModelScanMetricsReportTest {

  private static final String TEST_REPORT_ID = "report-123";
  private static final String TEST_REALM_ID = "realm-1";
  private static final long TEST_CATALOG_ID = 12345L;
  private static final long TEST_TABLE_ID = 67890L;
  private static final long TEST_TIMESTAMP_MS = 1704067200000L;
  private static final String TEST_PRINCIPAL = "user@example.com";
  private static final String TEST_REQUEST_ID = "req-456";
  private static final String TEST_OTEL_TRACE_ID = "trace-789";
  private static final String TEST_OTEL_SPAN_ID = "span-012";
  private static final String TEST_REPORT_TRACE_ID = "report-trace-345";
  private static final Long TEST_SNAPSHOT_ID = 123456789L;
  private static final Integer TEST_SCHEMA_ID = 1;
  private static final String TEST_FILTER = "id > 100";
  private static final String TEST_PROJECTED_IDS = "1,2,3";
  private static final String TEST_PROJECTED_NAMES = "id,name,value";
  private static final long TEST_RESULT_DATA_FILES = 10L;
  private static final long TEST_RESULT_DELETE_FILES = 2L;
  private static final long TEST_TOTAL_FILE_SIZE = 1024000L;
  private static final long TEST_TOTAL_DATA_MANIFESTS = 5L;
  private static final long TEST_TOTAL_DELETE_MANIFESTS = 1L;
  private static final long TEST_SCANNED_DATA_MANIFESTS = 3L;
  private static final long TEST_SCANNED_DELETE_MANIFESTS = 1L;
  private static final long TEST_SKIPPED_DATA_MANIFESTS = 2L;
  private static final long TEST_SKIPPED_DELETE_MANIFESTS = 0L;
  private static final long TEST_SKIPPED_DATA_FILES = 5L;
  private static final long TEST_SKIPPED_DELETE_FILES = 0L;
  private static final long TEST_PLANNING_DURATION = 150L;
  private static final long TEST_EQUALITY_DELETE_FILES = 1L;
  private static final long TEST_POSITIONAL_DELETE_FILES = 1L;
  private static final long TEST_INDEXED_DELETE_FILES = 0L;
  private static final long TEST_DELETE_FILE_SIZE = 2048L;
  private static final String TEST_METADATA = "{\"custom\":\"value\"}";

  @Test
  public void testFromResultSet() throws SQLException {
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.getString(ModelScanMetricsReport.REPORT_ID)).thenReturn(TEST_REPORT_ID);
    when(mockResultSet.getString(ModelScanMetricsReport.REALM_ID)).thenReturn(TEST_REALM_ID);
    when(mockResultSet.getLong(ModelScanMetricsReport.CATALOG_ID)).thenReturn(TEST_CATALOG_ID);
    when(mockResultSet.getLong(ModelScanMetricsReport.TABLE_ID_COL)).thenReturn(TEST_TABLE_ID);
    when(mockResultSet.getLong(ModelScanMetricsReport.TIMESTAMP_MS)).thenReturn(TEST_TIMESTAMP_MS);
    when(mockResultSet.getString(ModelScanMetricsReport.PRINCIPAL_NAME)).thenReturn(TEST_PRINCIPAL);
    when(mockResultSet.getString(ModelScanMetricsReport.REQUEST_ID)).thenReturn(TEST_REQUEST_ID);
    when(mockResultSet.getString(ModelScanMetricsReport.OTEL_TRACE_ID))
        .thenReturn(TEST_OTEL_TRACE_ID);
    when(mockResultSet.getString(ModelScanMetricsReport.OTEL_SPAN_ID))
        .thenReturn(TEST_OTEL_SPAN_ID);
    when(mockResultSet.getString(ModelScanMetricsReport.REPORT_TRACE_ID))
        .thenReturn(TEST_REPORT_TRACE_ID);
    when(mockResultSet.getObject(ModelScanMetricsReport.SNAPSHOT_ID, Long.class))
        .thenReturn(TEST_SNAPSHOT_ID);
    when(mockResultSet.getObject(ModelScanMetricsReport.SCHEMA_ID, Integer.class))
        .thenReturn(TEST_SCHEMA_ID);
    when(mockResultSet.getString(ModelScanMetricsReport.FILTER_EXPRESSION)).thenReturn(TEST_FILTER);
    when(mockResultSet.getString(ModelScanMetricsReport.PROJECTED_FIELD_IDS))
        .thenReturn(TEST_PROJECTED_IDS);
    when(mockResultSet.getString(ModelScanMetricsReport.PROJECTED_FIELD_NAMES))
        .thenReturn(TEST_PROJECTED_NAMES);
    when(mockResultSet.getLong(ModelScanMetricsReport.RESULT_DATA_FILES))
        .thenReturn(TEST_RESULT_DATA_FILES);
    when(mockResultSet.getLong(ModelScanMetricsReport.RESULT_DELETE_FILES))
        .thenReturn(TEST_RESULT_DELETE_FILES);
    when(mockResultSet.getLong(ModelScanMetricsReport.TOTAL_FILE_SIZE_BYTES))
        .thenReturn(TEST_TOTAL_FILE_SIZE);
    when(mockResultSet.getLong(ModelScanMetricsReport.TOTAL_DATA_MANIFESTS))
        .thenReturn(TEST_TOTAL_DATA_MANIFESTS);
    when(mockResultSet.getLong(ModelScanMetricsReport.TOTAL_DELETE_MANIFESTS))
        .thenReturn(TEST_TOTAL_DELETE_MANIFESTS);
    when(mockResultSet.getLong(ModelScanMetricsReport.SCANNED_DATA_MANIFESTS))
        .thenReturn(TEST_SCANNED_DATA_MANIFESTS);
    when(mockResultSet.getLong(ModelScanMetricsReport.SCANNED_DELETE_MANIFESTS))
        .thenReturn(TEST_SCANNED_DELETE_MANIFESTS);
    when(mockResultSet.getLong(ModelScanMetricsReport.SKIPPED_DATA_MANIFESTS))
        .thenReturn(TEST_SKIPPED_DATA_MANIFESTS);
    when(mockResultSet.getLong(ModelScanMetricsReport.SKIPPED_DELETE_MANIFESTS))
        .thenReturn(TEST_SKIPPED_DELETE_MANIFESTS);
    when(mockResultSet.getLong(ModelScanMetricsReport.SKIPPED_DATA_FILES))
        .thenReturn(TEST_SKIPPED_DATA_FILES);
    when(mockResultSet.getLong(ModelScanMetricsReport.SKIPPED_DELETE_FILES))
        .thenReturn(TEST_SKIPPED_DELETE_FILES);
    when(mockResultSet.getLong(ModelScanMetricsReport.TOTAL_PLANNING_DURATION_MS))
        .thenReturn(TEST_PLANNING_DURATION);
    when(mockResultSet.getLong(ModelScanMetricsReport.EQUALITY_DELETE_FILES))
        .thenReturn(TEST_EQUALITY_DELETE_FILES);
    when(mockResultSet.getLong(ModelScanMetricsReport.POSITIONAL_DELETE_FILES))
        .thenReturn(TEST_POSITIONAL_DELETE_FILES);
    when(mockResultSet.getLong(ModelScanMetricsReport.INDEXED_DELETE_FILES))
        .thenReturn(TEST_INDEXED_DELETE_FILES);
    when(mockResultSet.getLong(ModelScanMetricsReport.TOTAL_DELETE_FILE_SIZE_BYTES))
        .thenReturn(TEST_DELETE_FILE_SIZE);
    when(mockResultSet.getString(ModelScanMetricsReport.METADATA)).thenReturn(TEST_METADATA);

    ModelScanMetricsReport result = ModelScanMetricsReport.CONVERTER.fromResultSet(mockResultSet);

    assertEquals(TEST_REPORT_ID, result.getReportId());
    assertEquals(TEST_REALM_ID, result.getRealmId());
    assertEquals(TEST_CATALOG_ID, result.getCatalogId());
    assertEquals(TEST_TABLE_ID, result.getTableId());
    assertEquals(TEST_TIMESTAMP_MS, result.getTimestampMs());
    assertEquals(TEST_PRINCIPAL, result.getPrincipalName());
    assertEquals(TEST_REQUEST_ID, result.getRequestId());
    assertEquals(TEST_OTEL_TRACE_ID, result.getOtelTraceId());
    assertEquals(TEST_SNAPSHOT_ID, result.getSnapshotId());
    assertEquals(TEST_RESULT_DATA_FILES, result.getResultDataFiles());
    assertEquals(TEST_TOTAL_FILE_SIZE, result.getTotalFileSizeBytes());
    assertEquals(TEST_PLANNING_DURATION, result.getTotalPlanningDurationMs());
    assertEquals(TEST_METADATA, result.getMetadata());
  }

  @Test
  public void testToMapWithH2DatabaseType() {
    ModelScanMetricsReport report = createTestReport();

    Map<String, Object> resultMap = report.toMap(DatabaseType.H2);

    assertEquals(TEST_REPORT_ID, resultMap.get(ModelScanMetricsReport.REPORT_ID));
    assertEquals(TEST_REALM_ID, resultMap.get(ModelScanMetricsReport.REALM_ID));
    assertEquals(TEST_CATALOG_ID, resultMap.get(ModelScanMetricsReport.CATALOG_ID));
    assertEquals(TEST_TABLE_ID, resultMap.get(ModelScanMetricsReport.TABLE_ID_COL));
    assertEquals(TEST_TIMESTAMP_MS, resultMap.get(ModelScanMetricsReport.TIMESTAMP_MS));
    assertEquals(TEST_RESULT_DATA_FILES, resultMap.get(ModelScanMetricsReport.RESULT_DATA_FILES));
    assertEquals(TEST_METADATA, resultMap.get(ModelScanMetricsReport.METADATA));
  }

  @Test
  public void testToMapWithPostgresType() {
    ModelScanMetricsReport report = createTestReport();

    Map<String, Object> resultMap = report.toMap(DatabaseType.POSTGRES);

    assertEquals(TEST_REPORT_ID, resultMap.get(ModelScanMetricsReport.REPORT_ID));
    PGobject pgObject = (PGobject) resultMap.get(ModelScanMetricsReport.METADATA);
    assertEquals("jsonb", pgObject.getType());
    assertEquals(TEST_METADATA, pgObject.getValue());
  }

  @Test
  public void testConverterFromResultSet() throws SQLException {
    // Test the separate ModelScanMetricsReportConverter class (used in query methods)
    ModelScanMetricsReportConverter converter = new ModelScanMetricsReportConverter();

    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.getString(ModelScanMetricsReport.REPORT_ID)).thenReturn(TEST_REPORT_ID);
    when(mockResultSet.getString(ModelScanMetricsReport.REALM_ID)).thenReturn(TEST_REALM_ID);
    when(mockResultSet.getLong(ModelScanMetricsReport.CATALOG_ID)).thenReturn(TEST_CATALOG_ID);
    when(mockResultSet.getLong(ModelScanMetricsReport.TABLE_ID_COL)).thenReturn(TEST_TABLE_ID);
    when(mockResultSet.getLong(ModelScanMetricsReport.TIMESTAMP_MS)).thenReturn(TEST_TIMESTAMP_MS);
    when(mockResultSet.getString(ModelScanMetricsReport.PRINCIPAL_NAME)).thenReturn(TEST_PRINCIPAL);
    when(mockResultSet.getString(ModelScanMetricsReport.REQUEST_ID)).thenReturn(TEST_REQUEST_ID);
    when(mockResultSet.getString(ModelScanMetricsReport.OTEL_TRACE_ID))
        .thenReturn(TEST_OTEL_TRACE_ID);
    when(mockResultSet.getString(ModelScanMetricsReport.OTEL_SPAN_ID))
        .thenReturn(TEST_OTEL_SPAN_ID);
    when(mockResultSet.getString(ModelScanMetricsReport.REPORT_TRACE_ID))
        .thenReturn(TEST_REPORT_TRACE_ID);
    when(mockResultSet.getObject(ModelScanMetricsReport.SNAPSHOT_ID, Long.class))
        .thenReturn(TEST_SNAPSHOT_ID);
    when(mockResultSet.getObject(ModelScanMetricsReport.SCHEMA_ID, Integer.class))
        .thenReturn(TEST_SCHEMA_ID);
    when(mockResultSet.getString(ModelScanMetricsReport.FILTER_EXPRESSION)).thenReturn(TEST_FILTER);
    when(mockResultSet.getString(ModelScanMetricsReport.PROJECTED_FIELD_IDS))
        .thenReturn(TEST_PROJECTED_IDS);
    when(mockResultSet.getString(ModelScanMetricsReport.PROJECTED_FIELD_NAMES))
        .thenReturn(TEST_PROJECTED_NAMES);
    when(mockResultSet.getLong(ModelScanMetricsReport.RESULT_DATA_FILES))
        .thenReturn(TEST_RESULT_DATA_FILES);
    when(mockResultSet.getLong(ModelScanMetricsReport.RESULT_DELETE_FILES))
        .thenReturn(TEST_RESULT_DELETE_FILES);
    when(mockResultSet.getLong(ModelScanMetricsReport.TOTAL_FILE_SIZE_BYTES))
        .thenReturn(TEST_TOTAL_FILE_SIZE);
    when(mockResultSet.getLong(ModelScanMetricsReport.TOTAL_DATA_MANIFESTS))
        .thenReturn(TEST_TOTAL_DATA_MANIFESTS);
    when(mockResultSet.getLong(ModelScanMetricsReport.TOTAL_DELETE_MANIFESTS))
        .thenReturn(TEST_TOTAL_DELETE_MANIFESTS);
    when(mockResultSet.getLong(ModelScanMetricsReport.SCANNED_DATA_MANIFESTS))
        .thenReturn(TEST_SCANNED_DATA_MANIFESTS);
    when(mockResultSet.getLong(ModelScanMetricsReport.SCANNED_DELETE_MANIFESTS))
        .thenReturn(TEST_SCANNED_DELETE_MANIFESTS);
    when(mockResultSet.getLong(ModelScanMetricsReport.SKIPPED_DATA_MANIFESTS))
        .thenReturn(TEST_SKIPPED_DATA_MANIFESTS);
    when(mockResultSet.getLong(ModelScanMetricsReport.SKIPPED_DELETE_MANIFESTS))
        .thenReturn(TEST_SKIPPED_DELETE_MANIFESTS);
    when(mockResultSet.getLong(ModelScanMetricsReport.SKIPPED_DATA_FILES))
        .thenReturn(TEST_SKIPPED_DATA_FILES);
    when(mockResultSet.getLong(ModelScanMetricsReport.SKIPPED_DELETE_FILES))
        .thenReturn(TEST_SKIPPED_DELETE_FILES);
    when(mockResultSet.getLong(ModelScanMetricsReport.TOTAL_PLANNING_DURATION_MS))
        .thenReturn(TEST_PLANNING_DURATION);
    when(mockResultSet.getLong(ModelScanMetricsReport.EQUALITY_DELETE_FILES))
        .thenReturn(TEST_EQUALITY_DELETE_FILES);
    when(mockResultSet.getLong(ModelScanMetricsReport.POSITIONAL_DELETE_FILES))
        .thenReturn(TEST_POSITIONAL_DELETE_FILES);
    when(mockResultSet.getLong(ModelScanMetricsReport.INDEXED_DELETE_FILES))
        .thenReturn(TEST_INDEXED_DELETE_FILES);
    when(mockResultSet.getLong(ModelScanMetricsReport.TOTAL_DELETE_FILE_SIZE_BYTES))
        .thenReturn(TEST_DELETE_FILE_SIZE);
    when(mockResultSet.getString(ModelScanMetricsReport.METADATA)).thenReturn(TEST_METADATA);

    ModelScanMetricsReport result = converter.fromResultSet(mockResultSet);

    assertEquals(TEST_REPORT_ID, result.getReportId());
    assertEquals(TEST_REALM_ID, result.getRealmId());
    assertEquals(TEST_CATALOG_ID, result.getCatalogId());
    assertEquals(TEST_METADATA, result.getMetadata());
  }

  private ModelScanMetricsReport createTestReport() {
    return ImmutableModelScanMetricsReport.builder()
        .reportId(TEST_REPORT_ID)
        .realmId(TEST_REALM_ID)
        .catalogId(TEST_CATALOG_ID)
        .tableId(TEST_TABLE_ID)
        .timestampMs(TEST_TIMESTAMP_MS)
        .principalName(TEST_PRINCIPAL)
        .requestId(TEST_REQUEST_ID)
        .otelTraceId(TEST_OTEL_TRACE_ID)
        .snapshotId(TEST_SNAPSHOT_ID)
        .resultDataFiles(TEST_RESULT_DATA_FILES)
        .resultDeleteFiles(TEST_RESULT_DELETE_FILES)
        .totalFileSizeBytes(TEST_TOTAL_FILE_SIZE)
        .totalDataManifests(TEST_TOTAL_DATA_MANIFESTS)
        .totalDeleteManifests(TEST_TOTAL_DELETE_MANIFESTS)
        .scannedDataManifests(TEST_SCANNED_DATA_MANIFESTS)
        .scannedDeleteManifests(TEST_SCANNED_DELETE_MANIFESTS)
        .skippedDataManifests(TEST_SKIPPED_DATA_MANIFESTS)
        .skippedDeleteManifests(TEST_SKIPPED_DELETE_MANIFESTS)
        .skippedDataFiles(TEST_SKIPPED_DATA_FILES)
        .skippedDeleteFiles(TEST_SKIPPED_DELETE_FILES)
        .totalPlanningDurationMs(TEST_PLANNING_DURATION)
        .equalityDeleteFiles(TEST_EQUALITY_DELETE_FILES)
        .positionalDeleteFiles(TEST_POSITIONAL_DELETE_FILES)
        .indexedDeleteFiles(TEST_INDEXED_DELETE_FILES)
        .totalDeleteFileSizeBytes(TEST_DELETE_FILE_SIZE)
        .metadata(TEST_METADATA)
        .build();
  }
}
