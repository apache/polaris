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

public class ModelCommitMetricsReportTest {

  private static final String TEST_REPORT_ID = "commit-report-123";
  private static final String TEST_REALM_ID = "realm-1";
  private static final long TEST_CATALOG_ID = 12345L;
  private static final long TEST_TABLE_ID = 67890L;
  private static final long TEST_TIMESTAMP_MS = 1704067200000L;
  private static final String TEST_PRINCIPAL = "user@example.com";
  private static final String TEST_REQUEST_ID = "req-456";
  private static final String TEST_OTEL_TRACE_ID = "trace-789";
  private static final String TEST_OTEL_SPAN_ID = "span-012";
  private static final String TEST_REPORT_TRACE_ID = "report-trace-345";
  private static final long TEST_SNAPSHOT_ID = 987654321L;
  private static final Long TEST_SEQUENCE_NUMBER = 5L;
  private static final String TEST_OPERATION = "append";
  private static final long TEST_ADDED_DATA_FILES = 10L;
  private static final long TEST_REMOVED_DATA_FILES = 2L;
  private static final long TEST_TOTAL_DATA_FILES = 50L;
  private static final long TEST_ADDED_DELETE_FILES = 1L;
  private static final long TEST_REMOVED_DELETE_FILES = 0L;
  private static final long TEST_TOTAL_DELETE_FILES = 3L;
  private static final long TEST_ADDED_EQUALITY_DELETE_FILES = 1L;
  private static final long TEST_REMOVED_EQUALITY_DELETE_FILES = 0L;
  private static final long TEST_ADDED_POSITIONAL_DELETE_FILES = 0L;
  private static final long TEST_REMOVED_POSITIONAL_DELETE_FILES = 0L;
  private static final long TEST_ADDED_RECORDS = 1000L;
  private static final long TEST_REMOVED_RECORDS = 50L;
  private static final long TEST_TOTAL_RECORDS = 10000L;
  private static final long TEST_ADDED_FILE_SIZE = 1024000L;
  private static final long TEST_REMOVED_FILE_SIZE = 51200L;
  private static final long TEST_TOTAL_FILE_SIZE = 10240000L;
  private static final long TEST_TOTAL_DURATION = 250L;
  private static final int TEST_ATTEMPTS = 1;
  private static final String TEST_METADATA = "{\"commit\":\"info\"}";

  @Test
  public void testFromResultSet() throws SQLException {
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.getString(ModelCommitMetricsReport.REPORT_ID)).thenReturn(TEST_REPORT_ID);
    when(mockResultSet.getString(ModelCommitMetricsReport.REALM_ID)).thenReturn(TEST_REALM_ID);
    when(mockResultSet.getLong(ModelCommitMetricsReport.CATALOG_ID)).thenReturn(TEST_CATALOG_ID);
    when(mockResultSet.getLong(ModelCommitMetricsReport.TABLE_ID_COL)).thenReturn(TEST_TABLE_ID);
    when(mockResultSet.getLong(ModelCommitMetricsReport.TIMESTAMP_MS))
        .thenReturn(TEST_TIMESTAMP_MS);
    when(mockResultSet.getString(ModelCommitMetricsReport.PRINCIPAL_NAME))
        .thenReturn(TEST_PRINCIPAL);
    when(mockResultSet.getString(ModelCommitMetricsReport.REQUEST_ID)).thenReturn(TEST_REQUEST_ID);
    when(mockResultSet.getString(ModelCommitMetricsReport.OTEL_TRACE_ID))
        .thenReturn(TEST_OTEL_TRACE_ID);
    when(mockResultSet.getString(ModelCommitMetricsReport.OTEL_SPAN_ID))
        .thenReturn(TEST_OTEL_SPAN_ID);
    when(mockResultSet.getString(ModelCommitMetricsReport.REPORT_TRACE_ID))
        .thenReturn(TEST_REPORT_TRACE_ID);
    when(mockResultSet.getLong(ModelCommitMetricsReport.SNAPSHOT_ID)).thenReturn(TEST_SNAPSHOT_ID);
    when(mockResultSet.getObject(ModelCommitMetricsReport.SEQUENCE_NUMBER, Long.class))
        .thenReturn(TEST_SEQUENCE_NUMBER);
    when(mockResultSet.getString(ModelCommitMetricsReport.OPERATION)).thenReturn(TEST_OPERATION);
    when(mockResultSet.getLong(ModelCommitMetricsReport.ADDED_DATA_FILES))
        .thenReturn(TEST_ADDED_DATA_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.REMOVED_DATA_FILES))
        .thenReturn(TEST_REMOVED_DATA_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.TOTAL_DATA_FILES))
        .thenReturn(TEST_TOTAL_DATA_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.ADDED_DELETE_FILES))
        .thenReturn(TEST_ADDED_DELETE_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.REMOVED_DELETE_FILES))
        .thenReturn(TEST_REMOVED_DELETE_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.TOTAL_DELETE_FILES))
        .thenReturn(TEST_TOTAL_DELETE_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.ADDED_EQUALITY_DELETE_FILES))
        .thenReturn(TEST_ADDED_EQUALITY_DELETE_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.REMOVED_EQUALITY_DELETE_FILES))
        .thenReturn(TEST_REMOVED_EQUALITY_DELETE_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.ADDED_POSITIONAL_DELETE_FILES))
        .thenReturn(TEST_ADDED_POSITIONAL_DELETE_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.REMOVED_POSITIONAL_DELETE_FILES))
        .thenReturn(TEST_REMOVED_POSITIONAL_DELETE_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.ADDED_RECORDS))
        .thenReturn(TEST_ADDED_RECORDS);
    when(mockResultSet.getLong(ModelCommitMetricsReport.REMOVED_RECORDS))
        .thenReturn(TEST_REMOVED_RECORDS);
    when(mockResultSet.getLong(ModelCommitMetricsReport.TOTAL_RECORDS))
        .thenReturn(TEST_TOTAL_RECORDS);
    when(mockResultSet.getLong(ModelCommitMetricsReport.ADDED_FILE_SIZE_BYTES))
        .thenReturn(TEST_ADDED_FILE_SIZE);
    when(mockResultSet.getLong(ModelCommitMetricsReport.REMOVED_FILE_SIZE_BYTES))
        .thenReturn(TEST_REMOVED_FILE_SIZE);
    when(mockResultSet.getLong(ModelCommitMetricsReport.TOTAL_FILE_SIZE_BYTES))
        .thenReturn(TEST_TOTAL_FILE_SIZE);
    when(mockResultSet.getLong(ModelCommitMetricsReport.TOTAL_DURATION_MS))
        .thenReturn(TEST_TOTAL_DURATION);
    when(mockResultSet.getInt(ModelCommitMetricsReport.ATTEMPTS)).thenReturn(TEST_ATTEMPTS);
    when(mockResultSet.getString(ModelCommitMetricsReport.METADATA)).thenReturn(TEST_METADATA);

    ModelCommitMetricsReport result =
        ModelCommitMetricsReport.CONVERTER.fromResultSet(mockResultSet);

    assertEquals(TEST_REPORT_ID, result.getReportId());
    assertEquals(TEST_REALM_ID, result.getRealmId());
    assertEquals(TEST_CATALOG_ID, result.getCatalogId());
    assertEquals(TEST_TABLE_ID, result.getTableId());
    assertEquals(TEST_TIMESTAMP_MS, result.getTimestampMs());
    assertEquals(TEST_SNAPSHOT_ID, result.getSnapshotId());
    assertEquals(TEST_OPERATION, result.getOperation());
    assertEquals(TEST_ADDED_DATA_FILES, result.getAddedDataFiles());
    assertEquals(TEST_ADDED_RECORDS, result.getAddedRecords());
    assertEquals(TEST_TOTAL_DURATION, result.getTotalDurationMs());
    assertEquals(TEST_ATTEMPTS, result.getAttempts());
    assertEquals(TEST_METADATA, result.getMetadata());
  }

  @Test
  public void testToMapWithH2DatabaseType() {
    ModelCommitMetricsReport report = createTestReport();

    Map<String, Object> resultMap = report.toMap(DatabaseType.H2);

    assertEquals(TEST_REPORT_ID, resultMap.get(ModelCommitMetricsReport.REPORT_ID));
    assertEquals(TEST_REALM_ID, resultMap.get(ModelCommitMetricsReport.REALM_ID));
    assertEquals(TEST_SNAPSHOT_ID, resultMap.get(ModelCommitMetricsReport.SNAPSHOT_ID));
    assertEquals(TEST_OPERATION, resultMap.get(ModelCommitMetricsReport.OPERATION));
    assertEquals(TEST_ADDED_DATA_FILES, resultMap.get(ModelCommitMetricsReport.ADDED_DATA_FILES));
    assertEquals(TEST_METADATA, resultMap.get(ModelCommitMetricsReport.METADATA));
  }

  @Test
  public void testToMapWithPostgresType() {
    ModelCommitMetricsReport report = createTestReport();

    Map<String, Object> resultMap = report.toMap(DatabaseType.POSTGRES);

    assertEquals(TEST_REPORT_ID, resultMap.get(ModelCommitMetricsReport.REPORT_ID));
    PGobject pgObject = (PGobject) resultMap.get(ModelCommitMetricsReport.METADATA);
    assertEquals("jsonb", pgObject.getType());
    assertEquals(TEST_METADATA, pgObject.getValue());
  }

  @Test
  public void testConverterFromResultSet() throws SQLException {
    // Test the separate ModelCommitMetricsReportConverter class (used in query methods)
    ModelCommitMetricsReportConverter converter = new ModelCommitMetricsReportConverter();

    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.getString(ModelCommitMetricsReport.REPORT_ID)).thenReturn(TEST_REPORT_ID);
    when(mockResultSet.getString(ModelCommitMetricsReport.REALM_ID)).thenReturn(TEST_REALM_ID);
    when(mockResultSet.getLong(ModelCommitMetricsReport.CATALOG_ID)).thenReturn(TEST_CATALOG_ID);
    when(mockResultSet.getLong(ModelCommitMetricsReport.TABLE_ID_COL)).thenReturn(TEST_TABLE_ID);
    when(mockResultSet.getLong(ModelCommitMetricsReport.TIMESTAMP_MS))
        .thenReturn(TEST_TIMESTAMP_MS);
    when(mockResultSet.getString(ModelCommitMetricsReport.PRINCIPAL_NAME))
        .thenReturn(TEST_PRINCIPAL);
    when(mockResultSet.getString(ModelCommitMetricsReport.REQUEST_ID)).thenReturn(TEST_REQUEST_ID);
    when(mockResultSet.getString(ModelCommitMetricsReport.OTEL_TRACE_ID))
        .thenReturn(TEST_OTEL_TRACE_ID);
    when(mockResultSet.getString(ModelCommitMetricsReport.OTEL_SPAN_ID))
        .thenReturn(TEST_OTEL_SPAN_ID);
    when(mockResultSet.getString(ModelCommitMetricsReport.REPORT_TRACE_ID))
        .thenReturn(TEST_REPORT_TRACE_ID);
    when(mockResultSet.getObject(ModelCommitMetricsReport.SNAPSHOT_ID, Long.class))
        .thenReturn(TEST_SNAPSHOT_ID);
    when(mockResultSet.getObject(ModelCommitMetricsReport.SEQUENCE_NUMBER, Long.class))
        .thenReturn(TEST_SEQUENCE_NUMBER);
    when(mockResultSet.getString(ModelCommitMetricsReport.OPERATION)).thenReturn(TEST_OPERATION);
    when(mockResultSet.getLong(ModelCommitMetricsReport.ADDED_DATA_FILES))
        .thenReturn(TEST_ADDED_DATA_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.REMOVED_DATA_FILES))
        .thenReturn(TEST_REMOVED_DATA_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.TOTAL_DATA_FILES))
        .thenReturn(TEST_TOTAL_DATA_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.ADDED_DELETE_FILES))
        .thenReturn(TEST_ADDED_DELETE_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.REMOVED_DELETE_FILES))
        .thenReturn(TEST_REMOVED_DELETE_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.TOTAL_DELETE_FILES))
        .thenReturn(TEST_TOTAL_DELETE_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.ADDED_EQUALITY_DELETE_FILES))
        .thenReturn(TEST_ADDED_EQUALITY_DELETE_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.REMOVED_EQUALITY_DELETE_FILES))
        .thenReturn(TEST_REMOVED_EQUALITY_DELETE_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.ADDED_POSITIONAL_DELETE_FILES))
        .thenReturn(TEST_ADDED_POSITIONAL_DELETE_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.REMOVED_POSITIONAL_DELETE_FILES))
        .thenReturn(TEST_REMOVED_POSITIONAL_DELETE_FILES);
    when(mockResultSet.getLong(ModelCommitMetricsReport.ADDED_RECORDS))
        .thenReturn(TEST_ADDED_RECORDS);
    when(mockResultSet.getLong(ModelCommitMetricsReport.REMOVED_RECORDS))
        .thenReturn(TEST_REMOVED_RECORDS);
    when(mockResultSet.getLong(ModelCommitMetricsReport.TOTAL_RECORDS))
        .thenReturn(TEST_TOTAL_RECORDS);
    when(mockResultSet.getLong(ModelCommitMetricsReport.ADDED_FILE_SIZE_BYTES))
        .thenReturn(TEST_ADDED_FILE_SIZE);
    when(mockResultSet.getLong(ModelCommitMetricsReport.REMOVED_FILE_SIZE_BYTES))
        .thenReturn(TEST_REMOVED_FILE_SIZE);
    when(mockResultSet.getLong(ModelCommitMetricsReport.TOTAL_FILE_SIZE_BYTES))
        .thenReturn(TEST_TOTAL_FILE_SIZE);
    when(mockResultSet.getObject(ModelCommitMetricsReport.TOTAL_DURATION_MS, Long.class))
        .thenReturn(TEST_TOTAL_DURATION);
    when(mockResultSet.getObject(ModelCommitMetricsReport.ATTEMPTS, Integer.class))
        .thenReturn(TEST_ATTEMPTS);
    when(mockResultSet.getString(ModelCommitMetricsReport.METADATA)).thenReturn(TEST_METADATA);

    ModelCommitMetricsReport result = converter.fromResultSet(mockResultSet);

    assertEquals(TEST_REPORT_ID, result.getReportId());
    assertEquals(TEST_REALM_ID, result.getRealmId());
    assertEquals(TEST_CATALOG_ID, result.getCatalogId());
    assertEquals(TEST_METADATA, result.getMetadata());
  }

  private ModelCommitMetricsReport createTestReport() {
    return ImmutableModelCommitMetricsReport.builder()
        .reportId(TEST_REPORT_ID)
        .realmId(TEST_REALM_ID)
        .catalogId(TEST_CATALOG_ID)
        .tableId(TEST_TABLE_ID)
        .timestampMs(TEST_TIMESTAMP_MS)
        .principalName(TEST_PRINCIPAL)
        .requestId(TEST_REQUEST_ID)
        .otelTraceId(TEST_OTEL_TRACE_ID)
        .snapshotId(TEST_SNAPSHOT_ID)
        .sequenceNumber(TEST_SEQUENCE_NUMBER)
        .operation(TEST_OPERATION)
        .addedDataFiles(TEST_ADDED_DATA_FILES)
        .removedDataFiles(TEST_REMOVED_DATA_FILES)
        .totalDataFiles(TEST_TOTAL_DATA_FILES)
        .addedDeleteFiles(TEST_ADDED_DELETE_FILES)
        .removedDeleteFiles(TEST_REMOVED_DELETE_FILES)
        .totalDeleteFiles(TEST_TOTAL_DELETE_FILES)
        .addedEqualityDeleteFiles(TEST_ADDED_EQUALITY_DELETE_FILES)
        .removedEqualityDeleteFiles(TEST_REMOVED_EQUALITY_DELETE_FILES)
        .addedPositionalDeleteFiles(TEST_ADDED_POSITIONAL_DELETE_FILES)
        .removedPositionalDeleteFiles(TEST_REMOVED_POSITIONAL_DELETE_FILES)
        .addedRecords(TEST_ADDED_RECORDS)
        .removedRecords(TEST_REMOVED_RECORDS)
        .totalRecords(TEST_TOTAL_RECORDS)
        .addedFileSizeBytes(TEST_ADDED_FILE_SIZE)
        .removedFileSizeBytes(TEST_REMOVED_FILE_SIZE)
        .totalFileSizeBytes(TEST_TOTAL_FILE_SIZE)
        .totalDurationMs(TEST_TOTAL_DURATION)
        .attempts(TEST_ATTEMPTS)
        .metadata(TEST_METADATA)
        .build();
  }
}
