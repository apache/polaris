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

import static org.apache.polaris.persistence.relational.jdbc.QueryGenerator.PreparedQuery;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.MetricsQueryCriteria;
import org.apache.polaris.core.persistence.metrics.ReportIdToken;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.pagination.Token;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC implementation of {@link MetricsPersistence}.
 *
 * <p>This class provides direct JDBC persistence for metrics reports, converting between SPI record
 * types ({@link ScanMetricsRecord}, {@link CommitMetricsRecord}) and JDBC model types ({@link
 * ModelScanMetricsReport}, {@link ModelCommitMetricsReport}).
 *
 * <p>This implementation assumes that metrics tables exist. The producer ({@link
 * JdbcMetricsPersistenceProducer}) checks for metrics table availability at startup and returns
 * {@link MetricsPersistence#NOOP} if tables are not present.
 */
public class JdbcMetricsPersistence implements MetricsPersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMetricsPersistence.class);

  private final DatasourceOperations datasourceOperations;
  private final String realmId;
  private final PolarisPrincipal polarisPrincipal;
  private final RequestIdSupplier requestIdSupplier;

  /**
   * Creates a new JdbcMetricsPersistence instance.
   *
   * @param datasourceOperations the datasource operations for JDBC access
   * @param realmId the realm ID for multi-tenancy
   * @param polarisPrincipal the authenticated principal for the current request
   * @param requestIdSupplier supplier for obtaining the request ID
   */
  public JdbcMetricsPersistence(
      DatasourceOperations datasourceOperations,
      String realmId,
      PolarisPrincipal polarisPrincipal,
      RequestIdSupplier requestIdSupplier) {
    this.datasourceOperations = datasourceOperations;
    this.realmId = realmId;
    this.polarisPrincipal = polarisPrincipal;
    this.requestIdSupplier = requestIdSupplier;
  }

  @Override
  public void writeScanReport(@Nonnull ScanMetricsRecord record) {
    // Obtain request context fields
    String principalName = polarisPrincipal != null ? polarisPrincipal.getName() : null;
    String requestId = requestIdSupplier.getRequestId();
    String otelTraceId = null;
    String otelSpanId = null;

    // Get OpenTelemetry context if available
    SpanContext spanContext = Span.current().getSpanContext();
    if (spanContext.isValid()) {
      otelTraceId = spanContext.getTraceId();
      otelSpanId = spanContext.getSpanId();
    }

    ModelScanMetricsReport model =
        SpiModelConverter.toModelScanReport(
            record, realmId, principalName, requestId, otelTraceId, otelSpanId);
    writeScanMetricsReport(model);
  }

  @Override
  public void writeCommitReport(@Nonnull CommitMetricsRecord record) {
    // Obtain request context fields
    String principalName = polarisPrincipal != null ? polarisPrincipal.getName() : null;
    String requestId = requestIdSupplier.getRequestId();
    String otelTraceId = null;
    String otelSpanId = null;

    // Get OpenTelemetry context if available
    SpanContext spanContext = Span.current().getSpanContext();
    if (spanContext.isValid()) {
      otelTraceId = spanContext.getTraceId();
      otelSpanId = spanContext.getSpanId();
    }

    ModelCommitMetricsReport model =
        SpiModelConverter.toModelCommitReport(
            record, realmId, principalName, requestId, otelTraceId, otelSpanId);
    writeCommitMetricsReport(model);
  }

  @Override
  @Nonnull
  public Page<ScanMetricsRecord> queryScanReports(
      @Nonnull MetricsQueryCriteria criteria, @Nonnull PageToken pageToken) {
    // catalogId and tableId are required for queries
    if (criteria.catalogId().isEmpty() || criteria.tableId().isEmpty()) {
      return Page.fromItems(List.of());
    }

    int limit = pageToken.pageSize().orElse(100);
    Long startTimeMs = criteria.startTime().map(t -> t.toEpochMilli()).orElse(null);
    Long endTimeMs = criteria.endTime().map(t -> t.toEpochMilli()).orElse(null);

    // Extract composite cursor from page token if present
    // The cursor is (timestamp_ms, report_id) for chronological pagination
    var cursorToken = pageToken.valueAs(ReportIdToken.class);
    Long cursorTimestampMs = cursorToken.map(ReportIdToken::timestampMs).orElse(null);
    String cursorReportId = cursorToken.map(ReportIdToken::reportId).orElse(null);

    List<ModelScanMetricsReport> models =
        queryScanMetricsReports(
            criteria.catalogId().getAsLong(),
            criteria.tableId().getAsLong(),
            startTimeMs,
            endTimeMs,
            cursorTimestampMs,
            cursorReportId,
            limit);

    List<ScanMetricsRecord> records =
        models.stream().map(SpiModelConverter::toScanMetricsRecord).collect(Collectors.toList());

    // Build continuation token only when we might have more pages
    Token nextToken = records.size() >= limit ? ReportIdToken.fromRecord(records.getLast()) : null;

    return Page.page(pageToken, records, nextToken);
  }

  @Override
  @Nonnull
  public Page<CommitMetricsRecord> queryCommitReports(
      @Nonnull MetricsQueryCriteria criteria, @Nonnull PageToken pageToken) {
    // catalogId and tableId are required for queries
    if (criteria.catalogId().isEmpty() || criteria.tableId().isEmpty()) {
      return Page.fromItems(List.of());
    }

    int limit = pageToken.pageSize().orElse(100);
    Long startTimeMs = criteria.startTime().map(t -> t.toEpochMilli()).orElse(null);
    Long endTimeMs = criteria.endTime().map(t -> t.toEpochMilli()).orElse(null);

    // Extract composite cursor from page token if present
    // The cursor is (timestamp_ms, report_id) for chronological pagination
    var cursorToken = pageToken.valueAs(ReportIdToken.class);
    Long cursorTimestampMs = cursorToken.map(ReportIdToken::timestampMs).orElse(null);
    String cursorReportId = cursorToken.map(ReportIdToken::reportId).orElse(null);

    List<ModelCommitMetricsReport> models =
        queryCommitMetricsReports(
            criteria.catalogId().getAsLong(),
            criteria.tableId().getAsLong(),
            startTimeMs,
            endTimeMs,
            cursorTimestampMs,
            cursorReportId,
            limit);

    List<CommitMetricsRecord> records =
        models.stream().map(SpiModelConverter::toCommitMetricsRecord).collect(Collectors.toList());

    // Build continuation token only when we might have more pages
    Token nextToken = records.size() >= limit ? ReportIdToken.fromRecord(records.getLast()) : null;

    return Page.page(pageToken, records, nextToken);
  }

  // ========== Internal JDBC methods ==========

  /**
   * Writes a scan metrics report to the database.
   *
   * <p>This operation is idempotent - writing the same reportId twice has no effect. The primary
   * key (realm_id, report_id) constraint is used with ON CONFLICT DO NOTHING to ensure idempotency.
   *
   * @param report the scan metrics report to persist
   */
  void writeScanMetricsReport(@Nonnull ModelScanMetricsReport report) {
    try {
      PreparedQuery pq =
          buildIdempotentInsertQuery(
              ModelScanMetricsReport.ALL_COLUMNS,
              ModelScanMetricsReport.TABLE_NAME,
              report.toMap(datasourceOperations.getDatabaseType()).values().stream().toList(),
              datasourceOperations.getDatabaseType());
      // Note: updated may be 0 if the report already exists (idempotent insert)
      datasourceOperations.executeUpdate(pq);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to write scan metrics report due to %s", e.getMessage()), e);
    }
  }

  /**
   * Writes a commit metrics report to the database.
   *
   * <p>This operation is idempotent - writing the same reportId twice has no effect. The primary
   * key (realm_id, report_id) constraint is used with ON CONFLICT DO NOTHING to ensure idempotency.
   *
   * @param report the commit metrics report to persist
   */
  void writeCommitMetricsReport(@Nonnull ModelCommitMetricsReport report) {
    try {
      PreparedQuery pq =
          buildIdempotentInsertQuery(
              ModelCommitMetricsReport.ALL_COLUMNS,
              ModelCommitMetricsReport.TABLE_NAME,
              report.toMap(datasourceOperations.getDatabaseType()).values().stream().toList(),
              datasourceOperations.getDatabaseType());
      // Note: updated may be 0 if the report already exists (idempotent insert)
      datasourceOperations.executeUpdate(pq);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to write commit metrics report due to %s", e.getMessage()), e);
    }
  }

  /**
   * Builds an idempotent INSERT query that ignores conflicts on the primary key.
   *
   * <p>This ensures that duplicate reportIds are silently ignored, fulfilling the idempotency
   * contract of the {@link MetricsPersistence} interface.
   *
   * @param columns the column names
   * @param tableName the table name
   * @param values the values to insert
   * @param databaseType the database type (for dialect-specific syntax)
   * @return a PreparedQuery with the idempotent INSERT statement
   */
  private static PreparedQuery buildIdempotentInsertQuery(
      List<String> columns, String tableName, List<Object> values, DatabaseType databaseType) {
    String columnList = String.join(", ", columns);
    String placeholders = columns.stream().map(c -> "?").collect(Collectors.joining(", "));

    String sql;
    if (databaseType == DatabaseType.H2) {
      // H2 uses MERGE INTO for idempotent inserts, but INSERT ... ON CONFLICT also works in newer
      // versions
      // Using standard SQL MERGE syntax for H2 compatibility
      sql =
          "MERGE INTO "
              + QueryGenerator.getFullyQualifiedTableName(tableName)
              + " ("
              + columnList
              + ") KEY (realm_id, report_id) VALUES ("
              + placeholders
              + ")";
    } else {
      // PostgreSQL: Use INSERT ... ON CONFLICT DO NOTHING
      sql =
          "INSERT INTO "
              + QueryGenerator.getFullyQualifiedTableName(tableName)
              + " ("
              + columnList
              + ") VALUES ("
              + placeholders
              + ") ON CONFLICT (realm_id, report_id) DO NOTHING";
    }
    return new PreparedQuery(sql, values);
  }

  /**
   * Retrieves scan metrics reports for a specific table within a time range.
   *
   * @param catalogId the catalog entity ID
   * @param tableId the table entity ID
   * @param startTimeMs start of time range (inclusive), or null for no lower bound
   * @param endTimeMs end of time range (exclusive), or null for no upper bound
   * @param cursorTimestampMs cursor timestamp for pagination: return results after this timestamp,
   *     or null for first page
   * @param cursorReportId cursor report ID for pagination: used as tie-breaker for same-timestamp
   *     entries, or null for first page
   * @param limit maximum number of results to return
   * @return list of scan metrics reports matching the criteria
   */
  @Nonnull
  List<ModelScanMetricsReport> queryScanMetricsReports(
      long catalogId,
      long tableId,
      @Nullable Long startTimeMs,
      @Nullable Long endTimeMs,
      @Nullable Long cursorTimestampMs,
      @Nullable String cursorReportId,
      int limit) {
    try {
      StringBuilder whereClause = new StringBuilder();
      whereClause.append("realm_id = ? AND catalog_id = ? AND table_id = ?");
      List<Object> values = new ArrayList<>(List.of(realmId, catalogId, tableId));

      if (startTimeMs != null) {
        whereClause.append(" AND timestamp_ms >= ?");
        values.add(startTimeMs);
      }
      if (endTimeMs != null) {
        whereClause.append(" AND timestamp_ms < ?");
        values.add(endTimeMs);
      }
      // Composite cursor: (timestamp_ms, report_id) > (cursorTs, cursorId)
      // This is equivalent to: timestamp_ms > cursorTs OR (timestamp_ms = cursorTs AND report_id >
      // cursorId)
      if (cursorTimestampMs != null && cursorReportId != null) {
        whereClause.append(" AND (timestamp_ms > ? OR (timestamp_ms = ? AND report_id > ?))");
        values.add(cursorTimestampMs);
        values.add(cursorTimestampMs);
        values.add(cursorReportId);
      }

      // Order by (timestamp_ms, report_id) for chronological pagination.
      // timestamp_ms provides chronological order, report_id breaks ties.
      String sql =
          "SELECT * FROM "
              + QueryGenerator.getFullyQualifiedTableName(ModelScanMetricsReport.TABLE_NAME)
              + " WHERE "
              + whereClause
              + " ORDER BY timestamp_ms ASC, report_id ASC LIMIT "
              + limit;

      PreparedQuery query = new PreparedQuery(sql, values);
      var results = datasourceOperations.executeSelect(query, ModelScanMetricsReport.CONVERTER);
      return results == null ? Collections.emptyList() : results;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to query scan metrics reports due to %s", e.getMessage()), e);
    }
  }

  /**
   * Retrieves commit metrics reports for a specific table within a time range.
   *
   * @param catalogId the catalog entity ID
   * @param tableId the table entity ID
   * @param startTimeMs start of time range (inclusive), or null for no lower bound
   * @param endTimeMs end of time range (exclusive), or null for no upper bound
   * @param cursorTimestampMs cursor timestamp for pagination: return results after this timestamp,
   *     or null for first page
   * @param cursorReportId cursor report ID for pagination: used as tie-breaker for same-timestamp
   *     entries, or null for first page
   * @param limit maximum number of results to return
   * @return list of commit metrics reports matching the criteria
   */
  @Nonnull
  List<ModelCommitMetricsReport> queryCommitMetricsReports(
      long catalogId,
      long tableId,
      @Nullable Long startTimeMs,
      @Nullable Long endTimeMs,
      @Nullable Long cursorTimestampMs,
      @Nullable String cursorReportId,
      int limit) {
    try {
      List<Object> values = new ArrayList<>(List.of(realmId, catalogId, tableId));

      StringBuilder whereClause = new StringBuilder();
      whereClause.append("realm_id = ? AND catalog_id = ? AND table_id = ?");

      if (startTimeMs != null) {
        whereClause.append(" AND timestamp_ms >= ?");
        values.add(startTimeMs);
      }
      if (endTimeMs != null) {
        whereClause.append(" AND timestamp_ms < ?");
        values.add(endTimeMs);
      }
      // Composite cursor: (timestamp_ms, report_id) > (cursorTs, cursorId)
      // This is equivalent to: timestamp_ms > cursorTs OR (timestamp_ms = cursorTs AND report_id >
      // cursorId)
      if (cursorTimestampMs != null && cursorReportId != null) {
        whereClause.append(" AND (timestamp_ms > ? OR (timestamp_ms = ? AND report_id > ?))");
        values.add(cursorTimestampMs);
        values.add(cursorTimestampMs);
        values.add(cursorReportId);
      }

      // Order by (timestamp_ms, report_id) for chronological pagination.
      // timestamp_ms provides chronological order, report_id breaks ties.
      String sql =
          "SELECT * FROM "
              + QueryGenerator.getFullyQualifiedTableName(ModelCommitMetricsReport.TABLE_NAME)
              + " WHERE "
              + whereClause
              + " ORDER BY timestamp_ms ASC, report_id ASC LIMIT "
              + limit;

      PreparedQuery query = new PreparedQuery(sql, values);
      var results = datasourceOperations.executeSelect(query, ModelCommitMetricsReport.CONVERTER);
      return results == null ? Collections.emptyList() : results;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to query commit metrics reports due to %s", e.getMessage()), e);
    }
  }

  /**
   * Retrieves scan metrics reports by OpenTelemetry trace ID.
   *
   * @param traceId the OpenTelemetry trace ID
   * @return list of scan metrics reports with the given trace ID
   */
  @Nonnull
  public List<ModelScanMetricsReport> queryScanMetricsReportsByTraceId(@Nonnull String traceId) {
    try {
      String sql =
          "SELECT * FROM "
              + QueryGenerator.getFullyQualifiedTableName(ModelScanMetricsReport.TABLE_NAME)
              + " WHERE realm_id = ? AND otel_trace_id = ? ORDER BY timestamp_ms DESC";

      PreparedQuery query = new PreparedQuery(sql, List.of(realmId, traceId));
      var results = datasourceOperations.executeSelect(query, ModelScanMetricsReport.CONVERTER);
      return results == null ? Collections.emptyList() : results;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to query scan metrics reports by trace ID due to %s", e.getMessage()),
          e);
    }
  }

  /**
   * Retrieves commit metrics reports by OpenTelemetry trace ID.
   *
   * @param traceId the OpenTelemetry trace ID
   * @return list of commit metrics reports with the given trace ID
   */
  @Nonnull
  public List<ModelCommitMetricsReport> queryCommitMetricsReportsByTraceId(
      @Nonnull String traceId) {
    try {
      String sql =
          "SELECT * FROM "
              + QueryGenerator.getFullyQualifiedTableName(ModelCommitMetricsReport.TABLE_NAME)
              + " WHERE realm_id = ? AND otel_trace_id = ? ORDER BY timestamp_ms DESC";

      PreparedQuery query = new PreparedQuery(sql, List.of(realmId, traceId));
      var results = datasourceOperations.executeSelect(query, ModelCommitMetricsReport.CONVERTER);
      return results == null ? Collections.emptyList() : results;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to query commit metrics reports by trace ID due to %s", e.getMessage()),
          e);
    }
  }

  /**
   * Deletes scan metrics reports older than the specified timestamp.
   *
   * @param olderThanMs timestamp in milliseconds; reports with timestamp_ms less than this will be
   *     deleted
   * @return the number of reports deleted
   */
  public int deleteScanMetricsReportsOlderThan(long olderThanMs) {
    try {
      String sql =
          "DELETE FROM "
              + QueryGenerator.getFullyQualifiedTableName(ModelScanMetricsReport.TABLE_NAME)
              + " WHERE realm_id = ? AND timestamp_ms < ?";

      PreparedQuery query = new PreparedQuery(sql, List.of(realmId, olderThanMs));
      return datasourceOperations.executeUpdate(query);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to delete old scan metrics reports due to %s", e.getMessage()), e);
    }
  }

  /**
   * Deletes commit metrics reports older than the specified timestamp.
   *
   * @param olderThanMs timestamp in milliseconds; reports with timestamp_ms less than this will be
   *     deleted
   * @return the number of reports deleted
   */
  public int deleteCommitMetricsReportsOlderThan(long olderThanMs) {
    try {
      String sql =
          "DELETE FROM "
              + QueryGenerator.getFullyQualifiedTableName(ModelCommitMetricsReport.TABLE_NAME)
              + " WHERE realm_id = ? AND timestamp_ms < ?";

      PreparedQuery query = new PreparedQuery(sql, List.of(realmId, olderThanMs));
      return datasourceOperations.executeUpdate(query);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to delete old commit metrics reports due to %s", e.getMessage()),
          e);
    }
  }

  /**
   * Deletes all metrics reports (both scan and commit) older than the specified timestamp.
   *
   * @param olderThanMs timestamp in milliseconds; reports with timestamp_ms less than this will be
   *     deleted
   * @return the total number of reports deleted (scan + commit)
   */
  public int deleteAllMetricsReportsOlderThan(long olderThanMs) {
    int scanDeleted = deleteScanMetricsReportsOlderThan(olderThanMs);
    int commitDeleted = deleteCommitMetricsReportsOlderThan(olderThanMs);
    return scanDeleted + commitDeleted;
  }
}
