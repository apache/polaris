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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.MetricsQueryCriteria;
import org.apache.polaris.core.persistence.metrics.ReportIdToken;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.pagination.Token;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReportConverter;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReportConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC implementation of {@link MetricsPersistence}.
 *
 * <p>This class provides direct JDBC persistence for metrics reports, converting between SPI record
 * types ({@link ScanMetricsRecord}, {@link CommitMetricsRecord}) and JDBC model types ({@link
 * ModelScanMetricsReport}, {@link ModelCommitMetricsReport}).
 *
 * <p>Metrics tables (scan_metrics_report, commit_metrics_report) were introduced in schema version
 * 4. On older schemas, all operations are no-ops.
 */
public class JdbcMetricsPersistence implements MetricsPersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMetricsPersistence.class);

  // Minimum schema version that includes metrics tables
  private static final int METRICS_TABLES_MIN_SCHEMA_VERSION = 4;

  private final DatasourceOperations datasourceOperations;
  private final String realmId;
  private final int schemaVersion;

  /**
   * Creates a new JdbcMetricsPersistence instance.
   *
   * @param datasourceOperations the datasource operations for JDBC access
   * @param realmId the realm ID for multi-tenancy
   * @param schemaVersion the current schema version
   */
  public JdbcMetricsPersistence(
      DatasourceOperations datasourceOperations, String realmId, int schemaVersion) {
    this.datasourceOperations = datasourceOperations;
    this.realmId = realmId;
    this.schemaVersion = schemaVersion;
  }

  /**
   * Returns true if the current schema version supports metrics persistence tables.
   *
   * @return true if schema version >= 4, false otherwise
   */
  public boolean supportsMetricsPersistence() {
    return this.schemaVersion >= METRICS_TABLES_MIN_SCHEMA_VERSION;
  }

  @Override
  public void writeScanReport(@Nonnull ScanMetricsRecord record) {
    if (!supportsMetricsPersistence()) {
      LOGGER.debug(
          "Schema version {} does not support metrics tables. Skipping scan metrics write.",
          schemaVersion);
      return;
    }
    ModelScanMetricsReport model = SpiModelConverter.toModelScanReport(record, realmId);
    writeScanMetricsReport(model);
  }

  @Override
  public void writeCommitReport(@Nonnull CommitMetricsRecord record) {
    if (!supportsMetricsPersistence()) {
      LOGGER.debug(
          "Schema version {} does not support metrics tables. Skipping commit metrics write.",
          schemaVersion);
      return;
    }
    ModelCommitMetricsReport model = SpiModelConverter.toModelCommitReport(record, realmId);
    writeCommitMetricsReport(model);
  }

  @Override
  @Nonnull
  public Page<ScanMetricsRecord> queryScanReports(
      @Nonnull MetricsQueryCriteria criteria, @Nonnull PageToken pageToken) {
    if (!supportsMetricsPersistence()) {
      return Page.fromItems(List.of());
    }

    // catalogId and tableId are required for queries
    if (criteria.catalogId().isEmpty() || criteria.tableId().isEmpty()) {
      return Page.fromItems(List.of());
    }

    int limit = pageToken.pageSize().orElse(100);
    Long startTimeMs = criteria.startTime().map(t -> t.toEpochMilli()).orElse(null);
    Long endTimeMs = criteria.endTime().map(t -> t.toEpochMilli()).orElse(null);

    // Extract cursor from page token if present
    String lastReportId =
        pageToken.valueAs(ReportIdToken.class).map(ReportIdToken::reportId).orElse(null);

    List<ModelScanMetricsReport> models =
        queryScanMetricsReports(
            criteria.catalogId().getAsLong(),
            criteria.tableId().getAsLong(),
            startTimeMs,
            endTimeMs,
            lastReportId,
            limit);

    List<ScanMetricsRecord> records =
        models.stream().map(SpiModelConverter::toScanMetricsRecord).collect(Collectors.toList());

    // Build continuation token if we have results (there may be more pages)
    Token nextToken =
        records.isEmpty() ? null : ReportIdToken.fromReportId(records.getLast().reportId());

    return Page.page(pageToken, records, nextToken);
  }

  @Override
  @Nonnull
  public Page<CommitMetricsRecord> queryCommitReports(
      @Nonnull MetricsQueryCriteria criteria, @Nonnull PageToken pageToken) {
    if (!supportsMetricsPersistence()) {
      return Page.fromItems(List.of());
    }

    // catalogId and tableId are required for queries
    if (criteria.catalogId().isEmpty() || criteria.tableId().isEmpty()) {
      return Page.fromItems(List.of());
    }

    int limit = pageToken.pageSize().orElse(100);
    Long startTimeMs = criteria.startTime().map(t -> t.toEpochMilli()).orElse(null);
    Long endTimeMs = criteria.endTime().map(t -> t.toEpochMilli()).orElse(null);

    // Extract cursor from page token if present
    String lastReportId =
        pageToken.valueAs(ReportIdToken.class).map(ReportIdToken::reportId).orElse(null);

    List<ModelCommitMetricsReport> models =
        queryCommitMetricsReports(
            criteria.catalogId().getAsLong(),
            criteria.tableId().getAsLong(),
            startTimeMs,
            endTimeMs,
            lastReportId,
            limit);

    List<CommitMetricsRecord> records =
        models.stream().map(SpiModelConverter::toCommitMetricsRecord).collect(Collectors.toList());

    // Build continuation token if we have results (there may be more pages)
    Token nextToken =
        records.isEmpty() ? null : ReportIdToken.fromReportId(records.getLast().reportId());

    return Page.page(pageToken, records, nextToken);
  }

  // ========== Internal JDBC methods ==========

  /**
   * Writes a scan metrics report to the database.
   *
   * @param report the scan metrics report to persist
   */
  void writeScanMetricsReport(@Nonnull ModelScanMetricsReport report) {
    try {
      PreparedQuery pq =
          QueryGenerator.generateInsertQueryWithoutRealmId(
              ModelScanMetricsReport.ALL_COLUMNS,
              ModelScanMetricsReport.TABLE_NAME,
              report.toMap(datasourceOperations.getDatabaseType()).values().stream().toList());
      int updated = datasourceOperations.executeUpdate(pq);
      if (updated == 0) {
        throw new SQLException("Scan metrics report was not inserted.");
      }
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to write scan metrics report due to %s", e.getMessage()), e);
    }
  }

  /**
   * Writes a commit metrics report to the database.
   *
   * @param report the commit metrics report to persist
   */
  void writeCommitMetricsReport(@Nonnull ModelCommitMetricsReport report) {
    try {
      PreparedQuery pq =
          QueryGenerator.generateInsertQueryWithoutRealmId(
              ModelCommitMetricsReport.ALL_COLUMNS,
              ModelCommitMetricsReport.TABLE_NAME,
              report.toMap(datasourceOperations.getDatabaseType()).values().stream().toList());
      int updated = datasourceOperations.executeUpdate(pq);
      if (updated == 0) {
        throw new SQLException("Commit metrics report was not inserted.");
      }
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to write commit metrics report due to %s", e.getMessage()), e);
    }
  }

  /**
   * Retrieves scan metrics reports for a specific table within a time range.
   *
   * @param catalogId the catalog entity ID
   * @param tableId the table entity ID
   * @param startTimeMs start of time range (inclusive), or null for no lower bound
   * @param endTimeMs end of time range (exclusive), or null for no upper bound
   * @param lastReportId cursor for pagination: return results after this report ID, or null for
   *     first page
   * @param limit maximum number of results to return
   * @return list of scan metrics reports matching the criteria
   */
  @Nonnull
  List<ModelScanMetricsReport> queryScanMetricsReports(
      long catalogId,
      long tableId,
      @Nullable Long startTimeMs,
      @Nullable Long endTimeMs,
      @Nullable String lastReportId,
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
      if (lastReportId != null) {
        whereClause.append(" AND report_id > ?");
        values.add(lastReportId);
      }

      String sql =
          "SELECT * FROM "
              + QueryGenerator.getFullyQualifiedTableName(ModelScanMetricsReport.TABLE_NAME)
              + " WHERE "
              + whereClause
              + " ORDER BY report_id ASC LIMIT "
              + limit;

      PreparedQuery query = new PreparedQuery(sql, values);
      var results =
          datasourceOperations.executeSelect(query, new ModelScanMetricsReportConverter());
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
   * @param lastReportId cursor for pagination: return results after this report ID, or null for
   *     first page
   * @param limit maximum number of results to return
   * @return list of commit metrics reports matching the criteria
   */
  @Nonnull
  List<ModelCommitMetricsReport> queryCommitMetricsReports(
      long catalogId,
      long tableId,
      @Nullable Long startTimeMs,
      @Nullable Long endTimeMs,
      @Nullable String lastReportId,
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
      if (lastReportId != null) {
        whereClause.append(" AND report_id > ?");
        values.add(lastReportId);
      }

      String sql =
          "SELECT * FROM "
              + QueryGenerator.getFullyQualifiedTableName(ModelCommitMetricsReport.TABLE_NAME)
              + " WHERE "
              + whereClause
              + " ORDER BY report_id ASC LIMIT "
              + limit;

      PreparedQuery query = new PreparedQuery(sql, values);
      var results =
          datasourceOperations.executeSelect(query, new ModelCommitMetricsReportConverter());
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
    if (!supportsMetricsPersistence()) {
      return Collections.emptyList();
    }
    try {
      String sql =
          "SELECT * FROM "
              + QueryGenerator.getFullyQualifiedTableName(ModelScanMetricsReport.TABLE_NAME)
              + " WHERE realm_id = ? AND otel_trace_id = ? ORDER BY timestamp_ms DESC";

      PreparedQuery query = new PreparedQuery(sql, List.of(realmId, traceId));
      var results =
          datasourceOperations.executeSelect(query, new ModelScanMetricsReportConverter());
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
    if (!supportsMetricsPersistence()) {
      return Collections.emptyList();
    }
    try {
      String sql =
          "SELECT * FROM "
              + QueryGenerator.getFullyQualifiedTableName(ModelCommitMetricsReport.TABLE_NAME)
              + " WHERE realm_id = ? AND otel_trace_id = ? ORDER BY timestamp_ms DESC";

      PreparedQuery query = new PreparedQuery(sql, List.of(realmId, traceId));
      var results =
          datasourceOperations.executeSelect(query, new ModelCommitMetricsReportConverter());
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
   * @return the number of reports deleted, or 0 if schema version < 4
   */
  public int deleteScanMetricsReportsOlderThan(long olderThanMs) {
    if (!supportsMetricsPersistence()) {
      return 0;
    }
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
   * @return the number of reports deleted, or 0 if schema version < 4
   */
  public int deleteCommitMetricsReportsOlderThan(long olderThanMs) {
    if (!supportsMetricsPersistence()) {
      return 0;
    }
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
   * @return the total number of reports deleted (scan + commit), or 0 if schema version < 4
   */
  public int deleteAllMetricsReportsOlderThan(long olderThanMs) {
    int scanDeleted = deleteScanMetricsReportsOlderThan(olderThanMs);
    int commitDeleted = deleteCommitMetricsReportsOlderThan(olderThanMs);
    return scanDeleted + commitDeleted;
  }
}
