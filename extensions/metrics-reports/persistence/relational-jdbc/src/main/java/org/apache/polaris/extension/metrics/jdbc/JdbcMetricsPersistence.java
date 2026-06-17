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
package org.apache.polaris.extension.metrics.jdbc;

import static org.apache.polaris.persistence.relational.jdbc.QueryGenerator.PreparedQuery;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.MetricsQuerySpi;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.pagination.MetricsReportToken;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * JDBC implementation of {@link MetricsPersistence}.
 *
 * <p>Writes and reads scan/commit metrics reports using the shared {@link DatasourceOperations}
 * connection pool. The metrics tables ({@code SCAN_METRICS_REPORT}, {@code COMMIT_METRICS_REPORT})
 * are part of the standard JDBC schema (schema-v4).
 *
 * <p>This bean is contributed by the {@code polaris-extensions-metrics-reports-jdbc} extension
 * module. When this module is on the classpath the {@code "persisting"} reporter wires to it; when
 * absent the reporter falls back to no-op behavior.
 */
@ApplicationScoped
public class JdbcMetricsPersistence implements MetricsPersistence, MetricsQuerySpi {

  private final DatasourceOperations datasourceOperations;
  private final RealmContext realmContext;

  @Inject
  public JdbcMetricsPersistence(
      DatasourceOperations datasourceOperations, RealmContext realmContext) {
    this.datasourceOperations = datasourceOperations;
    this.realmContext = realmContext;
  }

  @Override
  public void writeScanReport(@NonNull ScanMetricsRecord record) {
    String realmId = realmContext.getRealmIdentifier();
    ModelScanMetricsReport model = ModelScanMetricsReport.fromRecord(record, realmId);
    PreparedQuery pq =
        QueryGenerator.generateInsertQuery(
            ModelScanMetricsReport.ALL_COLUMNS,
            ModelScanMetricsReport.TABLE_NAME,
            model.toMap(datasourceOperations.getDatabaseType()).values().stream().toList(),
            realmId);
    try {
      datasourceOperations.executeUpdate(pq);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to write scan metrics report: " + e.getMessage(), e);
    }
  }

  @Override
  public void writeCommitReport(@NonNull CommitMetricsRecord record) {
    String realmId = realmContext.getRealmIdentifier();
    ModelCommitMetricsReport model = ModelCommitMetricsReport.fromRecord(record, realmId);
    PreparedQuery pq =
        QueryGenerator.generateInsertQuery(
            ModelCommitMetricsReport.ALL_COLUMNS,
            ModelCommitMetricsReport.TABLE_NAME,
            model.toMap(datasourceOperations.getDatabaseType()).values().stream().toList(),
            realmId);
    try {
      datasourceOperations.executeUpdate(pq);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to write commit metrics report: " + e.getMessage(), e);
    }
  }

  @Override
  public Page<ScanMetricsRecord> listScanReports(
      long catalogId,
      long tableId,
      @Nullable Long snapshotId,
      @Nullable String principalName,
      @Nullable Long timestampFrom,
      @Nullable Long timestampTo,
      @NonNull PageToken pageToken) {
    String realmId = realmContext.getRealmIdentifier();
    try {
      PreparedQuery query =
          buildMetricsQuery(
              ModelScanMetricsReport.TABLE_NAME,
              realmId,
              catalogId,
              tableId,
              snapshotId,
              principalName,
              timestampFrom,
              timestampTo,
              pageToken);
      List<ModelScanMetricsReport> rows =
          datasourceOperations.executeSelect(query, ModelScanMetricsReport.CONVERTER);
      return Page.mapped(
          pageToken,
          rows.stream().map(ModelScanMetricsReport::toRecord),
          Function.identity(),
          MetricsReportToken::fromRecord);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to list scan metrics reports: " + e.getMessage(), e);
    }
  }

  @Override
  public Page<CommitMetricsRecord> listCommitReports(
      long catalogId,
      long tableId,
      @Nullable Long snapshotId,
      @Nullable String principalName,
      @Nullable Long timestampFrom,
      @Nullable Long timestampTo,
      @NonNull PageToken pageToken) {
    String realmId = realmContext.getRealmIdentifier();
    try {
      PreparedQuery query =
          buildMetricsQuery(
              ModelCommitMetricsReport.TABLE_NAME,
              realmId,
              catalogId,
              tableId,
              snapshotId,
              principalName,
              timestampFrom,
              timestampTo,
              pageToken);
      List<ModelCommitMetricsReport> rows =
          datasourceOperations.executeSelect(query, ModelCommitMetricsReport.CONVERTER);
      return Page.mapped(
          pageToken,
          rows.stream().map(ModelCommitMetricsReport::toRecord),
          Function.identity(),
          MetricsReportToken::fromRecord);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to list commit metrics reports: " + e.getMessage(), e);
    }
  }

  /**
   * Builds a parameterized SELECT query for a metrics report table using keyset pagination.
   *
   * <p>Rows are ordered by {@code (timestamp_ms DESC, report_id DESC)}. The cursor from {@link
   * MetricsReportToken} drives the keyset predicate: {@code (timestamp_ms < cursorTs) OR
   * (timestamp_ms = cursorTs AND report_id < cursorId)}.
   */
  private PreparedQuery buildMetricsQuery(
      String tableName,
      String realmId,
      long catalogId,
      long tableId,
      @Nullable Long snapshotId,
      @Nullable String principalName,
      @Nullable Long timestampFrom,
      @Nullable Long timestampTo,
      PageToken pageToken) {
    StringBuilder sql = new StringBuilder("SELECT * FROM ");
    sql.append(QueryGenerator.getFullyQualifiedTableName(tableName));
    sql.append(" WHERE realm_id = ? AND catalog_id = ? AND table_id = ?");

    List<Object> params = new ArrayList<>();
    params.add(realmId);
    params.add(catalogId);
    params.add(tableId);

    if (snapshotId != null) {
      sql.append(" AND snapshot_id = ?");
      params.add(snapshotId);
    }
    if (principalName != null) {
      sql.append(" AND principal_name = ?");
      params.add(principalName);
    }
    if (timestampFrom != null) {
      sql.append(" AND timestamp_ms >= ?");
      params.add(timestampFrom);
    }
    if (timestampTo != null) {
      sql.append(" AND timestamp_ms < ?");
      params.add(timestampTo);
    }

    if (pageToken.paginationRequested()) {
      if (pageToken.value().isPresent() && pageToken.valueAs(MetricsReportToken.class).isEmpty()) {
        throw new IllegalArgumentException(
            "pageToken contains a cursor of an unexpected type; expected MetricsReportToken");
      }
      pageToken
          .valueAs(MetricsReportToken.class)
          .ifPresent(
              cursor -> {
                sql.append(" AND (timestamp_ms < ? OR (timestamp_ms = ? AND report_id < ?))");
                params.add(cursor.timestampMs());
                params.add(cursor.timestampMs());
                params.add(cursor.reportId());
              });
    }

    sql.append(" ORDER BY timestamp_ms DESC, report_id DESC");

    int limit = pageToken.pageSize().orElse(100);
    sql.append(" LIMIT ?");
    params.add(limit + 1);

    return new PreparedQuery(sql.toString(), params);
  }
}
