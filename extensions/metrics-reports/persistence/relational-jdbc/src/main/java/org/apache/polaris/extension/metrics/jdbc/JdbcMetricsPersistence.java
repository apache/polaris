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
import java.util.stream.Collectors;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.MetricsRecordIdentity;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.extension.metrics.jdbc.pagination.MetricsReportToken;
import org.apache.polaris.extension.metrics.spi.MetricsQuerySpi;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
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
  public Page<? extends MetricsRecordIdentity> listReports(
      @NonNull MetricType metricType,
      long catalogId,
      @NonNull List<Long> tableIds,
      @Nullable Long snapshotId,
      @Nullable Long timestampFrom,
      @Nullable Long timestampTo,
      @NonNull PageToken pageToken) {
    String realmId = realmContext.getRealmIdentifier();
    String scope =
        scopeKey(realmId, metricType, catalogId, tableIds, snapshotId, timestampFrom, timestampTo);
    try {
      if (metricType == MetricType.COMMIT) {
        PreparedQuery query =
            buildMetricsQuery(
                ModelCommitMetricsReport.TABLE_NAME,
                realmId,
                catalogId,
                tableIds,
                snapshotId,
                timestampFrom,
                timestampTo,
                pageToken,
                scope);
        List<ModelCommitMetricsReport> rows =
            datasourceOperations.executeSelect(query, ModelCommitMetricsReport.CONVERTER);
        return Page.mapped(
            pageToken,
            rows.stream().map(ModelCommitMetricsReport::toRecord),
            Function.<CommitMetricsRecord>identity(),
            last -> MetricsReportToken.fromRecord(last, scope));
      }

      PreparedQuery query =
          buildMetricsQuery(
              ModelScanMetricsReport.TABLE_NAME,
              realmId,
              catalogId,
              tableIds,
              snapshotId,
              timestampFrom,
              timestampTo,
              pageToken,
              scope);
      List<ModelScanMetricsReport> rows =
          datasourceOperations.executeSelect(query, ModelScanMetricsReport.CONVERTER);
      return Page.mapped(
          pageToken,
          rows.stream().map(ModelScanMetricsReport::toRecord),
          Function.<ScanMetricsRecord>identity(),
          last -> MetricsReportToken.fromRecord(last, scope));
    } catch (SQLException e) {
      throw new RuntimeException("Failed to list metrics reports: " + e.getMessage(), e);
    }
  }

  /**
   * Builds a stable fingerprint of the query scope (realm, metric type, catalog, tables, and
   * filters). Used to reject a pagination cursor that was produced by a different query,
   * including one issued against a different realm.
   */
  private static String scopeKey(
      String realmId,
      MetricType metricType,
      long catalogId,
      List<Long> tableIds,
      @Nullable Long snapshotId,
      @Nullable Long timestampFrom,
      @Nullable Long timestampTo) {
    List<Long> sortedTableIds = tableIds.stream().sorted().collect(Collectors.toList());
    return realmId
        + "|"
        + metricType
        + "|"
        + catalogId
        + "|"
        + sortedTableIds
        + "|"
        + snapshotId
        + "|"
        + timestampFrom
        + "|"
        + timestampTo;
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
      List<Long> tableIds,
      @Nullable Long snapshotId,
      @Nullable Long timestampFrom,
      @Nullable Long timestampTo,
      PageToken pageToken,
      String scope) {
    StringBuilder sql = new StringBuilder("SELECT * FROM ");
    sql.append(QueryGenerator.getFullyQualifiedTableName(tableName));
    sql.append(" WHERE realm_id = ? AND catalog_id = ? AND table_id IN (");
    sql.append(tableIds.stream().map(id -> "?").collect(Collectors.joining(", ")));
    sql.append(")");

    List<Object> params = new ArrayList<>();
    params.add(realmId);
    params.add(catalogId);
    params.addAll(tableIds);

    if (snapshotId != null) {
      sql.append(" AND snapshot_id = ?");
      params.add(snapshotId);
    }
    if (timestampFrom != null) {
      sql.append(" AND timestamp_ms >= ?");
      params.add(timestampFrom);
    }
    if (timestampTo != null) {
      sql.append(" AND timestamp_ms < ?");
      params.add(timestampTo);
    }

    if (pageToken.value().isPresent() && pageToken.valueAs(MetricsReportToken.class).isEmpty()) {
      throw new IllegalArgumentException(
          "pageToken contains a cursor of an unexpected type; expected MetricsReportToken");
    }
    if (pageToken.paginationRequested()) {
      pageToken
          .valueAs(MetricsReportToken.class)
          .ifPresent(
              cursor -> {
                if (!cursor.scope().equals(scope)) {
                  throw new IllegalArgumentException(
                      "pageToken cursor does not match the current query scope");
                }
                sql.append(" AND (timestamp_ms < ? OR (timestamp_ms = ? AND report_id < ?))");
                params.add(cursor.timestampMs());
                params.add(cursor.timestampMs());
                params.add(cursor.reportId());
              });
    }

    sql.append(" ORDER BY timestamp_ms DESC, report_id DESC");

    if (pageToken.paginationRequested()) {
      int limit = pageToken.pageSize().orElse(100);
      sql.append(" LIMIT ?");
      params.add(limit + 1);
    }

    return new PreparedQuery(sql.toString(), params);
  }
}
