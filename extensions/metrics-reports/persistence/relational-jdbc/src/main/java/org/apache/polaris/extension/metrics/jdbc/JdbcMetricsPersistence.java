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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.SQLException;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator.PreparedQuery;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
import org.jspecify.annotations.NonNull;

/**
 * JDBC implementation of {@link MetricsPersistence}.
 *
 * <p>Writes scan/commit metrics reports using the shared {@link DatasourceOperations} connection
 * pool. The metrics tables ({@code SCAN_METRICS_REPORT}, {@code COMMIT_METRICS_REPORT}) are defined
 * in the relational-jdbc schema.
 *
 * <p>This bean is contributed by the {@code polaris-extensions-metrics-reports-jdbc} extension
 * module. When this module is on the classpath the {@code "persisting"} reporter wires to it; when
 * it is absent, the default (logging) reporter is used and no metrics persistence bean exists.
 */
@ApplicationScoped
public class JdbcMetricsPersistence implements MetricsPersistence {

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
}
