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
package org.apache.polaris.service.reporting;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.persistence.relational.jdbc.JdbcBasePersistenceImpl;
import org.apache.polaris.persistence.relational.jdbc.models.MetricsReportConverter;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
import org.apache.polaris.service.context.RealmContextConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MetricsProcessor} that persists metrics to dedicated database tables.
 *
 * <p>This processor stores Iceberg metrics reports in dedicated tables:
 *
 * <ul>
 *   <li>{@code scan_metrics_report} - For ScanReport metrics
 *   <li>{@code commit_metrics_report} - For CommitReport metrics
 * </ul>
 *
 * <p>The processor includes full context information such as realm ID, catalog ID, principal name,
 * request ID, and OpenTelemetry trace context for correlation and analysis.
 *
 * <p><strong>Requirements:</strong>
 *
 * <ul>
 *   <li>Requires JDBC-based persistence backend ({@code polaris.persistence.type=relational-jdbc})
 *   <li>Database schema must include metrics tables (created via Flyway migrations)
 * </ul>
 *
 * <p>Configuration:
 *
 * <pre>
 * polaris:
 *   metrics:
 *     processor:
 *       type: persistence
 *       retention:
 *         enabled: true
 *         retention-period: P30D
 *         cleanup-interval: PT6H
 * </pre>
 */
@ApplicationScoped
@Identifier("persistence")
public class PersistenceMetricsProcessor implements MetricsProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PersistenceMetricsProcessor.class);

  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final RealmContextConfiguration realmContextConfiguration;

  @Inject
  public PersistenceMetricsProcessor(
      MetaStoreManagerFactory metaStoreManagerFactory,
      RealmContextConfiguration realmContextConfiguration) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.realmContextConfiguration = realmContextConfiguration;
    LOGGER.info("PersistenceMetricsProcessor initialized - metrics will be persisted to database");
  }

  @Override
  public void process(MetricsProcessingContext context) {
    try {
      // Get the persistence session for the realm
      String realmId = context.realmId();
      RealmContext realmContext = () -> realmId;
      BasePersistence session = metaStoreManagerFactory.getOrCreateSession(realmContext);

      // Only JDBC persistence supports metrics tables
      if (!(session instanceof JdbcBasePersistenceImpl jdbcPersistence)) {
        LOGGER.warn(
            "Persistence metrics processor requires JDBC persistence backend. "
                + "Current backend: {}. Metrics will not be persisted.",
            session.getClass().getSimpleName());
        return;
      }

      // Persist based on report type
      if (context.metricsReport() instanceof ScanReport scanReport) {
        persistScanReport(jdbcPersistence, context, scanReport);
      } else if (context.metricsReport() instanceof CommitReport commitReport) {
        persistCommitReport(jdbcPersistence, context, commitReport);
      } else {
        LOGGER.warn(
            "Unknown metrics report type: {}. Metrics will not be persisted.",
            context.metricsReport().getClass().getName());
      }
    } catch (Exception e) {
      LOGGER.error(
          "Failed to persist metrics for {}.{}: {}",
          context.catalogName(),
          context.tableIdentifier(),
          e.getMessage(),
          e);
    }
  }

  private void persistScanReport(
      JdbcBasePersistenceImpl jdbcPersistence,
      MetricsProcessingContext context,
      ScanReport scanReport) {
    try {
      String namespace = context.tableIdentifier().namespace().toString();
      String catalogId = context.catalogId().map(String::valueOf).orElse(null);

      ModelScanMetricsReport modelReport =
          MetricsReportConverter.fromScanReport(
              scanReport,
              context.realmId(),
              catalogId,
              context.catalogName(),
              namespace,
              context.principalName().orElse(null),
              context.requestId().orElse(null),
              context.otelTraceId().orElse(null),
              context.otelSpanId().orElse(null));

      jdbcPersistence.writeScanMetricsReport(modelReport);
      LOGGER.debug(
          "Persisted scan metrics for {}.{}", context.catalogName(), context.tableIdentifier());
    } catch (Exception e) {
      LOGGER.error("Failed to persist scan metrics: {}", e.getMessage(), e);
    }
  }

  private void persistCommitReport(
      JdbcBasePersistenceImpl jdbcPersistence,
      MetricsProcessingContext context,
      CommitReport commitReport) {
    try {
      String namespace = context.tableIdentifier().namespace().toString();
      String catalogId = context.catalogId().map(String::valueOf).orElse(null);

      ModelCommitMetricsReport modelReport =
          MetricsReportConverter.fromCommitReport(
              commitReport,
              context.realmId(),
              catalogId,
              context.catalogName(),
              namespace,
              context.principalName().orElse(null),
              context.requestId().orElse(null),
              context.otelTraceId().orElse(null),
              context.otelSpanId().orElse(null));

      jdbcPersistence.writeCommitMetricsReport(modelReport);
      LOGGER.debug(
          "Persisted commit metrics for {}.{}", context.catalogName(), context.tableIdentifier());
    } catch (Exception e) {
      LOGGER.error("Failed to persist commit metrics: {}", e.getMessage(), e);
    }
  }
}
