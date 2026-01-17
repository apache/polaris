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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.security.Principal;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.persistence.relational.jdbc.JdbcBasePersistenceImpl;
import org.apache.polaris.persistence.relational.jdbc.models.MetricsReportConverter;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A metrics reporter that persists scan and commit reports as first-class entities in the database.
 * This provides better queryability and analytics capabilities compared to storing metrics as
 * generic events.
 *
 * <p>To enable this reporter, set the configuration:
 *
 * <pre>
 * polaris:
 *   iceberg-metrics:
 *     reporting:
 *       type: persistence
 * </pre>
 *
 * <p>Note: This reporter requires the relational-jdbc persistence backend. If a different
 * persistence backend is configured, metrics will be logged but not persisted.
 */
@ApplicationScoped
@Identifier("persistence")
public class PersistingMetricsReporter implements PolarisMetricsReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(PersistingMetricsReporter.class);

  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final RealmContext realmContext;
  private final Instance<SecurityIdentity> securityIdentityInstance;

  @Inject
  public PersistingMetricsReporter(
      MetaStoreManagerFactory metaStoreManagerFactory,
      RealmContext realmContext,
      Instance<SecurityIdentity> securityIdentityInstance) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.realmContext = realmContext;
    this.securityIdentityInstance = securityIdentityInstance;
  }

  @Override
  public void reportMetric(String catalogName, TableIdentifier table, MetricsReport metricsReport) {
    try {
      String realmId = realmContext.getRealmIdentifier();
      String catalogId = catalogName; // Using catalog name as ID for now
      String namespace = table.namespace().toString();

      // Extract principal name from security context
      String principalName = extractPrincipalName();
      String requestId = null;

      // Extract OpenTelemetry trace context from the current span
      String otelTraceId = null;
      String otelSpanId = null;
      Span currentSpan = Span.current();
      if (currentSpan != null) {
        SpanContext spanContext = currentSpan.getSpanContext();
        if (spanContext != null && spanContext.isValid()) {
          otelTraceId = spanContext.getTraceId();
          otelSpanId = spanContext.getSpanId();
          LOGGER.trace(
              "Captured OpenTelemetry context: traceId={}, spanId={}", otelTraceId, otelSpanId);
        }
      }

      // Get the persistence session for the current realm
      BasePersistence session = metaStoreManagerFactory.getOrCreateSession(realmContext);

      // Check if the session is a JdbcBasePersistenceImpl (supports metrics persistence)
      if (!(session instanceof JdbcBasePersistenceImpl jdbcPersistence)) {
        LOGGER.warn(
            "Metrics persistence is only supported with relational-jdbc backend. "
                + "Current backend: {}. Logging metrics instead.",
            session.getClass().getSimpleName());
        LOGGER.info("{}.{}: {}", catalogName, table, metricsReport);
        return;
      }

      if (metricsReport instanceof ScanReport scanReport) {
        ModelScanMetricsReport modelReport =
            MetricsReportConverter.fromScanReport(
                scanReport,
                realmId,
                catalogId,
                catalogName,
                namespace,
                principalName,
                requestId,
                otelTraceId,
                otelSpanId);
        jdbcPersistence.writeScanMetricsReport(modelReport);
        LOGGER.debug(
            "Persisted scan metrics report {} for table {}.{}",
            modelReport.getReportId(),
            catalogName,
            table);
      } else if (metricsReport instanceof CommitReport commitReport) {
        ModelCommitMetricsReport modelReport =
            MetricsReportConverter.fromCommitReport(
                commitReport,
                realmId,
                catalogId,
                catalogName,
                namespace,
                principalName,
                requestId,
                otelTraceId,
                otelSpanId);
        jdbcPersistence.writeCommitMetricsReport(modelReport);
        LOGGER.debug(
            "Persisted commit metrics report {} for table {}.{}",
            modelReport.getReportId(),
            catalogName,
            table);
      } else {
        LOGGER.warn("Unknown metrics report type: {}", metricsReport.getClass().getName());
      }
    } catch (Exception e) {
      LOGGER.error(
          "Failed to persist metrics report for table {}.{}: {}",
          catalogName,
          table,
          e.getMessage(),
          e);
    }
  }

  /**
   * Extracts the principal name from the current security context.
   *
   * @return the principal name, or null if not available
   */
  private String extractPrincipalName() {
    try {
      if (securityIdentityInstance.isResolvable()) {
        SecurityIdentity identity = securityIdentityInstance.get();
        if (identity != null && !identity.isAnonymous()) {
          Principal principal = identity.getPrincipal();
          if (principal != null) {
            return principal.getName();
          }
        }
      }
    } catch (Exception e) {
      LOGGER.trace("Could not extract principal name from security context: {}", e.getMessage());
    }
    return null;
  }
}
