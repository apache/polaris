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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Instant;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RequestIdSupplier;
import org.apache.polaris.core.metrics.IcebergMetricsReporter;
import org.apache.polaris.core.metrics.iceberg.MetricsRecordConverter;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persisting implementation of {@link IcebergMetricsReporter}.
 *
 * <p>Selected when {@code polaris.iceberg-metrics.reporting.type} is set to {@code "persisting"}.
 * Requires the {@code polaris-extensions-metrics-reports-jdbc} extension module on the classpath to
 * provide the {@link JdbcMetricsPersistence} bean.
 *
 * <p>Converts Iceberg {@link ScanReport}/{@link CommitReport} objects to backend-agnostic SPI
 * records via {@link MetricsRecordConverter}, then delegates to {@link MetricsPersistence} for
 * durable storage.
 */
@RequestScoped
@Identifier("persisting")
public class PersistingMetricsReporter implements IcebergMetricsReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(PersistingMetricsReporter.class);

  private final JdbcMetricsPersistence metricsPersistence;
  private final Instance<PolarisPrincipal> polarisPrincipal;
  private final Instance<RequestIdSupplier> requestIdSupplier;

  @Inject
  public PersistingMetricsReporter(
      JdbcMetricsPersistence metricsPersistence,
      Instance<PolarisPrincipal> polarisPrincipal,
      Instance<RequestIdSupplier> requestIdSupplier) {
    this.metricsPersistence = metricsPersistence;
    this.polarisPrincipal = polarisPrincipal;
    this.requestIdSupplier = requestIdSupplier;
  }

  @Override
  public void reportMetric(
      String catalogName,
      long catalogId,
      TableIdentifier table,
      long tableId,
      MetricsReport metricsReport,
      Instant receivedTimestamp) {

    String principalName = resolvePrincipalName();
    String requestId = resolveRequestId();
    String otelTraceId = null;
    String otelSpanId = null;

    SpanContext spanContext = Span.current().getSpanContext();
    if (spanContext.isValid()) {
      otelTraceId = spanContext.getTraceId();
      otelSpanId = spanContext.getSpanId();
    }

    if (metricsReport instanceof ScanReport scanReport) {
      ScanMetricsRecord record =
          MetricsRecordConverter.forScanReport(scanReport)
              .catalogId(catalogId)
              .tableId(tableId)
              .timestamp(receivedTimestamp)
              .principalName(principalName)
              .requestId(requestId)
              .otelTraceId(otelTraceId)
              .otelSpanId(otelSpanId)
              .build();
      metricsPersistence.writeScanReport(record);
      LOGGER.debug(
          "Persisted scan metrics for {}.{} (reportId={})", catalogName, table, record.reportId());
    } else if (metricsReport instanceof CommitReport commitReport) {
      CommitMetricsRecord record =
          MetricsRecordConverter.forCommitReport(commitReport)
              .catalogId(catalogId)
              .tableId(tableId)
              .timestamp(receivedTimestamp)
              .principalName(principalName)
              .requestId(requestId)
              .otelTraceId(otelTraceId)
              .otelSpanId(otelSpanId)
              .build();
      metricsPersistence.writeCommitReport(record);
      LOGGER.debug(
          "Persisted commit metrics for {}.{} (reportId={})",
          catalogName,
          table,
          record.reportId());
    } else {
      LOGGER.warn(
          "Unknown metrics report type: {}. Report will not be stored.",
          metricsReport.getClass().getName());
    }
  }

  private String resolvePrincipalName() {
    if (polarisPrincipal.isResolvable()) {
      PolarisPrincipal p = polarisPrincipal.get();
      return p != null ? p.getName() : null;
    }
    return null;
  }

  private String resolveRequestId() {
    if (requestIdSupplier.isResolvable()) {
      RequestIdSupplier s = requestIdSupplier.get();
      return s != null ? s.getRequestId() : null;
    }
    return null;
  }
}
