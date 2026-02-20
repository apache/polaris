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
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.time.Instant;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.polaris.core.metrics.iceberg.MetricsRecordConverter;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.service.tracing.RequestIdFilter;
import org.jboss.resteasy.reactive.server.core.CurrentRequestManager;
import org.jboss.resteasy.reactive.server.core.ResteasyReactiveRequestContext;
import org.jboss.resteasy.reactive.server.jaxrs.ContainerRequestContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link PolarisMetricsReporter} that persists metrics to the configured {@link
 * MetricsPersistence} backend.
 *
 * <p>This reporter is selected when {@code polaris.iceberg-metrics.reporting.type} is set to {@code
 * "persisting"}.
 *
 * <p>The reporter receives catalog and table IDs from the caller (already resolved during
 * authorization), avoiding redundant entity lookups. It uses {@link MetricsRecordConverter} to
 * convert Iceberg metrics reports to SPI records before persisting them.
 *
 * @see PolarisMetricsReporter
 * @see MetricsPersistence
 * @see MetricsRecordConverter
 */
@RequestScoped
@Identifier("persisting")
public class PersistingMetricsReporter implements PolarisMetricsReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PersistingMetricsReporter.class);

  private final MetricsPersistence metricsPersistence;
  private final SecurityContext securityContext;

  @Inject
  public PersistingMetricsReporter(
      MetricsPersistence metricsPersistence, SecurityContext securityContext) {
    this.metricsPersistence = metricsPersistence;
    this.securityContext = securityContext;
  }

  @Override
  public void reportMetric(
      String catalogName,
      long catalogId,
      TableIdentifier table,
      long tableId,
      MetricsReport metricsReport,
      Instant receivedTimestamp) {

    // Get request context for correlation fields
    String principalName = getPrincipalName();
    String requestId = getRequestId();
    String otelTraceId = null;
    String otelSpanId = null;

    // Get OpenTelemetry context if available
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
          "Unknown metrics report type: {}. Metrics will not be stored.",
          metricsReport.getClass().getName());
    }
  }

  /** Gets the principal name from the security context, or null if not available. */
  private String getPrincipalName() {
    if (securityContext != null) {
      Principal principal = securityContext.getUserPrincipal();
      if (principal != null) {
        return principal.getName();
      }
    }
    return null;
  }

  /**
   * Gets the request ID from the current request context.
   *
   * <p>Uses the RESTEasy Reactive context to access request properties.
   */
  private String getRequestId() {
    ResteasyReactiveRequestContext context = CurrentRequestManager.get();
    if (context != null) {
      ContainerRequestContextImpl request = context.getContainerRequestContext();
      if (request != null) {
        return (String) request.getProperty(RequestIdFilter.REQUEST_ID_KEY);
      }
    }
    return null;
  }
}
