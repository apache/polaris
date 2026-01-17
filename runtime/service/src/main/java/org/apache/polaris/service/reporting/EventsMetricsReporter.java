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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A metrics reporter that persists scan and commit reports to the events table as JSON. This
 * provides a unified audit trail where metrics are stored alongside other catalog events.
 *
 * <p>To enable this reporter, set the configuration:
 *
 * <pre>
 * polaris:
 *   iceberg-metrics:
 *     reporting:
 *       type: events
 * </pre>
 *
 * <p>Or use it as part of a composite reporter:
 *
 * <pre>
 * polaris:
 *   iceberg-metrics:
 *     reporting:
 *       type: composite
 *       targets:
 *         - events
 *         - persistence
 * </pre>
 */
@ApplicationScoped
@Identifier("events")
public class EventsMetricsReporter implements PolarisMetricsReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventsMetricsReporter.class);

  public static final String EVENT_TYPE_SCAN_REPORT = "ScanReport";
  public static final String EVENT_TYPE_COMMIT_REPORT = "CommitReport";

  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final RealmContext realmContext;
  private final ObjectMapper objectMapper;
  private final Instance<SecurityIdentity> securityIdentityInstance;

  @Inject
  public EventsMetricsReporter(
      MetaStoreManagerFactory metaStoreManagerFactory,
      RealmContext realmContext,
      ObjectMapper objectMapper,
      Instance<SecurityIdentity> securityIdentityInstance) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.realmContext = realmContext;
    this.objectMapper = objectMapper;
    this.securityIdentityInstance = securityIdentityInstance;
  }

  @Override
  public void reportMetric(String catalogName, TableIdentifier table, MetricsReport metricsReport) {
    try {
      String eventType;
      PolarisEvent.ResourceType resourceType = PolarisEvent.ResourceType.TABLE;
      String resourceIdentifier = table.toString();

      if (metricsReport instanceof ScanReport) {
        eventType = EVENT_TYPE_SCAN_REPORT;
      } else if (metricsReport instanceof CommitReport) {
        eventType = EVENT_TYPE_COMMIT_REPORT;
      } else {
        LOGGER.warn("Unknown metrics report type: {}", metricsReport.getClass().getName());
        return;
      }

      // Extract principal name from security context
      String principalName = extractPrincipalName();

      // Extract OpenTelemetry trace context
      String otelTraceId = null;
      String otelSpanId = null;
      Span currentSpan = Span.current();
      if (currentSpan != null) {
        SpanContext spanContext = currentSpan.getSpanContext();
        if (spanContext != null && spanContext.isValid()) {
          otelTraceId = spanContext.getTraceId();
          otelSpanId = spanContext.getSpanId();
        }
      }

      // Serialize the metrics report and add trace context to additional properties
      Map<String, Object> additionalProps = new HashMap<>();
      additionalProps.put("metricsReport", serializeMetricsReportToMap(metricsReport));
      if (otelTraceId != null) {
        additionalProps.put("otelTraceId", otelTraceId);
      }
      if (otelSpanId != null) {
        additionalProps.put("otelSpanId", otelSpanId);
      }
      String additionalPropsJson = serializeToJson(additionalProps);

      PolarisEvent event =
          new PolarisEvent(
              catalogName,
              UUID.randomUUID().toString(),
              null, // requestId - could be extracted from context if available
              eventType,
              System.currentTimeMillis(),
              principalName,
              resourceType,
              resourceIdentifier);
      event.setAdditionalProperties(additionalPropsJson);

      // Get the persistence session for the current realm and write the event
      BasePersistence session = metaStoreManagerFactory.getOrCreateSession(realmContext);
      session.writeEvents(List.of(event));

      LOGGER.debug("Persisted {} event for table {}.{}", eventType, catalogName, table);
    } catch (Exception e) {
      LOGGER.error(
          "Failed to persist metrics event for table {}.{}: {}",
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

  private Object serializeMetricsReportToMap(MetricsReport metricsReport) {
    try {
      String json = objectMapper.writeValueAsString(metricsReport);
      return objectMapper.readValue(json, Object.class);
    } catch (JsonProcessingException e) {
      LOGGER.warn("Failed to serialize metrics report: {}", e.getMessage());
      return Map.of();
    }
  }

  private String serializeToJson(Object obj) {
    try {
      return objectMapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      LOGGER.warn("Failed to serialize to JSON: {}", e.getMessage());
      return "{}";
    }
  }
}
