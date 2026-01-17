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

package org.apache.polaris.service.events.listeners;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;

public abstract class PolarisPersistenceEventListener implements PolarisEventListener {

  @Override
  public void onEvent(PolarisEvent event) {
    switch (event.type()) {
      case AFTER_CREATE_TABLE -> handleAfterCreateTable(event);
      case AFTER_CREATE_CATALOG -> handleAfterCreateCatalog(event);
      case AFTER_REPORT_METRICS -> handleAfterReportMetrics(event);
      default -> {
        // Other events not handled by this listener
      }
    }
  }

  private void handleAfterCreateTable(PolarisEvent event) {
    LoadTableResponse loadTableResponse =
        event.attributes().getRequired(EventAttributes.LOAD_TABLE_RESPONSE);
    TableMetadata tableMetadata = loadTableResponse.tableMetadata();
    String catalogName = event.attributes().getRequired(EventAttributes.CATALOG_NAME);
    Namespace namespace = event.attributes().getRequired(EventAttributes.NAMESPACE);
    String tableName = event.attributes().getRequired(EventAttributes.TABLE_NAME);

    org.apache.polaris.core.entity.PolarisEvent polarisEvent =
        new org.apache.polaris.core.entity.PolarisEvent(
            catalogName,
            event.metadata().eventId().toString(),
            event.metadata().requestId().orElse(null),
            event.type().name(),
            event.metadata().timestamp().toEpochMilli(),
            event.metadata().user().map(PolarisPrincipal::getName).orElse(null),
            org.apache.polaris.core.entity.PolarisEvent.ResourceType.TABLE,
            TableIdentifier.of(namespace, tableName).toString());
    var additionalParameters =
        ImmutableMap.<String, String>builder()
            .put("table-uuid", tableMetadata.uuid())
            .put("metadata", TableMetadataParser.toJson(tableMetadata));
    additionalParameters.putAll(event.metadata().openTelemetryContext());
    polarisEvent.setAdditionalProperties(additionalParameters.build());
    processEvent(event.metadata().realmId(), polarisEvent);
  }

  private void handleAfterCreateCatalog(PolarisEvent event) {
    Catalog catalog = event.attributes().getRequired(EventAttributes.CATALOG);
    org.apache.polaris.core.entity.PolarisEvent polarisEvent =
        new org.apache.polaris.core.entity.PolarisEvent(
            catalog.getName(),
            event.metadata().eventId().toString(),
            event.metadata().requestId().orElse(null),
            event.type().name(),
            event.metadata().timestamp().toEpochMilli(),
            event.metadata().user().map(PolarisPrincipal::getName).orElse(null),
            org.apache.polaris.core.entity.PolarisEvent.ResourceType.CATALOG,
            catalog.getName());
    Map<String, String> openTelemetryContext = event.metadata().openTelemetryContext();
    if (!openTelemetryContext.isEmpty()) {
      polarisEvent.setAdditionalProperties(openTelemetryContext);
    }
    processEvent(event.metadata().realmId(), polarisEvent);
  }

  private void handleAfterReportMetrics(PolarisEvent event) {
    ReportMetricsRequest request =
        event.attributes().getRequired(EventAttributes.REPORT_METRICS_REQUEST);
    String catalogName = event.attributes().getRequired(EventAttributes.CATALOG_NAME);
    Namespace namespace = event.attributes().getRequired(EventAttributes.NAMESPACE);
    String tableName = event.attributes().getRequired(EventAttributes.TABLE_NAME);

    org.apache.polaris.core.entity.PolarisEvent polarisEvent =
        new org.apache.polaris.core.entity.PolarisEvent(
            catalogName,
            event.metadata().eventId().toString(),
            event.metadata().requestId().orElse(null),
            event.type().name(),
            event.metadata().timestamp().toEpochMilli(),
            event.metadata().user().map(PolarisPrincipal::getName).orElse(null),
            org.apache.polaris.core.entity.PolarisEvent.ResourceType.TABLE,
            TableIdentifier.of(namespace, tableName).toString());

    var additionalParameters = ImmutableMap.<String, String>builder();
    MetricsReport report = request.report();
    if (report instanceof ScanReport scanReport) {
      additionalParameters.put("report_type", "scan");
      additionalParameters.put("snapshot_id", String.valueOf(scanReport.snapshotId()));
      additionalParameters.put("schema_id", String.valueOf(scanReport.schemaId()));
      Map<String, String> metadata = scanReport.metadata();
      if (metadata != null) {
        metadata.forEach(
            (key, value) -> {
              if (value != null) {
                additionalParameters.put("report." + key, value);
              }
            });
      }
    } else if (report instanceof CommitReport commitReport) {
      additionalParameters.put("report_type", "commit");
      additionalParameters.put("snapshot_id", String.valueOf(commitReport.snapshotId()));
      additionalParameters.put("sequence_number", String.valueOf(commitReport.sequenceNumber()));
      if (commitReport.operation() != null) {
        additionalParameters.put("operation", commitReport.operation());
      }
      Map<String, String> metadata = commitReport.metadata();
      if (metadata != null) {
        metadata.forEach(
            (key, value) -> {
              if (value != null) {
                additionalParameters.put("report." + key, value);
              }
            });
      }
    }
    additionalParameters.putAll(event.metadata().openTelemetryContext());
    polarisEvent.setAdditionalProperties(additionalParameters.build());
    processEvent(event.metadata().realmId(), polarisEvent);
  }

  protected abstract void processEvent(
      String realmId, org.apache.polaris.core.entity.PolarisEvent event);
}
