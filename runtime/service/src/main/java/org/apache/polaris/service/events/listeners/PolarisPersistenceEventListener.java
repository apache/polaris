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
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.service.events.CatalogsServiceEvents;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;

public abstract class PolarisPersistenceEventListener implements PolarisEventListener {

  // TODO: Ensure all events (except RateLimiter ones) call `processEvent`

  @Override
  public void onAfterCreateTable(IcebergRestCatalogEvents.AfterCreateTableEvent event) {
    TableMetadata tableMetadata = event.loadTableResponse().tableMetadata();
    PolarisEvent polarisEvent =
        new PolarisEvent(
            event.catalogName(),
            event.metadata().eventId().toString(),
            event.metadata().requestId().orElse(null),
            event.getClass().getSimpleName(),
            event.metadata().timestamp().toEpochMilli(),
            event.metadata().user().map(PolarisPrincipal::getName).orElse(null),
            PolarisEvent.ResourceType.TABLE,
            TableIdentifier.of(event.namespace(), event.tableName()).toString());
    var additionalParameters =
        ImmutableMap.<String, String>builder()
            .put("table-uuid", tableMetadata.uuid())
            .put("metadata", TableMetadataParser.toJson(tableMetadata));
    additionalParameters.putAll(event.metadata().openTelemetryContext());
    polarisEvent.setAdditionalProperties(additionalParameters.build());
    processEvent(event.metadata().realmId(), polarisEvent);
  }

  @Override
  public void onAfterCreateCatalog(CatalogsServiceEvents.AfterCreateCatalogEvent event) {
    PolarisEvent polarisEvent =
        new PolarisEvent(
            event.catalog().getName(),
            event.metadata().eventId().toString(),
            event.metadata().requestId().orElse(null),
            event.getClass().getSimpleName(),
            event.metadata().timestamp().toEpochMilli(),
            event.metadata().user().map(PolarisPrincipal::getName).orElse(null),
            PolarisEvent.ResourceType.CATALOG,
            event.catalog().getName());
    Map<String, String> openTelemetryContext = event.metadata().openTelemetryContext();
    if (!openTelemetryContext.isEmpty()) {
      polarisEvent.setAdditionalProperties(openTelemetryContext);
    }
    processEvent(event.metadata().realmId(), polarisEvent);
  }

  @Override
  public void onAfterReportMetrics(IcebergRestCatalogEvents.AfterReportMetricsEvent event) {
    // Build table identifier with null safety
    Namespace namespace = event.namespace();
    String table = event.table();
    String tableIdentifierStr =
        (namespace != null && table != null)
            ? TableIdentifier.of(namespace, table).toString()
            : (table != null ? table : "unknown");

    PolarisEvent polarisEvent =
        new PolarisEvent(
            event.catalogName(),
            event.metadata().eventId().toString(),
            event.metadata().requestId().orElse(null),
            event.getClass().getSimpleName(),
            event.metadata().timestamp().toEpochMilli(),
            event.metadata().user().map(PolarisPrincipal::getName).orElse(null),
            PolarisEvent.ResourceType.TABLE,
            tableIdentifierStr);

    // Build additional properties with metrics data and trace context
    ImmutableMap.Builder<String, String> additionalProperties = ImmutableMap.builder();

    // Add OpenTelemetry context from HTTP headers
    additionalProperties.putAll(event.metadata().openTelemetryContext());

    // Extract metadata and metrics from the report
    ReportMetricsRequest request = event.reportMetricsRequest();
    if (request != null) {
      MetricsReport report = request.report();
      if (report instanceof ScanReport scanReport) {
        extractScanReportData(scanReport, additionalProperties);
      } else if (report instanceof CommitReport commitReport) {
        extractCommitReportData(commitReport, additionalProperties);
      }
    }

    Map<String, String> props = additionalProperties.build();
    if (!props.isEmpty()) {
      polarisEvent.setAdditionalProperties(props);
    }
    processEvent(event.metadata().realmId(), polarisEvent);
  }

  /**
   * Extracts data from a ScanReport including metadata (trace-id) and key scan metrics.
   *
   * @param scanReport The scan report from the compute engine
   * @param builder The builder to add properties to
   */
  private void extractScanReportData(
      ScanReport scanReport, ImmutableMap.Builder<String, String> builder) {
    builder.put("report_type", "scan");
    builder.put("snapshot_id", String.valueOf(scanReport.snapshotId()));
    builder.put("schema_id", String.valueOf(scanReport.schemaId()));

    // Extract trace-id and other metadata from the report's metadata map
    // This is where compute engines pass trace context for correlation
    Map<String, String> reportMetadata = scanReport.metadata();
    addReportMetadata(builder, reportMetadata);

    // Extract key scan metrics for audit purposes
    ScanMetricsResult metrics = scanReport.scanMetrics();
    if (metrics != null) {
      addCounterIfPresent(builder, "result_data_files", metrics.resultDataFiles());
      addCounterIfPresent(builder, "result_delete_files", metrics.resultDeleteFiles());
      addCounterIfPresent(builder, "total_file_size_bytes", metrics.totalFileSizeInBytes());
      addCounterIfPresent(builder, "scanned_data_manifests", metrics.scannedDataManifests());
      addCounterIfPresent(builder, "skipped_data_manifests", metrics.skippedDataManifests());
    }
  }

  /**
   * Extracts data from a CommitReport including metadata (trace-id) and key commit metrics.
   *
   * @param commitReport The commit report from the compute engine
   * @param builder The builder to add properties to
   */
  private void extractCommitReportData(
      CommitReport commitReport, ImmutableMap.Builder<String, String> builder) {
    builder.put("report_type", "commit");
    builder.put("snapshot_id", String.valueOf(commitReport.snapshotId()));
    builder.put("sequence_number", String.valueOf(commitReport.sequenceNumber()));
    // Null-safe handling of operation - it may be null for some report types
    String operation = commitReport.operation();
    if (operation != null) {
      builder.put("operation", operation);
    }

    // Extract trace-id and other metadata from the report's metadata map
    // This is where compute engines pass trace context for correlation
    Map<String, String> reportMetadata = commitReport.metadata();
    addReportMetadata(builder, reportMetadata);

    // Extract key commit metrics for audit purposes
    CommitMetricsResult metrics = commitReport.commitMetrics();
    if (metrics != null) {
      addCounterIfPresent(builder, "added_data_files", metrics.addedDataFiles());
      addCounterIfPresent(builder, "removed_data_files", metrics.removedDataFiles());
      addCounterIfPresent(builder, "added_records", metrics.addedRecords());
      addCounterIfPresent(builder, "removed_records", metrics.removedRecords());
      addCounterIfPresent(builder, "added_file_size_bytes", metrics.addedFilesSizeInBytes());
      addCounterIfPresent(builder, "removed_file_size_bytes", metrics.removedFilesSizeInBytes());
    }
  }

  /**
   * Adds report metadata entries to the builder with null-safety checks. Entries with null keys or
   * values are skipped to prevent ImmutableMap.Builder from throwing NPE.
   *
   * @param builder The builder to add properties to
   * @param reportMetadata The metadata map from the report (may be null)
   */
  private void addReportMetadata(
      ImmutableMap.Builder<String, String> builder, Map<String, String> reportMetadata) {
    if (reportMetadata != null && !reportMetadata.isEmpty()) {
      for (Map.Entry<String, String> entry : reportMetadata.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        // Skip entries with null keys or values to prevent ImmutableMap.Builder NPE
        if (key != null && value != null) {
          // Prefix with "report." to distinguish from OpenTelemetry context
          builder.put("report." + key, value);
        }
      }
    }
  }

  /**
   * Adds a counter value to the builder if the counter is present and has a value.
   *
   * @param builder The builder to add the property to
   * @param key The property key
   * @param counter The counter result (may be null)
   */
  private void addCounterIfPresent(
      ImmutableMap.Builder<String, String> builder, String key, CounterResult counter) {
    if (counter != null) {
      builder.put(key, String.valueOf(counter.value()));
    }
  }

  protected abstract void processEvent(String realmId, PolarisEvent event);
}
