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
package org.apache.polaris.core.persistence.metrics;

import com.google.common.annotations.Beta;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Base interface containing common identification fields shared by all metrics records.
 *
 * <p>This interface defines the common fields that identify the source of a metrics report,
 * including the report ID, catalog ID, table ID, timestamp, and metadata.
 *
 * <p>Both {@link ScanMetricsRecord} and {@link CommitMetricsRecord} extend this interface to
 * inherit these common fields while adding their own specific metrics.
 *
 * <h3>Design Decisions</h3>
 *
 * <p><b>Entity IDs only (no names):</b> We store only catalog ID and table ID, not their names or
 * namespace paths. Names can change over time (via rename operations), which would make querying
 * historical metrics by name challenging and lead to correctness issues. Queries should resolve
 * names to IDs using the current catalog state. The table ID uniquely identifies the table, and the
 * namespace can be derived from the table entity if needed.
 *
 * <p><b>Realm ID:</b> Realm ID is intentionally not included in this interface. Multi-tenancy realm
 * context should be obtained from the CDI-injected {@code RealmContext} at persistence time. This
 * keeps catalog-specific code from needing to manage realm concerns.
 *
 * <p><b>Note:</b> This type is part of the experimental Metrics Persistence SPI and may change in
 * future releases.
 */
@Beta
public interface MetricsRecordIdentity {

  /**
   * Unique identifier for this report (UUID).
   *
   * <p>This ID is generated when the record is created and serves as the primary key for the
   * metrics record in persistence storage.
   */
  String reportId();

  /**
   * Internal catalog ID.
   *
   * <p>This matches the catalog entity ID in Polaris persistence, as defined by {@code
   * PolarisEntityCore#getId()}. The catalog name is not stored since it can change over time;
   * queries should resolve names to IDs using the current catalog state.
   */
  long catalogId();

  /**
   * Internal table entity ID.
   *
   * <p>This matches the table entity ID in Polaris persistence, as defined by {@code
   * PolarisEntityCore#getId()}. The table name is not stored since it can change over time; queries
   * should resolve names to IDs using the current catalog state. The namespace can be derived from
   * the table entity if needed.
   */
  long tableId();

  /**
   * Timestamp when the report was received.
   *
   * <p>This is the server-side timestamp when the metrics report was processed, not the client-side
   * timestamp when the operation occurred.
   */
  Instant timestamp();

  /**
   * Additional metadata as key-value pairs.
   *
   * <p>This map can contain additional contextual information from the original Iceberg report,
   * including client-provided trace IDs or other correlation data. Persistence implementations can
   * store and index specific metadata fields as needed.
   */
  Map<String, String> metadata();

  // === Request Context Fields ===

  /**
   * Name of the principal (user) who made the request.
   *
   * <p>This is the authenticated principal name from the security context when the metrics report
   * was received. Useful for auditing and tracking which users are accessing which tables.
   */
  Optional<String> principalName();

  /**
   * Server-generated request ID for correlation.
   *
   * <p>This is the request ID assigned by the Polaris server when the metrics report was received.
   * It can be used to correlate metrics with server logs and other telemetry.
   */
  Optional<String> requestId();

  /**
   * OpenTelemetry trace ID.
   *
   * <p>The trace ID from the OpenTelemetry context when the metrics report was received. This
   * enables correlation with distributed traces across services.
   */
  Optional<String> otelTraceId();

  /**
   * OpenTelemetry span ID.
   *
   * <p>The span ID from the OpenTelemetry context when the metrics report was received.
   */
  Optional<String> otelSpanId();

  /**
   * Client-provided report trace ID.
   *
   * <p>An optional trace ID provided by the Iceberg client in the report metadata. This can be used
   * by clients to correlate metrics reports with their own internal tracing systems.
   */
  Optional<String> reportTraceId();
}
