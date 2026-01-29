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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Backend-agnostic representation of an Iceberg scan metrics report.
 *
 * <p>This record captures all relevant metrics from an Iceberg {@code ScanReport} along with
 * contextual information such as realm, catalog, and request correlation data.
 */
@PolarisImmutable
public interface ScanMetricsRecord {

  // === Identification ===

  /** Unique identifier for this report (UUID). */
  String reportId();

  /** Multi-tenancy realm identifier. */
  String realmId();

  /** Internal catalog ID. */
  String catalogId();

  /** Human-readable catalog name. */
  String catalogName();

  /** Dot-separated namespace path (e.g., "db.schema"). */
  String namespace();

  /** Table name. */
  String tableName();

  // === Timing ===

  /** Timestamp when the report was received. */
  Instant timestamp();

  // === Client Correlation ===

  /**
   * Client-provided trace ID from the metrics report metadata.
   *
   * <p>This is an optional identifier that the Iceberg client may include in the report's metadata
   * map (typically under the key "trace-id"). It allows clients to correlate this metrics report
   * with their own distributed tracing system or query execution context.
   *
   * <p>Note: Server-side tracing information (e.g., OpenTelemetry trace/span IDs) and principal
   * information are not included in this record. The persistence implementation can obtain these
   * from the ambient request context (OTel context, security context) at write time if needed.
   */
  Optional<String> reportTraceId();

  // === Scan Context ===

  /** Snapshot ID that was scanned. */
  Optional<Long> snapshotId();

  /** Schema ID used for the scan. */
  Optional<Integer> schemaId();

  /** Filter expression applied to the scan (as string). */
  Optional<String> filterExpression();

  /** List of projected field IDs. */
  List<Integer> projectedFieldIds();

  /** List of projected field names. */
  List<String> projectedFieldNames();

  // === Scan Metrics - File Counts ===

  /** Number of data files in the result. */
  long resultDataFiles();

  /** Number of delete files in the result. */
  long resultDeleteFiles();

  /** Total size of files in bytes. */
  long totalFileSizeBytes();

  // === Scan Metrics - Manifest Counts ===

  /** Total number of data manifests. */
  long totalDataManifests();

  /** Total number of delete manifests. */
  long totalDeleteManifests();

  /** Number of data manifests that were scanned. */
  long scannedDataManifests();

  /** Number of delete manifests that were scanned. */
  long scannedDeleteManifests();

  /** Number of data manifests that were skipped. */
  long skippedDataManifests();

  /** Number of delete manifests that were skipped. */
  long skippedDeleteManifests();

  /** Number of data files that were skipped. */
  long skippedDataFiles();

  /** Number of delete files that were skipped. */
  long skippedDeleteFiles();

  // === Scan Metrics - Timing ===

  /** Total planning duration in milliseconds. */
  long totalPlanningDurationMs();

  // === Scan Metrics - Delete Files ===

  /** Number of equality delete files. */
  long equalityDeleteFiles();

  /** Number of positional delete files. */
  long positionalDeleteFiles();

  /** Number of indexed delete files. */
  long indexedDeleteFiles();

  /** Total size of delete files in bytes. */
  long totalDeleteFileSizeBytes();

  // === Extensibility ===

  /** Additional metadata as key-value pairs. */
  Map<String, String> metadata();

  /**
   * Creates a new builder for ScanMetricsRecord.
   *
   * @return a new builder instance
   */
  static ImmutableScanMetricsRecord.Builder builder() {
    return ImmutableScanMetricsRecord.builder();
  }
}
