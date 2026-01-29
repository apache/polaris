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
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Backend-agnostic representation of an Iceberg commit metrics report.
 *
 * <p>This record captures all relevant metrics from an Iceberg {@code CommitReport} along with
 * contextual information such as realm, catalog, and request correlation data.
 */
@PolarisImmutable
public interface CommitMetricsRecord {

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

  // === Request Context ===

  /** Name of the principal who initiated the operation. */
  Optional<String> principalName();

  /** Request ID for correlation. */
  Optional<String> requestId();

  /** OpenTelemetry trace ID for distributed tracing. */
  Optional<String> otelTraceId();

  /** OpenTelemetry span ID for distributed tracing. */
  Optional<String> otelSpanId();

  /** Trace ID from the report itself (may differ from OTel trace). */
  Optional<String> reportTraceId();

  // === Commit Context ===

  /** Snapshot ID created by this commit. */
  long snapshotId();

  /** Sequence number of the snapshot. */
  Optional<Long> sequenceNumber();

  /** Operation type (e.g., "append", "overwrite", "delete"). */
  String operation();

  // === File Metrics - Data Files ===

  /** Number of data files added. */
  long addedDataFiles();

  /** Number of data files removed. */
  long removedDataFiles();

  /** Total number of data files after commit. */
  long totalDataFiles();

  // === File Metrics - Delete Files ===

  /** Number of delete files added. */
  long addedDeleteFiles();

  /** Number of delete files removed. */
  long removedDeleteFiles();

  /** Total number of delete files after commit. */
  long totalDeleteFiles();

  /** Number of equality delete files added. */
  long addedEqualityDeleteFiles();

  /** Number of equality delete files removed. */
  long removedEqualityDeleteFiles();

  /** Number of positional delete files added. */
  long addedPositionalDeleteFiles();

  /** Number of positional delete files removed. */
  long removedPositionalDeleteFiles();

  // === Record Metrics ===

  /** Number of records added. */
  long addedRecords();

  /** Number of records removed. */
  long removedRecords();

  /** Total number of records after commit. */
  long totalRecords();

  // === Size Metrics ===

  /** Size of added files in bytes. */
  long addedFileSizeBytes();

  /** Size of removed files in bytes. */
  long removedFileSizeBytes();

  /** Total file size in bytes after commit. */
  long totalFileSizeBytes();

  // === Timing ===

  /** Total duration of the commit in milliseconds. */
  Optional<Long> totalDurationMs();

  /** Number of commit attempts. */
  int attempts();

  // === Extensibility ===

  /** Additional metadata as key-value pairs. */
  Map<String, String> metadata();

  /**
   * Creates a new builder for CommitMetricsRecord.
   *
   * @return a new builder instance
   */
  static ImmutableCommitMetricsRecord.Builder builder() {
    return ImmutableCommitMetricsRecord.builder();
  }
}
