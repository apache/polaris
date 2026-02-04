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

import java.util.List;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Backend-agnostic representation of an Iceberg scan metrics report.
 *
 * <p>This record captures all relevant metrics from an Iceberg {@code ScanReport} along with
 * contextual information such as catalog identification and table location.
 *
 * <p>Common identification fields are inherited from {@link MetricsRecordIdentity}.
 *
 * <p>Note: Realm ID is not included in this record. Multi-tenancy realm context should be obtained
 * from the CDI-injected {@code RealmContext} at persistence time.
 */
@PolarisImmutable
public interface ScanMetricsRecord extends MetricsRecordIdentity {

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

  /**
   * Creates a new builder for ScanMetricsRecord.
   *
   * @return a new builder instance
   */
  static ImmutableScanMetricsRecord.Builder builder() {
    return ImmutableScanMetricsRecord.builder();
  }
}
