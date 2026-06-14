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
import java.util.List;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Backend-agnostic representation of an Iceberg scan metrics report.
 *
 * <p><b>Note:</b> This type is part of the experimental Metrics Persistence SPI and may change in
 * future releases.
 */
@Beta
@PolarisImmutable
public interface ScanMetricsRecord extends MetricsRecordIdentity {

  Optional<Long> snapshotId();

  Optional<Integer> schemaId();

  Optional<String> filterExpression();

  List<Integer> projectedFieldIds();

  List<String> projectedFieldNames();

  long resultDataFiles();

  long resultDeleteFiles();

  long totalFileSizeBytes();

  long totalDataManifests();

  long totalDeleteManifests();

  long scannedDataManifests();

  long scannedDeleteManifests();

  long skippedDataManifests();

  long skippedDeleteManifests();

  long skippedDataFiles();

  long skippedDeleteFiles();

  long totalPlanningDurationMs();

  long equalityDeleteFiles();

  long positionalDeleteFiles();

  long indexedDeleteFiles();

  long totalDeleteFileSizeBytes();

  static ImmutableScanMetricsRecord.Builder builder() {
    return ImmutableScanMetricsRecord.builder();
  }
}
