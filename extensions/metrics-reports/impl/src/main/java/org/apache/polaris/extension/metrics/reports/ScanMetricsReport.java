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
package org.apache.polaris.extension.metrics.reports;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** A single scan metrics report entry returned by the list API. */
public record ScanMetricsReport(
    @JsonProperty("id") String id,
    @JsonProperty("timestampMs") long timestampMs,
    @JsonProperty("actor") MetricsReportActor actor,
    @JsonProperty("request") MetricsReportRequest request,
    @JsonProperty("object") TableObject object,
    @JsonProperty("payload") Payload payload) {

  /** The Iceberg object (table snapshot) associated with the scan. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record TableObject(@JsonProperty("snapshotId") Long snapshotId) {}

  /** The scan metrics payload envelope. */
  public record Payload(
      @JsonProperty("type") String type,
      @JsonProperty("version") int version,
      @JsonProperty("data") Data data) {

    /** Iceberg scan metrics data fields. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Data(
        @JsonProperty("schemaId") Integer schemaId,
        @JsonProperty("filterExpression") String filterExpression,
        @JsonProperty("projectedFieldIds") List<Integer> projectedFieldIds,
        @JsonProperty("projectedFieldNames") List<String> projectedFieldNames,
        @JsonProperty("resultDataFiles") long resultDataFiles,
        @JsonProperty("resultDeleteFiles") long resultDeleteFiles,
        @JsonProperty("totalFileSizeBytes") long totalFileSizeBytes,
        @JsonProperty("totalDataManifests") long totalDataManifests,
        @JsonProperty("totalDeleteManifests") long totalDeleteManifests,
        @JsonProperty("scannedDataManifests") long scannedDataManifests,
        @JsonProperty("scannedDeleteManifests") long scannedDeleteManifests,
        @JsonProperty("skippedDataManifests") long skippedDataManifests,
        @JsonProperty("skippedDeleteManifests") long skippedDeleteManifests,
        @JsonProperty("skippedDataFiles") long skippedDataFiles,
        @JsonProperty("skippedDeleteFiles") long skippedDeleteFiles,
        @JsonProperty("totalPlanningDurationMs") long totalPlanningDurationMs,
        @JsonProperty("equalityDeleteFiles") long equalityDeleteFiles,
        @JsonProperty("positionalDeleteFiles") long positionalDeleteFiles,
        @JsonProperty("indexedDeleteFiles") long indexedDeleteFiles,
        @JsonProperty("totalDeleteFileSizeBytes") long totalDeleteFileSizeBytes) {}
  }
}
