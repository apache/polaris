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

/** A single commit metrics report entry returned by the list API. */
public record CommitMetricsReport(
    @JsonProperty("id") String id,
    @JsonProperty("timestampMs") long timestampMs,
    @JsonProperty("actor") MetricsReportActor actor,
    @JsonProperty("request") MetricsReportRequest request,
    @JsonProperty("object") TableObject object,
    @JsonProperty("payload") Payload payload) {

  /** The Iceberg object (table snapshot) associated with the commit. */
  public record TableObject(@JsonProperty("snapshotId") long snapshotId) {}

  /** The commit metrics payload envelope. */
  public record Payload(
      @JsonProperty("type") String type,
      @JsonProperty("version") int version,
      @JsonProperty("data") Data data) {

    /** Iceberg commit metrics data fields. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Data(
        @JsonProperty("sequenceNumber") Long sequenceNumber,
        @JsonProperty("operation") String operation,
        @JsonProperty("addedDataFiles") long addedDataFiles,
        @JsonProperty("removedDataFiles") long removedDataFiles,
        @JsonProperty("totalDataFiles") long totalDataFiles,
        @JsonProperty("addedDeleteFiles") long addedDeleteFiles,
        @JsonProperty("removedDeleteFiles") long removedDeleteFiles,
        @JsonProperty("totalDeleteFiles") long totalDeleteFiles,
        @JsonProperty("addedEqualityDeleteFiles") long addedEqualityDeleteFiles,
        @JsonProperty("removedEqualityDeleteFiles") long removedEqualityDeleteFiles,
        @JsonProperty("addedPositionalDeleteFiles") long addedPositionalDeleteFiles,
        @JsonProperty("removedPositionalDeleteFiles") long removedPositionalDeleteFiles,
        @JsonProperty("addedRecords") long addedRecords,
        @JsonProperty("removedRecords") long removedRecords,
        @JsonProperty("totalRecords") long totalRecords,
        @JsonProperty("addedFileSizeBytes") long addedFileSizeBytes,
        @JsonProperty("removedFileSizeBytes") long removedFileSizeBytes,
        @JsonProperty("totalFileSizeBytes") long totalFileSizeBytes,
        @JsonProperty("totalDurationMs") Long totalDurationMs,
        @JsonProperty("attempts") int attempts) {}
  }
}
