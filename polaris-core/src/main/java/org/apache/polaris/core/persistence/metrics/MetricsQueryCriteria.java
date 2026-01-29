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
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * Query criteria for retrieving metrics reports.
 *
 * <p>This class defines the parameters that can be used to filter and paginate metrics query
 * results. Not all backends may support all query patterns - check the implementation documentation
 * for supported query patterns and required indexes.
 *
 * <h3>Supported Query Patterns</h3>
 *
 * <table>
 * <tr><th>Pattern</th><th>Fields Used</th><th>Index Required</th></tr>
 * <tr><td>By Table + Time</td><td>catalogName, namespace, tableName, startTime, endTime</td><td>Yes (OSS)</td></tr>
 * <tr><td>By Trace ID</td><td>otelTraceId</td><td>Yes (OSS)</td></tr>
 * <tr><td>By Principal</td><td>principalName</td><td>No (custom deployment)</td></tr>
 * <tr><td>By Time Only</td><td>startTime, endTime</td><td>Partial (timestamp index)</td></tr>
 * </table>
 */
@PolarisImmutable
public interface MetricsQueryCriteria {

  // === Table Identification (optional) ===

  /** Catalog name to filter by. */
  Optional<String> catalogName();

  /** Namespace to filter by (dot-separated). */
  Optional<String> namespace();

  /** Table name to filter by. */
  Optional<String> tableName();

  // === Time Range ===

  /** Start time for the query (inclusive). */
  Optional<Instant> startTime();

  /** End time for the query (exclusive). */
  Optional<Instant> endTime();

  // === Correlation ===

  /** OpenTelemetry trace ID to filter by. */
  Optional<String> otelTraceId();

  /**
   * Principal name to filter by.
   *
   * <p>Note: This query pattern may require a custom index in deployment environments. The OSS
   * codebase does not include an index for principal-based queries.
   */
  Optional<String> principalName();

  // === Pagination ===

  /** Maximum number of results to return. Defaults to 100. */
  @Value.Default
  default int limit() {
    return 100;
  }

  /** Number of results to skip. Defaults to 0. */
  @Value.Default
  default int offset() {
    return 0;
  }

  /**
   * Creates a new builder for MetricsQueryCriteria.
   *
   * @return a new builder instance
   */
  static ImmutableMetricsQueryCriteria.Builder builder() {
    return ImmutableMetricsQueryCriteria.builder();
  }

  /**
   * Creates criteria for querying by table and time range.
   *
   * @param catalogName the catalog name
   * @param namespace the namespace (dot-separated)
   * @param tableName the table name
   * @param startTime the start time (inclusive)
   * @param endTime the end time (exclusive)
   * @param limit maximum number of results
   * @return the query criteria
   */
  static MetricsQueryCriteria forTable(
      String catalogName,
      String namespace,
      String tableName,
      Instant startTime,
      Instant endTime,
      int limit) {
    return builder()
        .catalogName(catalogName)
        .namespace(namespace)
        .tableName(tableName)
        .startTime(startTime)
        .endTime(endTime)
        .limit(limit)
        .build();
  }

  /**
   * Creates criteria for querying by OpenTelemetry trace ID.
   *
   * @param traceId the trace ID to search for
   * @param limit maximum number of results
   * @return the query criteria
   */
  static MetricsQueryCriteria forTraceId(String traceId, int limit) {
    return builder().otelTraceId(traceId).limit(limit).build();
  }
}
