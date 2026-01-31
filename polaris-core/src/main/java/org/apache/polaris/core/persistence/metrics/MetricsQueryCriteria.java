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

/**
 * Query criteria for retrieving metrics reports.
 *
 * <p>This class defines the filter parameters for metrics queries. Pagination is handled separately
 * via {@link org.apache.polaris.core.persistence.pagination.PageToken}, which is passed as a
 * separate parameter to query methods. This separation of concerns allows:
 *
 * <ul>
 *   <li>Different backends to implement pagination in their optimal way
 *   <li>Cursor-based pagination that works with both RDBMS and NoSQL backends
 *   <li>Reuse of the existing Polaris pagination infrastructure
 * </ul>
 *
 * <h3>Supported Query Patterns</h3>
 *
 * <table>
 * <tr><th>Pattern</th><th>Fields Used</th><th>Index Required</th></tr>
 * <tr><td>By Table + Time</td><td>catalogName, namespace, tableName, startTime, endTime</td><td>Yes (OSS)</td></tr>
 * <tr><td>By Client Trace ID</td><td>reportTraceId</td><td>No (custom deployment)</td></tr>
 * <tr><td>By Time Only</td><td>startTime, endTime</td><td>Partial (timestamp index)</td></tr>
 * </table>
 *
 * <h3>Pagination</h3>
 *
 * <p>Pagination is handled via the {@link org.apache.polaris.core.persistence.pagination.PageToken}
 * passed to query methods. The token contains:
 *
 * <ul>
 *   <li>{@code pageSize()} - Maximum number of results to return
 *   <li>{@code value()} - Optional cursor token (e.g., {@link ReportIdToken}) for continuation
 * </ul>
 *
 * <p>Query results are returned as {@link org.apache.polaris.core.persistence.pagination.Page}
 * which includes an encoded token for fetching the next page.
 *
 * @see org.apache.polaris.core.persistence.pagination.PageToken
 * @see org.apache.polaris.core.persistence.pagination.Page
 * @see ReportIdToken
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

  /**
   * Client-provided trace ID to filter by (from report metadata).
   *
   * <p>This matches the {@code reportTraceId} field in the metrics records, which originates from
   * the client's metadata map. Useful for correlating metrics with client-side query execution.
   *
   * <p>Note: This query pattern may require a custom index in deployment environments. The OSS
   * codebase does not include an index for trace-based queries.
   */
  Optional<String> reportTraceId();

  // === Factory Methods ===

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
   * <p>Pagination is handled separately via the {@code PageToken} parameter to query methods.
   *
   * @param catalogName the catalog name
   * @param namespace the namespace (dot-separated)
   * @param tableName the table name
   * @param startTime the start time (inclusive)
   * @param endTime the end time (exclusive)
   * @return the query criteria
   */
  static MetricsQueryCriteria forTable(
      String catalogName, String namespace, String tableName, Instant startTime, Instant endTime) {
    return builder()
        .catalogName(catalogName)
        .namespace(namespace)
        .tableName(tableName)
        .startTime(startTime)
        .endTime(endTime)
        .build();
  }

  /**
   * Creates criteria for querying by client-provided trace ID.
   *
   * <p>Pagination is handled separately via the {@code PageToken} parameter to query methods.
   *
   * @param reportTraceId the client trace ID to search for
   * @return the query criteria
   */
  static MetricsQueryCriteria forReportTraceId(String reportTraceId) {
    return builder().reportTraceId(reportTraceId).build();
  }

  /**
   * Creates empty criteria (no filters). Useful for pagination-only queries.
   *
   * @return empty query criteria
   */
  static MetricsQueryCriteria empty() {
    return builder().build();
  }
}
