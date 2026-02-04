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
import java.util.OptionalLong;
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
 * <tr><td>By Table + Time</td><td>catalogId, namespace, tableName, startTime, endTime</td><td>Yes (OSS)</td></tr>
 * <tr><td>By Time Only</td><td>startTime, endTime</td><td>Partial (timestamp index)</td></tr>
 * </table>
 *
 * <p>Additional query patterns (e.g., by trace ID) can be implemented by persistence backends using
 * the {@link #metadata()} filter map. Client-provided correlation data should be stored in the
 * metrics record's metadata map and can be filtered using the metadata criteria.
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

  /**
   * Catalog ID to filter by.
   *
   * <p>This is the internal catalog entity ID. Callers should resolve catalog names to IDs before
   * querying, as catalog names can change over time.
   */
  OptionalLong catalogId();

  /**
   * Namespace to filter by.
   *
   * <p>The namespace is represented as a list of levels to avoid ambiguity when segments contain
   * dots. An empty list means no namespace filter is applied.
   */
  List<String> namespace();

  /** Table name to filter by. */
  Optional<String> tableName();

  // === Time Range ===

  /** Start time for the query (inclusive). */
  Optional<Instant> startTime();

  /** End time for the query (exclusive). */
  Optional<Instant> endTime();

  // === Metadata Filtering ===

  /**
   * Metadata key-value pairs to filter by.
   *
   * <p>This enables filtering metrics by client-provided correlation data stored in the record's
   * metadata map. For example, clients may include a trace ID in the metadata that can be queried
   * later.
   *
   * <p>Note: Metadata filtering may require custom indexes depending on the persistence backend.
   * The OSS codebase provides basic support, but performance optimizations may be needed for
   * high-volume deployments.
   */
  Map<String, String> metadata();

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
   * @param catalogId the catalog entity ID
   * @param namespace the namespace as a list of levels
   * @param tableName the table name
   * @param startTime the start time (inclusive)
   * @param endTime the end time (exclusive)
   * @return the query criteria
   */
  static MetricsQueryCriteria forTable(
      long catalogId,
      List<String> namespace,
      String tableName,
      Instant startTime,
      Instant endTime) {
    return builder()
        .catalogId(catalogId)
        .namespace(namespace)
        .tableName(tableName)
        .startTime(startTime)
        .endTime(endTime)
        .build();
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
