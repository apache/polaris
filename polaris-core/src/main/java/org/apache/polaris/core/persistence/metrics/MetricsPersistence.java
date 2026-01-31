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

import jakarta.annotation.Nonnull;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;

/**
 * Service Provider Interface (SPI) for persisting Iceberg metrics reports.
 *
 * <p>This interface enables different persistence backends (JDBC, NoSQL, custom) to implement
 * metrics storage in a way appropriate for their storage model, while allowing service code to
 * remain backend-agnostic.
 *
 * <p>Implementations should be idempotent - writing the same reportId twice should have no effect.
 * Implementations that don't support metrics persistence should return {@link #NOOP}.
 *
 * <h3>Pagination</h3>
 *
 * <p>Query methods use the standard Polaris pagination pattern with {@link PageToken} for requests
 * and {@link Page} for responses. This enables:
 *
 * <ul>
 *   <li>Backend-specific cursor implementations (RDBMS offset, NoSQL continuation tokens, etc.)
 *   <li>Consistent pagination interface across all Polaris persistence APIs
 *   <li>Efficient cursor-based pagination that works with large result sets
 * </ul>
 *
 * <p>The {@link ReportIdToken} provides a cursor based on the report ID (UUID), but backends may
 * use other cursor strategies internally.
 *
 * @see PageToken
 * @see Page
 * @see ReportIdToken
 */
public interface MetricsPersistence {

  /** A no-op implementation for backends that don't support metrics persistence. */
  MetricsPersistence NOOP = new NoOpMetricsPersistence();

  // ============================================================================
  // Capability Detection
  // ============================================================================

  /**
   * Returns whether this persistence backend supports metrics storage.
   *
   * <p>Backends that do not support metrics should return false. Service code should NOT use this
   * to branch with instanceof checks - instead, call the interface methods directly and rely on the
   * no-op behavior for unsupported backends.
   *
   * @return true if metrics persistence is supported, false otherwise
   */
  boolean isSupported();

  // ============================================================================
  // Write Operations
  // ============================================================================

  /**
   * Persists a scan metrics record.
   *
   * <p>This operation is idempotent - writing the same reportId twice has no effect. If {@link
   * #isSupported()} returns false, this is a no-op.
   *
   * @param record the scan metrics record to persist
   */
  void writeScanReport(@Nonnull ScanMetricsRecord record);

  /**
   * Persists a commit metrics record.
   *
   * <p>This operation is idempotent - writing the same reportId twice has no effect. If {@link
   * #isSupported()} returns false, this is a no-op.
   *
   * @param record the commit metrics record to persist
   */
  void writeCommitReport(@Nonnull CommitMetricsRecord record);

  // ============================================================================
  // Query Operations
  // ============================================================================

  /**
   * Queries scan metrics reports based on the specified criteria.
   *
   * <p>Returns an empty page if {@link #isSupported()} returns false.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * // First page
   * PageToken pageToken = PageToken.fromLimit(100);
   * Page<ScanMetricsRecord> page = persistence.queryScanReports(criteria, pageToken);
   *
   * // Next page (if available)
   * String nextPageToken = page.encodedResponseToken();
   * if (nextPageToken != null) {
   *   pageToken = PageToken.build(nextPageToken, null, () -> true);
   *   Page<ScanMetricsRecord> nextPage = persistence.queryScanReports(criteria, pageToken);
   * }
   * }</pre>
   *
   * @param criteria the query criteria (filters)
   * @param pageToken pagination parameters (page size and optional cursor)
   * @return page of matching scan metrics records with continuation token if more results exist
   */
  @Nonnull
  Page<ScanMetricsRecord> queryScanReports(
      @Nonnull MetricsQueryCriteria criteria, @Nonnull PageToken pageToken);

  /**
   * Queries commit metrics reports based on the specified criteria.
   *
   * <p>Returns an empty page if {@link #isSupported()} returns false.
   *
   * @param criteria the query criteria (filters)
   * @param pageToken pagination parameters (page size and optional cursor)
   * @return page of matching commit metrics records with continuation token if more results exist
   * @see #queryScanReports(MetricsQueryCriteria, PageToken) for pagination example
   */
  @Nonnull
  Page<CommitMetricsRecord> queryCommitReports(
      @Nonnull MetricsQueryCriteria criteria, @Nonnull PageToken pageToken);
}
