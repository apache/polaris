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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RequestIdSupplier;
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
 * Implementations that don't support metrics persistence can use {@link #NOOP} which silently
 * ignores write operations and returns empty pages for queries.
 *
 * <h3>Dependency Injection</h3>
 *
 * <p>This interface is designed to be injected via CDI (Contexts and Dependency Injection). The
 * deployment module (e.g., {@code polaris-quarkus-service}) should provide a {@code @Produces}
 * method that creates the appropriate implementation based on the configured persistence backend.
 *
 * <p>Example producer:
 *
 * <pre>{@code
 * @Produces
 * @RequestScoped
 * MetricsPersistence metricsPersistence(RealmContext realmContext, PersistenceBackend backend) {
 *   if (backend.supportsMetrics()) {
 *     return backend.createMetricsPersistence(realmContext);
 *   }
 *   return MetricsPersistence.NOOP;
 * }
 * }</pre>
 *
 * <h3>Multi-Tenancy</h3>
 *
 * <p>Realm context is not passed in the record objects. Implementations should obtain the realm
 * from the CDI-injected {@code RealmContext} at write/query time. This keeps catalog-specific code
 * from needing to manage realm concerns directly.
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
 * <p>The {@link ReportIdToken} provides a reference cursor implementation based on report ID
 * (UUID), but backends may use other cursor strategies internally.
 *
 * <p><b>Note:</b> This SPI is currently experimental and not yet implemented in all persistence
 * backends. The API may change in future releases.
 *
 * @see PageToken
 * @see Page
 * @see ReportIdToken
 */
@Beta
public interface MetricsPersistence {

  /** A no-op implementation for backends that don't support metrics persistence. */
  MetricsPersistence NOOP = new NoOpMetricsPersistence();

  // ============================================================================
  // Request Context
  // ============================================================================

  /**
   * Sets the request-scoped context for metrics persistence.
   *
   * <p>This method should be called per-request to provide the principal and request ID supplier
   * needed for metrics persistence operations. Implementations may use this information to enrich
   * stored metrics with request context (e.g., who made the request, trace correlation).
   *
   * <p>The default implementation does nothing, allowing implementations that don't need request
   * context to ignore this method.
   *
   * @param principal the authenticated principal for the current request (may be null)
   * @param requestIdSupplier supplier for obtaining the server-generated request ID
   */
  default void setMetricsRequestContext(
      @Nullable PolarisPrincipal principal, @Nullable RequestIdSupplier requestIdSupplier) {
    // Default: no-op - implementations can override if they need request context
  }

  // ============================================================================
  // Write Operations
  // ============================================================================

  /**
   * Persists a scan metrics record.
   *
   * <p>This operation is idempotent - writing the same reportId twice has no effect.
   *
   * @param record the scan metrics record to persist
   */
  void writeScanReport(@Nonnull ScanMetricsRecord record);

  /**
   * Persists a commit metrics record.
   *
   * <p>This operation is idempotent - writing the same reportId twice has no effect.
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
   * @param criteria the query criteria (filters)
   * @param pageToken pagination parameters (page size and optional cursor)
   * @return page of matching commit metrics records with continuation token if more results exist
   * @see #queryScanReports(MetricsQueryCriteria, PageToken) for pagination example
   */
  @Nonnull
  Page<CommitMetricsRecord> queryCommitReports(
      @Nonnull MetricsQueryCriteria criteria, @Nonnull PageToken pageToken);
}
