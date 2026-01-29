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
import java.util.List;

/**
 * Service Provider Interface (SPI) for persisting Iceberg metrics reports.
 *
 * <p>This interface enables different persistence backends (JDBC, NoSQL, custom) to implement
 * metrics storage in a way appropriate for their storage model, while allowing service code to
 * remain backend-agnostic.
 *
 * <p>Implementations should be idempotent - writing the same reportId twice should have no effect.
 * Implementations that don't support metrics persistence should return {@link #NOOP}.
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
   * <p>Returns an empty list if {@link #isSupported()} returns false.
   *
   * @param criteria the query criteria
   * @return list of matching scan metrics records, or empty list if not supported
   */
  @Nonnull
  List<ScanMetricsRecord> queryScanReports(@Nonnull MetricsQueryCriteria criteria);

  /**
   * Queries commit metrics reports based on the specified criteria.
   *
   * <p>Returns an empty list if {@link #isSupported()} returns false.
   *
   * @param criteria the query criteria
   * @return list of matching commit metrics records, or empty list if not supported
   */
  @Nonnull
  List<CommitMetricsRecord> queryCommitReports(@Nonnull MetricsQueryCriteria criteria);
}
