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

/**
 * Service Provider Interface (SPI) for persisting Iceberg metrics reports.
 *
 * <p>This interface enables different persistence backends (JDBC, NoSQL, custom) to implement
 * metrics storage in a way appropriate for their storage model, while allowing service code to
 * remain backend-agnostic.
 *
 * <p>All methods have default no-op implementations. Persistence backends that support metrics
 * (e.g., JDBC) should override these methods to provide actual storage. Backends that don't support
 * metrics can use the default implementations which silently ignore writes.
 *
 * <p>Implementations should be idempotent - writing the same reportId twice should have no effect.
 *
 * <h3>Multi-Tenancy</h3>
 *
 * <p>Realm context is not passed in the record objects. Implementations should obtain the realm
 * from the CDI-injected {@code RealmContext} at write time. This keeps catalog-specific code from
 * needing to manage realm concerns directly.
 *
 * <p><b>Note:</b> This SPI is currently experimental and not yet implemented in all persistence
 * backends. The API may change in future releases.
 *
 * @see PolarisMetricsManager
 */
@Beta
public interface MetricsPersistence {

  /**
   * Persists a scan metrics record.
   *
   * <p>This operation is idempotent - writing the same reportId twice has no effect.
   *
   * <p>Default implementation is a no-op. Override in implementations that support metrics.
   *
   * @param record the scan metrics record to persist
   */
  default void writeScanReport(@Nonnull ScanMetricsRecord record) {
    // No-op by default - backends that don't support metrics silently ignore
  }

  /**
   * Persists a commit metrics record.
   *
   * <p>This operation is idempotent - writing the same reportId twice has no effect.
   *
   * <p>Default implementation is a no-op. Override in implementations that support metrics.
   *
   * @param record the commit metrics record to persist
   */
  default void writeCommitReport(@Nonnull CommitMetricsRecord record) {
    // No-op by default - backends that don't support metrics silently ignore
  }
}
