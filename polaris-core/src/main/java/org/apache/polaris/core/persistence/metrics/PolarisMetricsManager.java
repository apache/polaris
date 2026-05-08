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
import org.apache.polaris.core.PolarisCallContext;

/**
 * Interface for managing Iceberg metrics persistence through the metastore manager layer.
 *
 * <p>This follows the same pattern as {@link org.apache.polaris.core.entity.PolarisEventManager},
 * providing a high-level interface that delegates to the underlying persistence layer.
 *
 * <p>The service layer should interact with this interface (via {@link
 * org.apache.polaris.core.persistence.PolarisMetaStoreManager}) rather than directly accessing
 * persistence implementations.
 *
 * <p>Request context (principal name, request ID, OTEL trace/span IDs) should be populated in the
 * record by the caller before invoking these methods. This keeps the SPI simple with a single
 * method parameter containing all the data needed for persistence.
 *
 * <p>Since {@link org.apache.polaris.core.persistence.BasePersistence} now extends {@link
 * MetricsPersistence} with default no-op implementations, all persistence backends automatically
 * support this interface. Backends that want actual metrics persistence (e.g., JDBC) override the
 * methods; others use the default no-op behavior.
 */
public interface PolarisMetricsManager {

  /**
   * Writes a scan metrics record to the persistence layer.
   *
   * <p>Delegates to the underlying {@link MetricsPersistence#writeScanReport} method. If the
   * persistence backend doesn't override the default implementation, this is a no-op.
   *
   * <p>The record should contain all request context fields (principalName, requestId, otelTraceId,
   * otelSpanId) populated by the caller.
   *
   * @param callCtx the call context containing the persistence layer
   * @param record the scan metrics record to persist (including request context)
   */
  default void writeScanMetrics(
      @Nonnull PolarisCallContext callCtx, @Nonnull ScanMetricsRecord record) {
    callCtx.getMetaStore().writeScanReport(record);
  }

  /**
   * Writes a commit metrics record to the persistence layer.
   *
   * <p>Delegates to the underlying {@link MetricsPersistence#writeCommitReport} method. If the
   * persistence backend doesn't override the default implementation, this is a no-op.
   *
   * <p>The record should contain all request context fields (principalName, requestId, otelTraceId,
   * otelSpanId) populated by the caller.
   *
   * @param callCtx the call context containing the persistence layer
   * @param record the commit metrics record to persist (including request context)
   */
  default void writeCommitMetrics(
      @Nonnull PolarisCallContext callCtx, @Nonnull CommitMetricsRecord record) {
    callCtx.getMetaStore().writeCommitReport(record);
  }
}
