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
import jakarta.annotation.Nullable;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RequestIdSupplier;
import org.apache.polaris.core.persistence.BasePersistence;

/**
 * Interface for managing Iceberg metrics persistence through the metastore manager layer.
 *
 * <p>This follows the same pattern as {@link org.apache.polaris.core.entity.PolarisEventManager},
 * providing a high-level interface that delegates to the underlying persistence layer when metrics
 * persistence is supported.
 *
 * <p>The service layer should interact with this interface (via {@link
 * org.apache.polaris.core.persistence.PolarisMetaStoreManager}) rather than directly accessing
 * persistence implementations.
 */
public interface PolarisMetricsManager {

  /**
   * Writes a scan metrics record to the persistence layer.
   *
   * <p>If the underlying persistence does not support metrics, this method is a no-op.
   *
   * @param callCtx the call context containing the persistence layer
   * @param record the scan metrics record to persist
   * @param principal the authenticated principal (may be null)
   * @param requestIdSupplier supplier for the request ID (may be null)
   */
  default void writeScanMetrics(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull ScanMetricsRecord record,
      @Nullable PolarisPrincipal principal,
      @Nullable RequestIdSupplier requestIdSupplier) {
    BasePersistence ms = callCtx.getMetaStore();
    if (ms instanceof MetricsPersistence metricsPersistence) {
      metricsPersistence.setMetricsRequestContext(principal, requestIdSupplier);
      metricsPersistence.writeScanReport(record);
    }
    // If persistence doesn't support metrics, silently ignore
  }

  /**
   * Writes a commit metrics record to the persistence layer.
   *
   * <p>If the underlying persistence does not support metrics, this method is a no-op.
   *
   * @param callCtx the call context containing the persistence layer
   * @param record the commit metrics record to persist
   * @param principal the authenticated principal (may be null)
   * @param requestIdSupplier supplier for the request ID (may be null)
   */
  default void writeCommitMetrics(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull CommitMetricsRecord record,
      @Nullable PolarisPrincipal principal,
      @Nullable RequestIdSupplier requestIdSupplier) {
    BasePersistence ms = callCtx.getMetaStore();
    if (ms instanceof MetricsPersistence metricsPersistence) {
      metricsPersistence.setMetricsRequestContext(principal, requestIdSupplier);
      metricsPersistence.writeCommitReport(record);
    }
    // If persistence doesn't support metrics, silently ignore
  }
}
