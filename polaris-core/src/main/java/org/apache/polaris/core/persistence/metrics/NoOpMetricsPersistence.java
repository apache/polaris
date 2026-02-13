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
import java.util.Collections;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;

/**
 * A no-op implementation of {@link MetricsPersistence} for backends that don't support metrics
 * persistence.
 *
 * <p>This implementation is used as the default when a persistence backend does not support metrics
 * storage. All write operations are silently ignored, and all query operations return empty pages.
 */
final class NoOpMetricsPersistence implements MetricsPersistence {

  NoOpMetricsPersistence() {}

  @Override
  public void writeScanReport(@Nonnull ScanMetricsRecord record) {
    // No-op
  }

  @Override
  public void writeCommitReport(@Nonnull CommitMetricsRecord record) {
    // No-op
  }

  @Nonnull
  @Override
  public Page<ScanMetricsRecord> queryScanReports(
      @Nonnull MetricsQueryCriteria criteria, @Nonnull PageToken pageToken) {
    return Page.fromItems(Collections.emptyList());
  }

  @Nonnull
  @Override
  public Page<CommitMetricsRecord> queryCommitReports(
      @Nonnull MetricsQueryCriteria criteria, @Nonnull PageToken pageToken) {
    return Page.fromItems(Collections.emptyList());
  }
}
