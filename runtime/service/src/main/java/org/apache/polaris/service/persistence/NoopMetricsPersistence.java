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
package org.apache.polaris.service.persistence;

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.MetricsQueryCriteria;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;

/**
 * A CDI-managed no-op implementation of {@link MetricsPersistence}.
 *
 * <p>This bean is selected when {@code polaris.persistence.metrics.type} is set to {@code "noop"}
 * (the default). All write operations are silently ignored, and all query operations return empty
 * pages.
 *
 * <p>This is useful when metrics persistence is not needed or when the persistence backend does not
 * support metrics storage.
 *
 * @see MetricsPersistence#NOOP
 */
@ApplicationScoped
@Identifier("noop")
public class NoopMetricsPersistence implements MetricsPersistence {

  @Override
  public void writeScanReport(@Nonnull ScanMetricsRecord record) {
    MetricsPersistence.NOOP.writeScanReport(record);
  }

  @Override
  public void writeCommitReport(@Nonnull CommitMetricsRecord record) {
    MetricsPersistence.NOOP.writeCommitReport(record);
  }

  @Nonnull
  @Override
  public Page<ScanMetricsRecord> queryScanReports(
      @Nonnull MetricsQueryCriteria criteria, @Nonnull PageToken pageToken) {
    return MetricsPersistence.NOOP.queryScanReports(criteria, pageToken);
  }

  @Nonnull
  @Override
  public Page<CommitMetricsRecord> queryCommitReports(
      @Nonnull MetricsQueryCriteria criteria, @Nonnull PageToken pageToken) {
    return MetricsPersistence.NOOP.queryCommitReports(criteria, pageToken);
  }
}
