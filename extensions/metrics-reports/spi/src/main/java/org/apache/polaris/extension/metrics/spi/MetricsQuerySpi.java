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
package org.apache.polaris.extension.metrics.spi;

import com.google.common.annotations.Beta;
import java.util.List;
import java.util.Set;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsRecordIdentity;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * SPI for querying persisted Iceberg metrics reports.
 *
 * <p>Implementations are provided by persistence-backend extension modules (e.g. {@code
 * polaris-extensions-metrics-reports-jdbc}). When no durable implementation is installed, the no-op
 * default in {@code polaris-extensions-metrics-reports} is used and the read path returns an empty
 * page.
 *
 * @see org.apache.polaris.core.persistence.metrics.MetricsPersistence for the corresponding write
 *     SPI
 */
@Beta
public interface MetricsQuerySpi {

  /** Discriminates which kinds of metrics reports {@link #listReports} should query. */
  enum MetricType {
    SCAN,
    COMMIT
  }

  /**
   * Selects the reports of one table, optionally restricted to a single snapshot.
   *
   * @param tableId internal table entity ID
   * @param snapshotId snapshot to match, or {@code null} to match reports of every snapshot
   */
  record TableSelector(long tableId, @Nullable Long snapshotId) {}

  /**
   * Lists persisted metrics reports of the given {@link MetricType}s matching any of the given
   * table selectors, applying the supplied filters and returning at most one page of results merged
   * across all requested types and tables, ordered by timestamp descending.
   *
   * <p>Selectors are combined with OR semantics, and a report matched by more than one selector is
   * returned only once. Each item of the returned page is a {@link ScanMetricsRecord} for {@link
   * MetricType#SCAN} or a {@link CommitMetricsRecord} for {@link MetricType#COMMIT}.
   *
   * @param tables table selectors, all referring to tables belonging to {@code catalogId}
   */
  Page<MetricsRecordIdentity> listReports(
      @NonNull Set<MetricType> metricTypes,
      long catalogId,
      @NonNull List<TableSelector> tables,
      @Nullable Long timestampFrom,
      @Nullable Long timestampTo,
      @NonNull PageToken pageToken);
}
