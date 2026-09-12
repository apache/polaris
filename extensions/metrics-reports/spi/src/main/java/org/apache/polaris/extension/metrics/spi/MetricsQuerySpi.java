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
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * SPI for querying persisted Iceberg metrics reports.
 *
 * <p>Implementations are provided by persistence-backend extension modules (e.g. {@code
 * polaris-extensions-metrics-reports-jdbc}). When no implementation is on the classpath, the read
 * path returns HTTP 501 Not Implemented.
 *
 * @see org.apache.polaris.core.persistence.metrics.MetricsPersistence for the corresponding write
 *     SPI
 */
@Beta
public interface MetricsQuerySpi {

  /** Discriminates which kind of metrics report {@link #listReports} should query. */
  enum MetricType {
    SCAN,
    COMMIT
  }

  /**
   * Result of {@link #listReports}, pairing the requested {@link MetricType} with the page of
   * records of the matching record type so callers can switch exhaustively without casting.
   */
  sealed interface QueryResult permits ScanResult, CommitResult {
    MetricType metricType();
  }

  record ScanResult(Page<ScanMetricsRecord> reports) implements QueryResult {
    @Override
    public MetricType metricType() {
      return MetricType.SCAN;
    }
  }

  record CommitResult(Page<CommitMetricsRecord> reports) implements QueryResult {
    @Override
    public MetricType metricType() {
      return MetricType.COMMIT;
    }
  }

  /**
   * Lists persisted metrics reports of the given {@link MetricType} for the given tables, applying
   * the supplied filters and returning at most one page of results merged across all requested
   * tables.
   *
   * <p>The returned {@link QueryResult#metricType()} must equal {@code metricType}: {@code SCAN}
   * must yield a {@link ScanResult} and {@code COMMIT} must yield a {@link CommitResult}.
   *
   * @param tableIds internal table entity IDs to query, all belonging to {@code catalogId}
   */
  QueryResult listReports(
      @NonNull MetricType metricType,
      long catalogId,
      @NonNull List<Long> tableIds,
      @Nullable Long snapshotId,
      @Nullable Long timestampFrom,
      @Nullable Long timestampTo,
      @NonNull PageToken pageToken);
}
