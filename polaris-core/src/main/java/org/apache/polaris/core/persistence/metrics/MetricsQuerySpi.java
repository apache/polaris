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
 * @see MetricsPersistence for the corresponding write SPI
 */
@Beta
public interface MetricsQuerySpi {

  /**
   * Lists persisted scan metrics reports for the given table, applying the supplied filters and
   * returning at most one page of results.
   */
  Page<ScanMetricsRecord> listScanReports(
      long catalogId,
      long tableId,
      @Nullable Long snapshotId,
      @Nullable String principalName,
      @Nullable Long timestampFrom,
      @Nullable Long timestampTo,
      @NonNull PageToken pageToken);

  /**
   * Lists persisted commit metrics reports for the given table, applying the supplied filters and
   * returning at most one page of results.
   */
  Page<CommitMetricsRecord> listCommitReports(
      long catalogId,
      long tableId,
      @Nullable Long snapshotId,
      @Nullable String principalName,
      @Nullable Long timestampFrom,
      @Nullable Long timestampTo,
      @NonNull PageToken pageToken);
}
