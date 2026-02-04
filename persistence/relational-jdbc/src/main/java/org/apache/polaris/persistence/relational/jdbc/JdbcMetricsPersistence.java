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
package org.apache.polaris.persistence.relational.jdbc;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.MetricsQueryCriteria;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;

/**
 * JDBC implementation of {@link MetricsPersistence}.
 *
 * <p>This class bridges the SPI interface with the existing JDBC persistence implementation,
 * converting between SPI record types ({@link ScanMetricsRecord}, {@link CommitMetricsRecord}) and
 * JDBC model types ({@link ModelScanMetricsReport}, {@link ModelCommitMetricsReport}).
 */
public class JdbcMetricsPersistence implements MetricsPersistence {

  private final JdbcBasePersistenceImpl jdbcPersistence;
  private final String realmId;

  /**
   * Creates a new JdbcMetricsPersistence instance.
   *
   * @param jdbcPersistence the underlying JDBC persistence implementation
   * @param realmId the realm ID for multi-tenancy
   */
  public JdbcMetricsPersistence(JdbcBasePersistenceImpl jdbcPersistence, String realmId) {
    this.jdbcPersistence = jdbcPersistence;
    this.realmId = realmId;
  }

  @Override
  public void writeScanReport(@Nonnull ScanMetricsRecord record) {
    if (!jdbcPersistence.supportsMetricsPersistence()) {
      return;
    }
    ModelScanMetricsReport model = SpiModelConverter.toModelScanReport(record, realmId);
    jdbcPersistence.writeScanMetricsReport(model);
  }

  @Override
  public void writeCommitReport(@Nonnull CommitMetricsRecord record) {
    if (!jdbcPersistence.supportsMetricsPersistence()) {
      return;
    }
    ModelCommitMetricsReport model = SpiModelConverter.toModelCommitReport(record, realmId);
    jdbcPersistence.writeCommitMetricsReport(model);
  }

  @Override
  @Nonnull
  public Page<ScanMetricsRecord> queryScanReports(
      @Nonnull MetricsQueryCriteria criteria, @Nonnull PageToken pageToken) {
    if (!jdbcPersistence.supportsMetricsPersistence()) {
      return Page.fromItems(List.of());
    }

    int limit = pageToken.pageSize().orElse(100);
    Long startTimeMs = criteria.startTime().map(t -> t.toEpochMilli()).orElse(null);
    Long endTimeMs = criteria.endTime().map(t -> t.toEpochMilli()).orElse(null);

    List<ModelScanMetricsReport> models =
        jdbcPersistence.queryScanMetricsReports(
            criteria.catalogName().orElse(""),
            criteria.namespace().orElse(""),
            criteria.tableName().orElse(""),
            startTimeMs,
            endTimeMs,
            limit);

    List<ScanMetricsRecord> records =
        models.stream().map(SpiModelConverter::toScanMetricsRecord).collect(Collectors.toList());

    return Page.fromItems(records);
  }

  @Override
  @Nonnull
  public Page<CommitMetricsRecord> queryCommitReports(
      @Nonnull MetricsQueryCriteria criteria, @Nonnull PageToken pageToken) {
    if (!jdbcPersistence.supportsMetricsPersistence()) {
      return Page.fromItems(List.of());
    }

    int limit = pageToken.pageSize().orElse(100);
    Long startTimeMs = criteria.startTime().map(t -> t.toEpochMilli()).orElse(null);
    Long endTimeMs = criteria.endTime().map(t -> t.toEpochMilli()).orElse(null);

    List<ModelCommitMetricsReport> models =
        jdbcPersistence.queryCommitMetricsReports(
            criteria.catalogName().orElse(""),
            criteria.namespace().orElse(""),
            criteria.tableName().orElse(""),
            startTimeMs,
            endTimeMs,
            limit);

    List<CommitMetricsRecord> records =
        models.stream().map(SpiModelConverter::toCommitMetricsRecord).collect(Collectors.toList());

    return Page.fromItems(records);
  }
}
