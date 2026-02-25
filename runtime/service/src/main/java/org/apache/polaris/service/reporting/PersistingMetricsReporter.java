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
package org.apache.polaris.service.reporting;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Instant;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RequestIdSupplier;
import org.apache.polaris.core.metrics.iceberg.MetricsRecordConverter;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link PolarisMetricsReporter} that persists metrics using the {@link
 * PolarisMetaStoreManager} from the current {@link CallContext}.
 *
 * <p>This reporter is selected when {@code polaris.iceberg-metrics.reporting.type} is set to {@code
 * "persisting"}.
 *
 * <p>The reporter uses {@link PolarisMetaStoreManager} to persist metrics, following the same
 * abstraction pattern as other Polaris operations. If the underlying persistence does not support
 * metrics, they are silently discarded.
 *
 * <p>The reporter receives catalog and table IDs from the caller (already resolved during
 * authorization), avoiding redundant entity lookups. It uses {@link MetricsRecordConverter} to
 * convert Iceberg metrics reports to SPI records before persisting them.
 *
 * @see PolarisMetricsReporter
 * @see PolarisMetaStoreManager
 * @see MetricsRecordConverter
 */
@RequestScoped
@Identifier("persisting")
public class PersistingMetricsReporter implements PolarisMetricsReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PersistingMetricsReporter.class);

  private final CallContext callContext;
  private final PolarisMetaStoreManager metaStoreManager;
  private final Instance<PolarisPrincipal> polarisPrincipal;
  private final Instance<RequestIdSupplier> requestIdSupplier;

  @Inject
  public PersistingMetricsReporter(
      CallContext callContext,
      PolarisMetaStoreManager metaStoreManager,
      Instance<PolarisPrincipal> polarisPrincipal,
      Instance<RequestIdSupplier> requestIdSupplier) {
    this.callContext = callContext;
    this.metaStoreManager = metaStoreManager;
    this.polarisPrincipal = polarisPrincipal;
    this.requestIdSupplier = requestIdSupplier;
  }

  @Override
  public void reportMetric(
      String catalogName,
      long catalogId,
      TableIdentifier table,
      long tableId,
      MetricsReport metricsReport,
      Instant receivedTimestamp) {

    PolarisPrincipal principal = polarisPrincipal.isResolvable() ? polarisPrincipal.get() : null;
    RequestIdSupplier supplier =
        requestIdSupplier.isResolvable() ? requestIdSupplier.get() : RequestIdSupplier.NOOP;

    if (metricsReport instanceof ScanReport scanReport) {
      ScanMetricsRecord record =
          MetricsRecordConverter.forScanReport(scanReport)
              .catalogId(catalogId)
              .tableId(tableId)
              .timestamp(receivedTimestamp)
              .build();
      metaStoreManager.writeScanMetrics(
          callContext.getPolarisCallContext(), record, principal, supplier);
      LOGGER.debug(
          "Persisted scan metrics for {}.{} (reportId={})", catalogName, table, record.reportId());
    } else if (metricsReport instanceof CommitReport commitReport) {
      CommitMetricsRecord record =
          MetricsRecordConverter.forCommitReport(commitReport)
              .catalogId(catalogId)
              .tableId(tableId)
              .timestamp(receivedTimestamp)
              .build();
      metaStoreManager.writeCommitMetrics(
          callContext.getPolarisCallContext(), record, principal, supplier);
      LOGGER.debug(
          "Persisted commit metrics for {}.{} (reportId={})",
          catalogName,
          table,
          record.reportId());
    } else {
      LOGGER.warn(
          "Unknown metrics report type: {}. Metrics will not be stored.",
          metricsReport.getClass().getName());
    }
  }
}
