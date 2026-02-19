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
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.metrics.iceberg.MetricsRecordConverter;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link PolarisMetricsReporter} that persists metrics to the configured {@link
 * MetricsPersistence} backend.
 *
 * <p>This reporter is selected when {@code polaris.iceberg-metrics.reporting.type} is set to {@code
 * "persisting"}.
 *
 * <p>The reporter looks up the catalog entity by name to obtain the catalog ID, then uses {@link
 * MetricsRecordConverter} to convert Iceberg metrics reports to SPI records before persisting them.
 *
 * @see PolarisMetricsReporter
 * @see MetricsPersistence
 * @see MetricsRecordConverter
 */
@RequestScoped
@Identifier("persisting")
public class PersistingMetricsReporter implements PolarisMetricsReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PersistingMetricsReporter.class);

  private final CallContext callContext;
  private final PolarisMetaStoreManager metaStoreManager;
  private final MetricsPersistence metricsPersistence;

  @Inject
  public PersistingMetricsReporter(
      CallContext callContext,
      PolarisMetaStoreManager metaStoreManager,
      MetricsPersistence metricsPersistence) {
    this.callContext = callContext;
    this.metaStoreManager = metaStoreManager;
    this.metricsPersistence = metricsPersistence;
  }

  @Override
  public void reportMetric(
      String catalogName,
      TableIdentifier table,
      MetricsReport metricsReport,
      Instant receivedTimestamp) {

    // Look up the catalog entity to get the catalog ID
    EntityResult catalogResult =
        metaStoreManager.readEntityByName(
            callContext.getPolarisCallContext(),
            null, // catalogPath is null for top-level entities
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.ANY_SUBTYPE,
            catalogName);

    if (!catalogResult.isSuccess()) {
      LOGGER.warn(
          "Failed to find catalog '{}' for metrics persistence. Metrics will not be stored.",
          catalogName);
      return;
    }

    PolarisBaseEntity catalogEntity = catalogResult.getEntity();
    long catalogId = catalogEntity.getId();

    // Build the full path from catalog through namespace to resolve the table.
    // The path contains the catalog, then each namespace level.
    // The last element in the path becomes the parent for the lookup.
    List<PolarisEntityCore> entityPath = new ArrayList<>();
    entityPath.add(PolarisEntity.toCore(catalogEntity));

    // Resolve each namespace level
    String[] namespaceLevels = table.namespace().levels();
    for (String nsLevel : namespaceLevels) {
      EntityResult nsResult =
          metaStoreManager.readEntityByName(
              callContext.getPolarisCallContext(),
              entityPath,
              PolarisEntityType.NAMESPACE,
              PolarisEntitySubType.ANY_SUBTYPE,
              nsLevel);

      if (!nsResult.isSuccess()) {
        LOGGER.warn(
            "Failed to find namespace '{}' in catalog '{}' for metrics persistence. Metrics will not be stored.",
            nsLevel,
            catalogName);
        return;
      }
      entityPath.add(PolarisEntity.toCore(nsResult.getEntity()));
    }

    // Now look up the table with the full namespace path
    EntityResult tableResult =
        metaStoreManager.readEntityByName(
            callContext.getPolarisCallContext(),
            entityPath,
            PolarisEntityType.TABLE_LIKE,
            PolarisEntitySubType.ANY_SUBTYPE,
            table.name());

    if (!tableResult.isSuccess()) {
      LOGGER.warn(
          "Failed to find table '{}' in catalog '{}' for metrics persistence. Metrics will not be stored.",
          table,
          catalogName);
      return;
    }

    long tableId = tableResult.getEntity().getId();

    if (metricsReport instanceof ScanReport scanReport) {
      ScanMetricsRecord record =
          MetricsRecordConverter.forScanReport(scanReport)
              .catalogId(catalogId)
              .tableId(tableId)
              .timestamp(receivedTimestamp)
              .build();
      metricsPersistence.writeScanReport(record);
      LOGGER.debug(
          "Persisted scan metrics for {}.{} (reportId={})", catalogName, table, record.reportId());
    } else if (metricsReport instanceof CommitReport commitReport) {
      CommitMetricsRecord record =
          MetricsRecordConverter.forCommitReport(commitReport)
              .catalogId(catalogId)
              .tableId(tableId)
              .timestamp(receivedTimestamp)
              .build();
      metricsPersistence.writeCommitReport(record);
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
