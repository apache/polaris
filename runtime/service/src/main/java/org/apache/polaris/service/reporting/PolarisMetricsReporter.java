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

import java.time.Instant;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;

/**
 * SPI interface for reporting Iceberg metrics received by Polaris.
 *
 * <p>Implementations can be used to send metrics to external systems for analysis and monitoring.
 * Custom implementations can be annotated with appropriate {@code Quarkus} scope and {@link
 * io.smallrye.common.annotation.Identifier @Identifier("my-reporter-type")} for CDI discovery.
 *
 * <p>The implementation to use is selected via the {@code polaris.iceberg-metrics.reporting.type}
 * configuration property, which defaults to {@code "default"}.
 *
 * <p>Implementations can inject other CDI beans for context.
 *
 * @see DefaultMetricsReporter
 * @see MetricsReportingConfiguration
 */
public interface PolarisMetricsReporter {

  /**
   * Reports an Iceberg metrics report for a specific table.
   *
   * @param catalogName the name of the catalog containing the table
   * @param catalogId the internal Polaris ID of the catalog
   * @param table the identifier of the table the metrics are for
   * @param tableId the internal Polaris ID of the table entity
   * @param metricsReport the Iceberg metrics report (e.g., {@link
   *     org.apache.iceberg.metrics.ScanReport} or {@link org.apache.iceberg.metrics.CommitReport})
   * @param receivedTimestamp the timestamp when the metrics were received by Polaris
   */
  void reportMetric(
      String catalogName,
      long catalogId,
      TableIdentifier table,
      long tableId,
      MetricsReport metricsReport,
      Instant receivedTimestamp);
}
