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

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;

/**
 * SPI interface for reporting Iceberg metrics received by Polaris.
 *
 * <p>Implementations can be used to send metrics to external systems for analysis and monitoring.
 * Custom implementations must be annotated with {@link
 * jakarta.enterprise.context.ApplicationScoped @ApplicationScoped} and {@link
 * io.smallrye.common.annotation.Identifier @Identifier("my-reporter-type")} for CDI discovery.
 *
 * <p>The implementation to use is selected via the {@code polaris.iceberg-metrics.reporting.type}
 * configuration property, which defaults to {@code "default"}.
 *
 * <p>Custom implementations that need access to request-scoped context (such as realm information
 * or principal details) should inject the appropriate CDI beans (e.g., {@code RealmContext}, {@code
 * PolarisPrincipalHolder}) rather than expecting this data to be passed as parameters.
 *
 * <p>Example implementation:
 *
 * <pre>{@code
 * @ApplicationScoped
 * @Identifier("custom")
 * public class CustomMetricsReporter implements PolarisMetricsReporter {
 *
 *   @Inject RealmContext realmContext;
 *
 *   @Override
 *   public void reportMetric(
 *       String catalogName, TableIdentifier table, MetricsReport metricsReport, long timestampMs) {
 *     // Send metrics to external system
 *   }
 * }
 * }</pre>
 *
 * @see DefaultMetricsReporter
 * @see MetricsReportingConfiguration
 */
public interface PolarisMetricsReporter {

  /**
   * Reports an Iceberg metrics report for a specific table.
   *
   * @param catalogName the name of the catalog containing the table
   * @param table the identifier of the table the metrics are for
   * @param metricsReport the Iceberg metrics report (e.g., {@link
   *     org.apache.iceberg.metrics.ScanReport} or {@link org.apache.iceberg.metrics.CommitReport})
   * @param timestampMs the timestamp in milliseconds when the metrics were received
   */
  void reportMetric(
      String catalogName, TableIdentifier table, MetricsReport metricsReport, long timestampMs);

  /**
   * Reports an Iceberg metrics report for a specific table.
   *
   * @param catalogName the name of the catalog containing the table
   * @param table the identifier of the table the metrics are for
   * @param metricsReport the Iceberg metrics report
   * @deprecated Use {@link #reportMetric(String, TableIdentifier, MetricsReport, long)} instead.
   *     This method is provided for backward compatibility and will be removed in a future release.
   */
  @Deprecated
  default void reportMetric(
      String catalogName, TableIdentifier table, MetricsReport metricsReport) {
    reportMetric(catalogName, table, metricsReport, System.currentTimeMillis());
  }
}
