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
 * Interface for reporting Iceberg metrics in Polaris.
 *
 * <p>Implementations of this interface handle the persistence or forwarding of Iceberg metrics
 * reports (such as {@link org.apache.iceberg.metrics.ScanReport} and {@link
 * org.apache.iceberg.metrics.CommitReport}) to various backends.
 *
 * <p>Available implementations:
 *
 * <ul>
 *   <li>{@code default} - Logs metrics to console only (no persistence)
 *   <li>{@code events} - Persists metrics to the events table as JSON
 *   <li>{@code persistence} - Persists metrics to dedicated tables (scan_metrics_report,
 *       commit_metrics_report)
 *   <li>{@code composite} - Delegates to multiple reporters based on configuration
 * </ul>
 *
 * @see MetricsReportingConfiguration
 */
public interface PolarisMetricsReporter {

  /**
   * Reports a metrics event for a table operation.
   *
   * @param catalogName the name of the catalog containing the table
   * @param table the identifier of the table the metrics are for
   * @param metricsReport the Iceberg metrics report (ScanReport or CommitReport)
   */
  void reportMetric(String catalogName, TableIdentifier table, MetricsReport metricsReport);
}
