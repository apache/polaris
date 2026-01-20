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
 * Adapter that bridges the legacy {@link PolarisMetricsReporter} interface to the new {@link
 * MetricsProcessor} interface.
 *
 * <p>This adapter allows existing {@link PolarisMetricsReporter} implementations to work with the
 * new {@link MetricsProcessor} system, providing backward compatibility during the migration
 * period.
 *
 * <p>The adapter converts the simple {@link PolarisMetricsReporter#reportMetric(String,
 * TableIdentifier, MetricsReport)} call into a full {@link MetricsProcessingContext} by extracting
 * available information from the current request context.
 */
public class MetricsReporterToProcessorAdapter implements MetricsProcessor {

  private final PolarisMetricsReporter reporter;

  public MetricsReporterToProcessorAdapter(PolarisMetricsReporter reporter) {
    this.reporter = reporter;
  }

  @Override
  public void process(MetricsProcessingContext context) {
    // Delegate to the legacy reporter interface with just the basic parameters
    reporter.reportMetric(
        context.catalogName(),
        context.tableIdentifier(),
        context.metricsReport(),
        java.time.Instant.ofEpochMilli(context.timestampMs()));
  }

  /**
   * Get the underlying legacy reporter.
   *
   * @return the wrapped PolarisMetricsReporter
   */
  public PolarisMetricsReporter getReporter() {
    return reporter;
  }
}
