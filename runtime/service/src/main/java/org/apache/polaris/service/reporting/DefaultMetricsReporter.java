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

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link PolarisMetricsReporter} that logs metrics to the configured
 * logger.
 *
 * <p>This implementation is selected when {@code polaris.iceberg-metrics.reporting.type} is set to
 * {@code "default"} (the default value).
 *
 * <p>By default, logging is disabled. To enable metrics logging, set the logger level for {@code
 * org.apache.polaris.service.reporting} to {@code INFO} in your logging configuration.
 *
 * @see PolarisMetricsReporter
 */
@ApplicationScoped
@Identifier("default")
public class DefaultMetricsReporter implements PolarisMetricsReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetricsReporter.class);

  private final QuadConsumer<String, TableIdentifier, MetricsReport, Long> reportConsumer;

  /** Functional interface for consuming metrics reports with timestamp. */
  @FunctionalInterface
  interface QuadConsumer<T1, T2, T3, T4> {
    void accept(T1 t1, T2 t2, T3 t3, T4 t4);
  }

  /** Creates a new DefaultMetricsReporter that logs metrics to the class logger. */
  public DefaultMetricsReporter() {
    this(
        (catalogName, table, metricsReport, timestampMs) ->
            LOGGER.info("{}.{} (ts={}): {}", catalogName, table, timestampMs, metricsReport));
  }

  @VisibleForTesting
  DefaultMetricsReporter(
      QuadConsumer<String, TableIdentifier, MetricsReport, Long> reportConsumer) {
    this.reportConsumer = reportConsumer;
  }

  @Override
  public void reportMetric(
      String catalogName, TableIdentifier table, MetricsReport metricsReport, long timestampMs) {
    reportConsumer.accept(catalogName, table, metricsReport, timestampMs);
  }
}
