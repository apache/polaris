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
package org.apache.polaris.extension.metrics.reports;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.polaris.core.metrics.IcebergMetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log-only implementation of {@link IcebergMetricsReporter}.
 *
 * <p>Selected when {@code polaris.iceberg-metrics.reporting.type} is set to {@code "log"}.
 *
 * <p>Logging is at INFO level. Enable it by setting the logger level for {@code
 * org.apache.polaris.extension.metrics.reports} to {@code INFO}.
 */
@ApplicationScoped
@Identifier("default")
public class LoggingMetricsReporter implements IcebergMetricsReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingMetricsReporter.class);

  @FunctionalInterface
  interface ReportConsumer {
    void accept(
        String catalogName,
        long catalogId,
        TableIdentifier table,
        long tableId,
        MetricsReport metricsReport,
        Instant receivedTimestamp);
  }

  private final ReportConsumer reportConsumer;

  public LoggingMetricsReporter() {
    this(
        (catalogName, catalogId, table, tableId, metricsReport, receivedTimestamp) ->
            LOGGER.info("{}.{} (ts={}): {}", catalogName, table, receivedTimestamp, metricsReport));
  }

  LoggingMetricsReporter(ReportConsumer reportConsumer) {
    this.reportConsumer = reportConsumer;
  }

  @Override
  public void reportMetric(
      String catalogName,
      long catalogId,
      TableIdentifier table,
      long tableId,
      MetricsReport metricsReport,
      Instant receivedTimestamp) {
    reportConsumer.accept(catalogName, catalogId, table, tableId, metricsReport, receivedTimestamp);
  }
}
