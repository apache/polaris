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
import org.apache.commons.lang3.function.TriConsumer;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Identifier("default")
public class DefaultMetricsReporter implements PolarisMetricsReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetricsReporter.class);

  private final TriConsumer<String, TableIdentifier, MetricsReport> reportConsumer;

  public DefaultMetricsReporter() {
    this(
        (catalogName, table, metricsReport) ->
            LOGGER.info("{}.{}: {}", catalogName, table, metricsReport));
  }

  @VisibleForTesting
  DefaultMetricsReporter(TriConsumer<String, TableIdentifier, MetricsReport> reportConsumer) {
    this.reportConsumer = reportConsumer;
  }

  @Override
  public void reportMetric(String catalogName, TableIdentifier table, MetricsReport metricsReport) {
    reportConsumer.accept(catalogName, table, metricsReport);
  }
}
