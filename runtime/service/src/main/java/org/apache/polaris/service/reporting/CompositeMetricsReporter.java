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

import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A composite metrics reporter that delegates to multiple child reporters. This allows metrics to
 * be sent to multiple destinations simultaneously, such as both the events table and dedicated
 * metrics tables.
 *
 * <p>To enable this reporter, set the configuration:
 *
 * <pre>
 * polaris:
 *   iceberg-metrics:
 *     reporting:
 *       type: composite
 *       targets:
 *         - events       # Write to events table
 *         - persistence  # Write to dedicated tables
 * </pre>
 *
 * <p>The composite reporter will call each configured target reporter in order. If one reporter
 * fails, the others will still be called.
 */
public class CompositeMetricsReporter implements PolarisMetricsReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompositeMetricsReporter.class);

  private final List<PolarisMetricsReporter> delegates;

  /**
   * Creates a composite reporter with the given delegate reporters.
   *
   * @param delegates the list of reporters to delegate to
   */
  public CompositeMetricsReporter(List<PolarisMetricsReporter> delegates) {
    this.delegates = List.copyOf(delegates);
    LOGGER.info(
        "CompositeMetricsReporter initialized with {} delegate(s): {}",
        delegates.size(),
        delegates.stream().map(r -> r.getClass().getSimpleName()).toList());
  }

  @Override
  public void reportMetric(String catalogName, TableIdentifier table, MetricsReport metricsReport) {
    for (PolarisMetricsReporter delegate : delegates) {
      try {
        delegate.reportMetric(catalogName, table, metricsReport);
      } catch (Exception e) {
        LOGGER.error(
            "Delegate reporter {} failed for table {}.{}: {}",
            delegate.getClass().getSimpleName(),
            catalogName,
            table,
            e.getMessage(),
            e);
        // Continue with other delegates even if one fails
      }
    }
  }

  /**
   * Returns the list of delegate reporters.
   *
   * @return unmodifiable list of delegates
   */
  public List<PolarisMetricsReporter> getDelegates() {
    return delegates;
  }
}
