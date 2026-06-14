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
package org.apache.polaris.core.metrics;

import com.google.common.annotations.Beta;
import java.time.Instant;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;

/**
 * SPI for reporting Iceberg metrics received by Polaris.
 *
 * <p>Implementations receive a resolved table context (catalog name/id and table name/id) along
 * with the raw Iceberg {@link MetricsReport}. Custom implementations should be annotated with the
 * appropriate CDI scope and {@code @Identifier("my-type")} for selection via the {@code
 * polaris.iceberg-metrics.reporting.type} configuration property.
 *
 * <p>This interface is intentionally runtime/framework-agnostic. CDI and configuration concerns
 * belong in the implementing class, not here.
 */
@Beta
public interface IcebergMetricsReporter {

  /**
   * Reports an Iceberg metrics report for a resolved table.
   *
   * @param catalogName the name of the catalog containing the table
   * @param catalogId the internal Polaris ID of the catalog
   * @param table the identifier of the table the metrics are for
   * @param tableId the internal Polaris ID of the table entity
   * @param metricsReport the Iceberg metrics report
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
