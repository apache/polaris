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
package org.apache.polaris.service.metrics;

import com.google.common.annotations.Beta;

/**
 * Discriminator for the kind of Iceberg metrics report carried by a {@link MetricsReportEnvelope}.
 *
 * <p>Making the type explicit at the SPI boundary means {@link IcebergMetricsReporter}
 * implementations do not need to {@code instanceof}-check the raw Iceberg report.
 */
@Beta
public enum MetricType {
  /** An Iceberg scan report ({@link org.apache.iceberg.metrics.ScanReport}). */
  SCAN,
  /** An Iceberg commit report ({@link org.apache.iceberg.metrics.CommitReport}). */
  COMMIT
}
