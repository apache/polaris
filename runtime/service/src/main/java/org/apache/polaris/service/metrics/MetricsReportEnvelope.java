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
import java.time.Instant;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.polaris.core.entity.PolarisEntity;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Stable, self-describing carrier for an Iceberg metrics report received by Polaris.
 *
 * <p>Bundles catalog context and an optional resolved table entity with the raw Iceberg {@link
 * MetricsReport} and an explicit {@link MetricType} discriminator, plus the timestamp when the
 * metrics were received. {@link #tableEntity()} may be {@code null} when the leaf could not be
 * resolved as a local {@code TABLE_LIKE} (for example passthrough-facade tables); reporters that
 * need a table id must handle that themselves. Passing an envelope keeps the {@link
 * IcebergMetricsReporter} SPI signature stable as new context fields are added, and lets
 * implementations branch on {@link #metricType()} without inspecting the concrete Iceberg report
 * type.
 *
 * @param catalogName the name of the catalog containing the table
 * @param catalogId the internal Polaris ID of the catalog
 * @param table the identifier of the table the metrics are for
 * @param tableEntity the resolved table leaf entity, or {@code null} if unresolved / not {@code
 *     TABLE_LIKE}
 * @param metricType the kind of metrics report ({@link MetricType#SCAN} or {@link
 *     MetricType#COMMIT})
 * @param report the Iceberg metrics report ({@link org.apache.iceberg.metrics.ScanReport} or {@link
 *     org.apache.iceberg.metrics.CommitReport})
 * @param receivedTimestamp the timestamp when the metrics were received by Polaris
 */
@Beta
public record MetricsReportEnvelope(
    @NonNull String catalogName,
    long catalogId,
    @NonNull TableIdentifier table,
    @Nullable PolarisEntity tableEntity,
    @NonNull MetricType metricType,
    @NonNull MetricsReport report,
    @NonNull Instant receivedTimestamp) {}
