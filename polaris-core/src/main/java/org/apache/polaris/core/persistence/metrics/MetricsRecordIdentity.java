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
package org.apache.polaris.core.persistence.metrics;

import com.google.common.annotations.Beta;
import java.time.Instant;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Base interface containing common identification fields shared by all metrics records.
 *
 * <p>Both {@link ScanMetricsRecord} and {@link CommitMetricsRecord} extend this interface to
 * inherit these common fields while adding their own specific metrics.
 *
 * <p><b>Note:</b> This type is part of the experimental Metrics Persistence SPI and may change in
 * future releases.
 */
@Beta
public interface MetricsRecordIdentity {

  /** Unique identifier for this report (UUID). */
  String reportId();

  /** Internal catalog ID. */
  long catalogId();

  /** Internal table entity ID. */
  long tableId();

  /** Timestamp when the report was received. */
  Instant timestamp();

  /** Additional metadata as key-value pairs. */
  Map<String, String> metadata();

  /** Name of the principal who made the request. */
  @Nullable String principalName();

  /** Server-generated request ID for correlation. */
  @Nullable String requestId();

  /** OpenTelemetry trace ID for distributed tracing correlation. */
  @Nullable String otelTraceId();

  /** OpenTelemetry span ID for distributed tracing correlation. */
  @Nullable String otelSpanId();
}
