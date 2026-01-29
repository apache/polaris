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

import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Context information needed when converting Iceberg metrics reports to persistence records.
 *
 * <p>This context captures information from the request environment that is not available in the
 * Iceberg report itself, such as realm, catalog, principal, and tracing information.
 */
@PolarisImmutable
public interface MetricsContext {

  /** Multi-tenancy realm identifier. */
  String realmId();

  /** Internal catalog ID. */
  String catalogId();

  /** Human-readable catalog name. */
  String catalogName();

  /** Dot-separated namespace path (e.g., "db.schema"). */
  String namespace();

  /** Name of the principal who initiated the operation. */
  Optional<String> principalName();

  /** Request ID for correlation. */
  Optional<String> requestId();

  /** OpenTelemetry trace ID for distributed tracing. */
  Optional<String> otelTraceId();

  /** OpenTelemetry span ID for distributed tracing. */
  Optional<String> otelSpanId();

  /**
   * Creates a new builder for MetricsContext.
   *
   * @return a new builder instance
   */
  static ImmutableMetricsContext.Builder builder() {
    return ImmutableMetricsContext.builder();
  }
}
