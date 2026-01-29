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

import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Context information needed when converting Iceberg metrics reports to persistence records.
 *
 * <p>This context captures information from the request environment that is not available in the
 * Iceberg report itself, such as realm and catalog identification.
 *
 * <p>Note: Principal and tracing information (e.g., OpenTelemetry trace/span IDs) are not included
 * in this context. The persistence implementation can obtain these from the ambient request context
 * (OTel context, security context) at write time if needed.
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

  /**
   * Creates a new builder for MetricsContext.
   *
   * @return a new builder instance
   */
  static ImmutableMetricsContext.Builder builder() {
    return ImmutableMetricsContext.builder();
  }
}
