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

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Optional;

/**
 * Configuration for metrics processing in Polaris.
 *
 * <p>This configuration controls how Iceberg metrics reports are processed and persisted. The
 * processor type determines which implementation is used.
 *
 * <p>Example configuration:
 *
 * <pre>
 * polaris:
 *   metrics:
 *     processor:
 *       type: persistence
 *       retention:
 *         enabled: true
 *         retention-period: P30D
 *         cleanup-interval: PT6H
 * </pre>
 */
@ConfigMapping(prefix = "polaris.metrics.processor")
public interface MetricsProcessorConfiguration {

  /**
   * The type of metrics processor to use.
   *
   * <p>Supported built-in values:
   *
   * <ul>
   *   <li>{@code noop} - No processing, discards all metrics (default)
   *   <li>{@code logging} - Log metrics to console for debugging
   *   <li>{@code persistence} - Persist to dedicated metrics tables
   * </ul>
   *
   * <p>Custom processor types can be specified if a corresponding {@link MetricsProcessor}
   * implementation is available with a matching {@link io.smallrye.common.annotation.Identifier}.
   *
   * @return the processor type identifier
   */
  @WithDefault("noop")
  String type();

  /**
   * Retention policy configuration for persisted metrics reports.
   *
   * @return the retention configuration
   */
  Optional<Retention> retention();

  /** Retention policy configuration for metrics reports. */
  interface Retention {

    /**
     * Whether automatic cleanup of old metrics reports is enabled.
     *
     * @return true if cleanup is enabled
     */
    @WithDefault("false")
    boolean enabled();

    /**
     * How long to retain metrics reports before they are eligible for deletion.
     *
     * @return the retention period (default: 30 days)
     */
    @WithDefault("P30D")
    Duration retentionPeriod();

    /**
     * How often to run the cleanup job.
     *
     * @return the cleanup interval (default: 6 hours)
     */
    @WithDefault("PT6H")
    Duration cleanupInterval();
  }
}
