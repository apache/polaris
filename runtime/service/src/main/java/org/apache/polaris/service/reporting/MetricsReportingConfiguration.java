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
import java.util.List;

@ConfigMapping(prefix = "polaris.iceberg-metrics.reporting")
public interface MetricsReportingConfiguration {
  /**
   * The type of metrics reporter to use. Supported values:
   *
   * <ul>
   *   <li>{@code default} - Log metrics to console only (no persistence)
   *   <li>{@code events} - Persist metrics to the events table as JSON
   *   <li>{@code persistence} - Persist metrics to dedicated tables (scan_metrics_report,
   *       commit_metrics_report)
   *   <li>{@code composite} - Use multiple reporters based on the {@link #targets()} configuration
   * </ul>
   *
   * @return the reporter type
   */
  @WithDefault("default")
  String type();

  /**
   * List of reporter targets to use when {@link #type()} is set to {@code composite}. Each target
   * corresponds to a reporter type: {@code default}, {@code events}, or {@code persistence}.
   *
   * <p>Example configuration:
   *
   * <pre>
   * polaris:
   *   iceberg-metrics:
   *     reporting:
   *       type: composite
   *       targets:
   *         - events
   *         - persistence
   * </pre>
   *
   * @return list of reporter targets, empty if not using composite type
   */
  default List<String> targets() {
    return List.of();
  }

  /** Configuration for metrics retention and cleanup. */
  RetentionConfig retention();

  interface RetentionConfig {
    /**
     * Whether automatic cleanup of old metrics reports is enabled. Default is false (disabled).
     *
     * @return true if cleanup is enabled
     */
    @WithDefault("false")
    boolean enabled();

    /**
     * How long to retain metrics reports before they are eligible for cleanup. Default is 30 days.
     * Supports ISO-8601 duration format (e.g., "P30D" for 30 days, "PT24H" for 24 hours).
     *
     * @return the retention period
     */
    @WithDefault("P30D")
    Duration retentionPeriod();

    /**
     * How often to run the cleanup job. Default is every 6 hours. Supports ISO-8601 duration
     * format.
     *
     * @return the cleanup interval
     */
    @WithDefault("PT6H")
    Duration cleanupInterval();
  }
}
