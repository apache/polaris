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

/**
 * Interface for processing Iceberg metrics reports in Polaris.
 *
 * <p>This interface provides a pluggable mechanism for handling metrics reports from Iceberg table
 * operations. Implementations can persist metrics to various backends, forward them to external
 * systems, or perform custom processing.
 *
 * <p>Processors are discovered via CDI using the {@link io.smallrye.common.annotation.Identifier}
 * annotation. Custom processors can be implemented and registered by annotating them with
 * {@code @ApplicationScoped} and {@code @Identifier("custom-name")}.
 *
 * <p>Available built-in processors:
 *
 * <ul>
 *   <li>{@code noop} - Discards all metrics (default)
 *   <li>{@code logging} - Logs metrics to console for debugging
 *   <li>{@code persistence} - Persists to dedicated metrics tables
 * </ul>
 *
 * <p>Example configuration:
 *
 * <pre>
 * polaris:
 *   metrics:
 *     processor:
 *       type: persistence
 * </pre>
 *
 * <p>Custom implementations should be annotated with:
 *
 * <pre>
 * {@literal @}ApplicationScoped
 * {@literal @}Identifier("custom-processor")
 * public class CustomMetricsProcessor implements MetricsProcessor {
 *   {@literal @}Override
 *   public void process(MetricsProcessingContext context) {
 *     // implementation
 *   }
 * }
 * </pre>
 *
 * @see MetricsProcessingContext
 * @see MetricsProcessorConfiguration
 */
public interface MetricsProcessor {

  /**
   * Process a metrics report with full context information.
   *
   * <p>Implementations should handle exceptions gracefully and not throw exceptions that would
   * disrupt the metrics reporting flow. Errors should be logged and metrics about processing
   * failures should be emitted.
   *
   * @param context the complete context for metrics processing
   */
  void process(MetricsProcessingContext context);
}
