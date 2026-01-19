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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MetricsProcessor} that delegates to the legacy {@link PolarisMetricsReporter} for
 * backward compatibility.
 *
 * <p>This processor is used when the old configuration path {@code
 * polaris.iceberg-metrics.reporting.type} is specified. It wraps the configured {@link
 * PolarisMetricsReporter} and adapts it to the new {@link MetricsProcessor} interface.
 *
 * <p>This allows existing configurations to continue working without changes during the migration
 * to the new metrics processing system.
 *
 * <p>To use this processor with the new configuration:
 *
 * <pre>
 * polaris:
 *   metrics:
 *     processor:
 *       type: legacy
 * </pre>
 *
 * <p>Or continue using the old configuration (automatically mapped to this processor):
 *
 * <pre>
 * polaris:
 *   iceberg-metrics:
 *     reporting:
 *       type: default
 * </pre>
 */
@ApplicationScoped
@Identifier("legacy")
public class LegacyMetricsProcessor implements MetricsProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(LegacyMetricsProcessor.class);

  private final PolarisMetricsReporter reporter;

  @Inject
  public LegacyMetricsProcessor(PolarisMetricsReporter reporter) {
    this.reporter = reporter;
    LOGGER.info(
        "LegacyMetricsProcessor initialized with reporter: {}", reporter.getClass().getName());
  }

  @Override
  public void process(MetricsProcessingContext context) {
    // Delegate to the legacy reporter with basic parameters
    reporter.reportMetric(
        context.catalogName(), context.tableIdentifier(), context.metricsReport());
  }
}

