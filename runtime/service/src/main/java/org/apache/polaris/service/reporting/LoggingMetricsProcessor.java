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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MetricsProcessor} implementation that logs metrics to the console.
 *
 * <p>This processor logs all metrics reports at INFO level, including the full context
 * information such as realm ID, principal, request ID, and OpenTelemetry trace context.
 *
 * <p>This processor is useful for:
 *
 * <ul>
 *   <li>Development and debugging
 *   <li>Understanding what metrics are being reported
 *   <li>Troubleshooting metrics processing issues
 * </ul>
 *
 * <p>Configuration:
 *
 * <pre>
 * polaris:
 *   metrics:
 *     processor:
 *       type: logging
 * </pre>
 *
 * <p>To see the logs, ensure the logging level is set appropriately:
 *
 * <pre>
 * quarkus.log.category."org.apache.polaris.service.reporting".level=INFO
 * </pre>
 */
@ApplicationScoped
@Identifier("logging")
public class LoggingMetricsProcessor implements MetricsProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingMetricsProcessor.class);

  public LoggingMetricsProcessor() {
    LOGGER.info("LoggingMetricsProcessor initialized - metrics will be logged to console");
  }

  @Override
  public void process(MetricsProcessingContext context) {
    LOGGER.info(
        "Metrics Report: catalog={}, table={}, realm={}, principal={}, requestId={}, "
            + "traceId={}, spanId={}, timestamp={}, report={}",
        context.catalogName(),
        context.tableIdentifier(),
        context.realmId(),
        context.principalName().orElse("unknown"),
        context.requestId().orElse("none"),
        context.otelTraceId().orElse("none"),
        context.otelSpanId().orElse("none"),
        context.timestampMs(),
        context.metricsReport());
  }
}

