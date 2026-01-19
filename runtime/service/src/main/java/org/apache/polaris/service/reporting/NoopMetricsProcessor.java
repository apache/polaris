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
 * A no-op implementation of {@link MetricsProcessor} that discards all metrics.
 *
 * <p>This is the default processor when no specific type is configured. It performs no processing
 * and simply discards all metrics reports.
 *
 * <p>This processor is useful when:
 *
 * <ul>
 *   <li>Metrics processing is not needed
 *   <li>You want to disable metrics processing temporarily
 *   <li>You're testing and don't want metrics overhead
 * </ul>
 *
 * <p>Configuration:
 *
 * <pre>
 * polaris:
 *   metrics:
 *     processor:
 *       type: noop
 * </pre>
 */
@ApplicationScoped
@Identifier("noop")
public class NoopMetricsProcessor implements MetricsProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(NoopMetricsProcessor.class);

  public NoopMetricsProcessor() {
    LOGGER.debug("NoopMetricsProcessor initialized - all metrics will be discarded");
  }

  @Override
  public void process(MetricsProcessingContext context) {
    // Intentionally do nothing - discard all metrics
    LOGGER.trace(
        "Discarding metrics for {}.{}", context.catalogName(), context.tableIdentifier());
  }
}

