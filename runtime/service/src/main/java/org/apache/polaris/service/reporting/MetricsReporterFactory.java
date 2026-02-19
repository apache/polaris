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
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

/**
 * Factory for creating {@link PolarisMetricsReporter} instances.
 *
 * <p>This factory is {@link ApplicationScoped} to resolve the configured {@link
 * PolarisMetricsReporter} implementation once at startup. The heavy CDI bean selection is performed
 * once, and subsequent calls to {@link #create()} are lightweight operations that simply retrieve
 * an instance from the already-selected implementation.
 *
 * <p>This pattern avoids resolving the bean selector on every request, improving performance.
 */
@ApplicationScoped
public class MetricsReporterFactory {

  private final Instance<PolarisMetricsReporter> selectedImpl;

  @Inject
  public MetricsReporterFactory(
      MetricsReportingConfiguration config, @Any Instance<PolarisMetricsReporter> reporters) {
    // Resolve the selector once at startup based on configuration
    this.selectedImpl = reporters.select(Identifier.Literal.of(config.type()));
  }

  /**
   * Creates a {@link PolarisMetricsReporter} instance from the configured implementation.
   *
   * <p>This is a lightweight operation since the implementation was already selected at startup.
   *
   * @return a PolarisMetricsReporter instance
   */
  public PolarisMetricsReporter create() {
    return selectedImpl.get();
  }
}
