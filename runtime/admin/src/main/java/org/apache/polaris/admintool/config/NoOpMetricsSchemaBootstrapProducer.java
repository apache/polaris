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
package org.apache.polaris.admintool.config;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.polaris.core.persistence.metrics.MetricsSchemaBootstrap;
import org.apache.polaris.core.persistence.metrics.NoOpMetricsSchemaBootstrap;

/**
 * CDI producer for the no-op {@link MetricsSchemaBootstrap} implementation.
 *
 * <p>This producer provides a no-op implementation for backends that don't support metrics schema
 * bootstrapping or when metrics persistence is disabled.
 */
@ApplicationScoped
public class NoOpMetricsSchemaBootstrapProducer {

  /**
   * Produces a no-op {@link MetricsSchemaBootstrap} instance.
   *
   * @return a no-op MetricsSchemaBootstrap implementation
   */
  @Produces
  @ApplicationScoped
  @Identifier("noop")
  public MetricsSchemaBootstrap noopMetricsSchemaBootstrap() {
    return NoOpMetricsSchemaBootstrap.INSTANCE;
  }
}

