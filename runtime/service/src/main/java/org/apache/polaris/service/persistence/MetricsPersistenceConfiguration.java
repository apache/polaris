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
package org.apache.polaris.service.persistence;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Configuration for selecting the {@link
 * org.apache.polaris.core.persistence.metrics.MetricsPersistence} implementation.
 *
 * <p>This configuration allows selecting the metrics persistence backend independently from the
 * entity metastore. Available types include:
 *
 * <ul>
 *   <li>{@code noop} (default) - No persistence, metrics are discarded
 *   <li>{@code relational-jdbc} - Persists metrics to the JDBC database (requires metrics schema)
 * </ul>
 */
@ConfigMapping(prefix = "polaris.persistence.metrics")
public interface MetricsPersistenceConfiguration {

  /**
   * The type of the metrics persistence to use. Must be a registered {@link
   * org.apache.polaris.core.persistence.metrics.MetricsPersistence} identifier.
   *
   * <p>Defaults to {@code noop} which discards all metrics.
   */
  @WithDefault("noop")
  String type();
}
