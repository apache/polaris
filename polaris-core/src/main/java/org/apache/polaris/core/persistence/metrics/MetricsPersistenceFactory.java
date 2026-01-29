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

import org.apache.polaris.core.context.RealmContext;

/**
 * Factory interface for creating {@link MetricsPersistence} instances.
 *
 * <p>Implementations may cache instances per realm for efficiency. For backends that don't support
 * metrics persistence, implementations should return {@link MetricsPersistence#NOOP}.
 */
public interface MetricsPersistenceFactory {

  /**
   * Gets or creates a {@link MetricsPersistence} instance for the given realm.
   *
   * <p>Implementations may cache instances per realm. If the persistence backend does not support
   * metrics persistence, this method should return {@link MetricsPersistence#NOOP}.
   *
   * @param realmContext the realm context
   * @return a MetricsPersistence instance for the realm, or NOOP if not supported
   */
  MetricsPersistence getOrCreateMetricsPersistence(RealmContext realmContext);
}
