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

package org.apache.polaris.core.config;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import org.apache.polaris.core.context.RealmContext;

/**
 * This is an SPI interface used by Polaris Core for loading base configuration values from the
 * environment.
 *
 * <p>Core classes are expected to use {@link RealmConfig} for configuration lookup in runtime.
 */
public interface RealmConfigurationSource {
  RealmConfigurationSource EMPTY_CONFIG = (rc, name) -> null;

  static RealmConfigurationSource global(Map<String, ?> config) {
    return (rc, name) -> config.get(name);
  }

  /**
   * Retrieve the current value for a configuration key for a given realm. May be null if not set.
   *
   * @param realmContext realm context for the configuration lookup request.
   * @param configName the name of the configuration key to look up.
   * @return the current value set for the configuration key for the given realm, or null if not
   *     set.
   */
  @Nullable
  Object getConfigValue(@Nonnull RealmContext realmContext, String configName);
}
