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
package org.apache.polaris.service.config;

import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class DefaultConfigurationStore implements PolarisConfigurationStore {
  private final Map<String, Object> config;
  private final Map<String, Map<String, Object>> realmConfig;
  private final DynamicFeatureConfigResolver dynamicFeatureConfigResolver;

  public DefaultConfigurationStore(Map<String, Object> config) {
    this.config = config;
    this.realmConfig = Map.of();
    this.dynamicFeatureConfigResolver = new NoOpDynamicFeatureConfigResolver();
  }

  public DefaultConfigurationStore(
      Map<String, Object> config,
      Map<String, Map<String, Object>> realmConfig,
      DynamicFeatureConfigResolver dynamicFeatureConfigResolver) {
    this.config = config;
    this.realmConfig = realmConfig;
    this.dynamicFeatureConfigResolver = dynamicFeatureConfigResolver;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> @Nullable T getConfiguration(@NotNull PolarisCallContext ctx, String configName) {
    String realm = CallContext.getCurrentContext().getRealmContext().getRealmIdentifier();
    return (T)
        dynamicFeatureConfigResolver
            .resolve(configName)
            .orElse(
                realmConfig
                    .getOrDefault(realm, Map.of())
                    .getOrDefault(configName, config.get(configName)));
  }
}
