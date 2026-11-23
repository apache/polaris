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

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.config.RealmConfigurationSource;
import org.apache.polaris.core.context.RealmContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("removal")
@ApplicationScoped
public class DefaultConfigurationStore
    implements org.apache.polaris.core.config.PolarisConfigurationStore, RealmConfigurationSource {
  Logger LOGGER = LoggerFactory.getLogger(DefaultConfigurationStore.class);

  private final Map<String, Object> defaults;
  private final Map<String, Map<String, Object>> realmOverrides;

  // FIXME the whole PolarisConfigurationStore + PolarisConfiguration needs to be refactored
  // to become a proper Quarkus configuration object
  @Inject
  public DefaultConfigurationStore(
      ObjectMapper objectMapper,
      FeaturesConfiguration configurations,
      BehaviorChangesConfiguration behaviorChanges) {
    // Behavior changes are set under polaris.behavior-changes and features under
    // polaris.features; their keys do not overlap. A key set in both places takes the feature
    // value.
    Map<String, Object> defaults = new HashMap<>(behaviorChanges.parseDefaults(objectMapper));
    defaults.putAll(configurations.parseDefaults(objectMapper));
    this.defaults = Map.copyOf(defaults);
    Map<String, Map<String, Object>> realmOverrides =
        new HashMap<>(behaviorChanges.parseRealmOverrides(objectMapper));
    configurations
        .parseRealmOverrides(objectMapper)
        .forEach((realm, overrides) ->
            realmOverrides.merge(
                realm,
                overrides,
                (behaviorChangeOverrides, featureOverrides) -> {
                  Map<String, Object> merged = new HashMap<>(behaviorChangeOverrides);
                  merged.putAll(featureOverrides);
                  return merged;
                }));
    this.realmOverrides = Map.copyOf(realmOverrides);
  }

  @Override
  public @Nullable Object getConfigValue(@NonNull RealmContext realmContext, String configName) {
    String realm = realmContext.getRealmIdentifier();
    LOGGER.debug("Get configuration value for {} with realm {}", configName, realm);
    return Optional.ofNullable(realmOverrides.getOrDefault(realm, Map.of()).get(configName))
        .orElseGet(() -> getDefaultConfiguration(configName));
  }

  @Override
  public <T> @Nullable T getConfiguration(@NonNull RealmContext realmContext, String configName) {
    @SuppressWarnings("unchecked")
    T value = (T) getConfigValue(realmContext, configName);
    return value;
  }

  private <T> @Nullable T getDefaultConfiguration(String configName) {
    @SuppressWarnings("unchecked")
    T confgValue = (T) defaults.get(configName);
    return confgValue;
  }
}
