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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.RealmContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class DefaultConfigurationStore implements PolarisConfigurationStore {
  Logger LOGGER = LoggerFactory.getLogger(DefaultConfigurationStore.class);

  private final Map<String, Object> defaults;
  private final Map<String, Map<String, Object>> realmOverrides;
  @Inject private Instance<RealmContext> realmContextInstance;

  // FIXME the whole PolarisConfigurationStore + PolarisConfiguration needs to be refactored
  // to become a proper Quarkus configuration object
  @Inject
  public DefaultConfigurationStore(
      ObjectMapper objectMapper, FeaturesConfiguration configurations) {
    this.defaults = Map.copyOf(configurations.parseDefaults(objectMapper));
    this.realmOverrides = Map.copyOf(configurations.parseRealmOverrides(objectMapper));
  }

  @Override
  public <T> @Nullable T getConfiguration(@Nonnull RealmContext realmContext, String configName) {
    String realm = realmContext.getRealmIdentifier();
    LOGGER.debug("Get configuration value for {} with realm {}", configName, realm);
    @SuppressWarnings("unchecked")
    T confgValue =
        (T)
            Optional.ofNullable(realmOverrides.getOrDefault(realm, Map.of()).get(configName))
                .orElseGet(() -> getDefaultConfiguration(configName));
    return confgValue;
  }

  @Override
  public <T> @Nullable T getConfiguration(@Nonnull PolarisCallContext ctx, String configName) {
    if (realmContextInstance.isResolvable()) {
      RealmContext realmContext = realmContextInstance.get();
      return getConfiguration(realmContext, configName);
    } else {
      LOGGER.debug(
          "No RealmContext is injected when lookup value for configuration {} ", configName);
      return getDefaultConfiguration(configName);
    }
  }

  private <T> @Nullable T getDefaultConfiguration(String configName) {
    @SuppressWarnings("unchecked")
    T confgValue = (T) defaults.get(configName);
    return confgValue;
  }
}
