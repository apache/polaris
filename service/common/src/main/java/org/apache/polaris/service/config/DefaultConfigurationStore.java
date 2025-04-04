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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;

@ApplicationScoped
public class DefaultConfigurationStore implements PolarisConfigurationStore {

  private final Map<String, Object> defaults;
  private final Map<String, Map<String, Object>> realmOverrides;

  // FIXME the whole PolarisConfigurationStore + PolarisConfiguration needs to be refactored
  // to become a proper Quarkus configuration object
  @Inject
  public DefaultConfigurationStore(
      ObjectMapper objectMapper, FeaturesConfiguration configurations) {
    this(
        configurations.parseDefaults(objectMapper),
        configurations.parseRealmOverrides(objectMapper));
  }

  public DefaultConfigurationStore(Map<String, Object> defaults) {
    this(defaults, Map.of());
  }

  public DefaultConfigurationStore(
      Map<String, Object> defaults, Map<String, Map<String, Object>> realmOverrides) {
    this.defaults = Map.copyOf(defaults);
    this.realmOverrides = Map.copyOf(realmOverrides);
  }

  @Override
  public <T> @Nonnull Optional<T> getConfiguration(
      @Nonnull PolarisCallContext ctx, String configName) {
    String realm = CallContext.getCurrentContext().getRealmContext().getRealmIdentifier();
    @SuppressWarnings("unchecked")
    T configValue =
        (T)
            realmOverrides
                .getOrDefault(realm, Map.of())
                .getOrDefault(configName, defaults.get(configName));
    return Optional.ofNullable(configValue);
  }
}
