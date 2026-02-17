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

import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;

public class RealmConfigImpl implements RealmConfig {

  private final RealmConfigurationSource configurationSource;
  private final RealmContext realmContext;

  public RealmConfigImpl(RealmConfigurationSource configurationSource, RealmContext realmContext) {
    this.configurationSource = configurationSource;
    this.realmContext = realmContext;
  }

  @SuppressWarnings("removal")
  @Override
  public <T> @Nullable T getConfig(String configName) {
    @SuppressWarnings("unchecked")
    T value = (T) configurationSource.getConfigValue(realmContext, configName);
    return value;
  }

  @SuppressWarnings("removal")
  @Override
  public <T> T getConfig(String configName, T defaultValue) {
    @SuppressWarnings("unchecked")
    T value = (T) getConfig(configName);
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  @Override
  public <T> T getConfig(PolarisConfiguration<T> config) {
    return getConfig(config, Collections.emptyMap());
  }

  @Override
  public <T> T getConfig(PolarisConfiguration<T> config, CatalogEntity catalogEntity) {
    return getConfig(config, catalogEntity.getPropertiesAsMap());
  }

  private Object realmConfigValue(String configName) {
    return configurationSource.getConfigValue(realmContext, configName);
  }

  @Override
  public <T> T getConfig(PolarisConfiguration<T> config, Map<String, String> catalogProperties) {
    return config.resolveValue(this::realmConfigValue, catalogProperties::get);
  }
}
