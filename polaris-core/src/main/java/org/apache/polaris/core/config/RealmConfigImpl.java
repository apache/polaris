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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RealmConfigImpl implements RealmConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealmConfigImpl.class);

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

  @Override
  public <T> T getConfig(PolarisConfiguration<T> config, Map<String, String> catalogProperties) {
    Object propertyValue = null;
    if (config.hasCatalogConfig() || config.hasCatalogConfigUnsafe()) {
      if (config.hasCatalogConfig()) {
        propertyValue = catalogProperties.get(config.catalogConfig());
      }
      if (propertyValue == null) {
        if (config.hasCatalogConfigUnsafe()) {
          propertyValue = catalogProperties.get(config.catalogConfigUnsafe());
        }
        if (propertyValue != null) {
          LOGGER.warn(
              String.format(
                  "Deprecated config %s is in use and will be removed in a future version",
                  config.catalogConfigUnsafe()));
        }
      }
    }

    if (propertyValue == null) {
      propertyValue = configurationSource.getConfigValue(realmContext, config.key());
    }

    return tryCast(config, propertyValue);
  }

  /**
   * In some cases, we may extract a value that doesn't match the expected type for a config. This
   * method can be used to attempt to force-cast it using `String.valueOf`
   */
  private <T> @Nonnull T tryCast(PolarisConfiguration<T> config, Object value) {
    if (value == null) {
      return config.defaultValue();
    }

    if (config.defaultValue() instanceof Boolean) {
      return config.cast(Boolean.valueOf(String.valueOf(value)));
    } else if (config.defaultValue() instanceof Integer) {
      return config.cast(Integer.valueOf(String.valueOf(value)));
    } else if (config.defaultValue() instanceof Long) {
      return config.cast(Long.valueOf(String.valueOf(value)));
    } else if (config.defaultValue() instanceof Double) {
      return config.cast(Double.valueOf(String.valueOf(value)));
    } else if (config.defaultValue() instanceof List<?>) {
      return config.cast(new ArrayList<>((List<?>) value));
    } else {
      return config.cast(value);
    }
  }
}
