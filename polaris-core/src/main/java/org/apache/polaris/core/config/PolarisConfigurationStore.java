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

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dynamic configuration store used to retrieve runtime parameters, which may vary by realm or by
 * request.
 */
public interface PolarisConfigurationStore {
  Logger LOGGER = LoggerFactory.getLogger(PolarisConfigurationStore.class);

  /**
   * Retrieve the current value for a configuration key. May be null if not set.
   *
   * @param ctx the current call context
   * @param configName the name of the configuration key to check
   * @return the current value set for the configuration key or null if not set
   * @param <T> the type of the configuration value
   */
  default <T> @Nullable T getConfiguration(PolarisCallContext ctx, String configName) {
    return null;
  }

  /**
   * Retrieve the current value for a configuration key. If not set, return the non-null default
   * value.
   *
   * @param ctx the current call context
   * @param configName the name of the configuration key to check
   * @param defaultValue the default value if the configuration key has no value
   * @return the current value or the supplied default value
   * @param <T> the type of the configuration value
   */
  default <T> @Nonnull T getConfiguration(
      PolarisCallContext ctx, String configName, @Nonnull T defaultValue) {
    Preconditions.checkNotNull(defaultValue, "Cannot pass null as a default value");
    T configValue = getConfiguration(ctx, configName);
    return configValue != null ? configValue : defaultValue;
  }

  /**
   * In some cases, we may extract a value that doesn't match the expected type for a config. This
   * method can be used to attempt to force-cast it using `String.valueOf`
   */
  private <T> @Nonnull T tryCast(PolarisConfiguration<T> config, Object value) {
    if (value == null) {
      return config.defaultValue;
    }

    if (config.defaultValue instanceof Boolean) {
      return config.apply(Boolean.valueOf(String.valueOf(value)));
    } else if (config.defaultValue instanceof Integer) {
      return config.apply(Integer.valueOf(String.valueOf(value)));
    } else if (config.defaultValue instanceof Long) {
      return config.apply(Long.valueOf(String.valueOf(value)));
    } else if (config.defaultValue instanceof Double) {
      return config.apply(Double.valueOf(String.valueOf(value)));
    } else if (config.defaultValue instanceof List<?>) {
      return config.apply(new ArrayList<>((List<?>) value));
    } else {
      return config.apply(value);
    }
  }

  /**
   * Retrieve the current value for a configuration.
   *
   * @param ctx the current call context
   * @param config the configuration to load
   * @return the current value set for the configuration key or null if not set
   * @param <T> the type of the configuration value
   */
  default <T> @Nonnull T getConfiguration(PolarisCallContext ctx, PolarisConfiguration<T> config) {
    T result = getConfiguration(ctx, config.key, config.defaultValue);
    return tryCast(config, result);
  }

  /**
   * Retrieve the current value for a configuration, overriding with a catalog config if it is
   * present.
   *
   * @param ctx the current call context
   * @param catalogEntity the catalog to check for an override
   * @param config the configuration to load
   * @return the current value set for the configuration key or null if not set
   * @param <T> the type of the configuration value
   */
  default <T> @Nonnull T getConfiguration(
      PolarisCallContext ctx,
      @Nonnull CatalogEntity catalogEntity,
      PolarisConfiguration<T> config) {
    if (config.hasCatalogConfig() || config.hasCatalogConfigUnsafe()) {
      Map<String, String> propertiesMap = catalogEntity.getPropertiesAsMap();
      String propertyValue = propertiesMap.get(config.catalogConfig());
      if (propertyValue == null) {
        propertyValue = propertiesMap.get(config.catalogConfigUnsafe());
        if (propertyValue != null) {
          LOGGER.warn(
              String.format(
                  "Deprecated config %s is in use and will be removed in a future version",
                  config.catalogConfigUnsafe()));
        }
      }
      if (propertyValue != null) {
        return tryCast(config, propertyValue);
      }
    }
    return getConfiguration(ctx, config);
  }
}
