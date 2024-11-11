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
package org.apache.polaris.core;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.entity.CatalogEntity;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Dynamic configuration store used to retrieve runtime parameters, which may vary by realm or by
 * request.
 */
public interface PolarisConfigurationStore {
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
  default <T> @NotNull T getConfiguration(
      PolarisCallContext ctx, String configName, @NotNull T defaultValue) {
    Preconditions.checkNotNull(defaultValue, "Cannot pass null as a default value");
    T configValue = getConfiguration(ctx, configName);
    return configValue != null ? configValue : defaultValue;
  }

  /**
   * In some cases, we may extract a value that doesn't match the expected type for a config. This
   * method can be used to attempt to force-cast it using `String.valueOf`
   */
  private <T> @NotNull T tryCast(PolarisConfiguration<T> config, Object value) {
    if (value == null) {
      return config.defaultValue;
    }

    if (config.defaultValue instanceof Boolean) {
      return config.cast(Boolean.valueOf(String.valueOf(value)));
    } else if (config.defaultValue instanceof Integer) {
      return config.cast(Integer.valueOf(String.valueOf(value)));
    } else if (config.defaultValue instanceof Long) {
      return config.cast(Long.valueOf(String.valueOf(value)));
    } else if (config.defaultValue instanceof Double) {
      return config.cast(Double.valueOf(String.valueOf(value)));
    } else if (config.defaultValue instanceof List<?>) {
      return config.cast(new ArrayList<>((List<?>) value));
    } else {
      return config.cast(value);
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
  default <T> @NotNull T getConfiguration(PolarisCallContext ctx, PolarisConfiguration<T> config) {
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
  default <T> @NotNull T getConfiguration(
      PolarisCallContext ctx,
      @NotNull CatalogEntity catalogEntity,
      PolarisConfiguration<T> config) {
    if (config.hasCatalogConfig()
        && catalogEntity.getPropertiesAsMap().containsKey(config.catalogConfig())) {
      return tryCast(config, catalogEntity.getPropertiesAsMap().get(config.catalogConfig()));
    } else {
      return getConfiguration(ctx, config);
    }
  }
}
