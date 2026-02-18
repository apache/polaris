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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An ABC for Polaris configurations that alter the service's behavior TODO: deprecate unsafe
 * catalog configs and remove related code
 *
 * @param <T> The type of the configuration
 */
public abstract class PolarisConfiguration<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisConfiguration.class);

  private static final Map<String, PolarisConfiguration<?>> ALL_CONFIGURATIONS =
      new ConcurrentHashMap<>();
  private static final Set<String> ALL_CATALOG_CONFIGS = new ConcurrentSkipListSet<>();

  private final String key;
  private final String description;
  private final T defaultValue;
  private final Optional<String> catalogConfigImpl;
  private final Optional<String> catalogConfigUnsafeImpl;
  private final Class<T> typ;

  /** catalog configs are expected to start with this prefix */
  private static final String SAFE_CATALOG_CONFIG_PREFIX = "polaris.config.";

  /**
   * Helper method for building `allConfigurations` and checking for duplicate use of keys across
   * configs.
   */
  private static void registerConfiguration(PolarisConfiguration<?> configuration) {
    if (ALL_CONFIGURATIONS.putIfAbsent(configuration.key(), configuration) != null) {
      throw new IllegalArgumentException(
          String.format("Config '%s' is already in use", configuration.key));
    }
    if (configuration.hasCatalogConfig()) {
      if (!ALL_CATALOG_CONFIGS.add(configuration.catalogConfig())) {
        throw new IllegalArgumentException(
            String.format("Catalog config '%s' is already in use", configuration.catalogConfig()));
      }
    }
    if (configuration.hasCatalogConfigUnsafe()) {
      if (!ALL_CATALOG_CONFIGS.add(configuration.catalogConfigUnsafe())) {
        throw new IllegalArgumentException(
            String.format(
                "Catalog config '%s' is already in use", configuration.catalogConfigUnsafe()));
      }
    }
  }

  /** Returns a list of all PolarisConfigurations that have been registered. */
  public static List<PolarisConfiguration<?>> getAllConfigurations() {
    return List.copyOf(ALL_CONFIGURATIONS.values());
  }

  /** Returns a set of all catalog config keys that have been registered. */
  public static Set<String> getAllCatalogConfigs() {
    return Set.copyOf(ALL_CATALOG_CONFIGS);
  }

  @SuppressWarnings("unchecked")
  protected PolarisConfiguration(
      String key,
      String description,
      T defaultValue,
      Optional<String> catalogConfig,
      Optional<String> catalogConfigUnsafe) {
    this.key = key;
    this.description = description;
    this.defaultValue = defaultValue;
    this.catalogConfigImpl = catalogConfig;
    this.catalogConfigUnsafeImpl = catalogConfigUnsafe;
    this.typ = (Class<T>) defaultValue.getClass();
  }

  public final boolean hasCatalogConfig() {
    return catalogConfigImpl.isPresent();
  }

  public final String catalogConfig() {
    return catalogConfigImpl.orElseThrow(
        () ->
            new IllegalStateException(
                "Attempted to read a catalog config key from a configuration that doesn't have one."));
  }

  public final boolean hasCatalogConfigUnsafe() {
    return catalogConfigUnsafeImpl.isPresent();
  }

  public final String catalogConfigUnsafe() {
    return catalogConfigUnsafeImpl.orElseThrow(
        () ->
            new IllegalStateException(
                "Attempted to read an unsafe catalog config key from a configuration that doesn't have one."));
  }

  T cast(Object value) {
    return this.typ.cast(value);
  }

  public final String key() {
    return key;
  }

  public final String description() {
    return description;
  }

  public final T defaultValue() {
    return defaultValue;
  }

  public T resolveValue(
      Function<String, ?> globalProperties, Function<String, ?> catalogProperties) {
    Object propertyValue = null;
    if (hasCatalogConfig() || hasCatalogConfigUnsafe()) {
      if (hasCatalogConfig()) {
        propertyValue = catalogProperties.apply(catalogConfig());
      }
      if (propertyValue == null) {
        if (hasCatalogConfigUnsafe()) {
          propertyValue = catalogProperties.apply(catalogConfigUnsafe());
        }
        if (propertyValue != null) {
          LOGGER.warn(
              String.format(
                  "Deprecated config %s is in use and will be removed in a future version",
                  catalogConfigUnsafe()));
        }
      }
    }

    if (propertyValue == null) {
      propertyValue = globalProperties.apply(key());
    }

    return tryCast(propertyValue);
  }

  /**
   * In some cases, we may extract a value that doesn't match the expected type for a config. This
   * method can be used to attempt to force-cast it using `String.valueOf`
   */
  private @Nonnull T tryCast(Object value) {
    if (value == null) {
      return defaultValue();
    }

    if (defaultValue() instanceof Boolean) {
      return cast(Boolean.valueOf(String.valueOf(value)));
    } else if (defaultValue() instanceof Integer) {
      return cast(Integer.valueOf(String.valueOf(value)));
    } else if (defaultValue() instanceof Long) {
      return cast(Long.valueOf(String.valueOf(value)));
    } else if (defaultValue() instanceof Double) {
      return cast(Double.valueOf(String.valueOf(value)));
    } else if (defaultValue() instanceof Float) {
      return cast(Float.valueOf(String.valueOf(value)));
    } else if (defaultValue() instanceof List<?>) {
      return cast(new ArrayList<>((List<?>) value));
    } else {
      return cast(value);
    }
  }

  public static class Builder<T> {
    private String key;
    private String description;
    private T defaultValue;
    private Optional<String> catalogConfig = Optional.empty();
    private Optional<String> catalogConfigUnsafe = Optional.empty();

    public Builder<T> key(String key) {
      this.key = key;
      return this;
    }

    public Builder<T> description(String description) {
      this.description = description;
      return this;
    }

    @SuppressWarnings("unchecked")
    public Builder<T> defaultValue(T defaultValue) {
      if (defaultValue instanceof List<?>) {
        // Type-safe handling of List
        this.defaultValue = (T) new ArrayList<>((List<?>) defaultValue);
      } else {
        this.defaultValue = defaultValue;
      }
      return this;
    }

    public Builder<T> catalogConfig(String catalogConfig) {
      if (!catalogConfig.startsWith(SAFE_CATALOG_CONFIG_PREFIX)) {
        throw new IllegalArgumentException(
            "Catalog configs are expected to start with " + SAFE_CATALOG_CONFIG_PREFIX);
      }
      this.catalogConfig = Optional.of(catalogConfig);
      return this;
    }

    /**
     * @deprecated Use {@link #legacyCatalogConfig(String)} instead.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated(since = "1.0.0-incubating", forRemoval = true)
    public Builder<T> catalogConfigUnsafe(String catalogConfig) {
      return legacyCatalogConfig(catalogConfig);
    }

    /** Used to support backwards compatibility before there were reserved properties. */
    Builder<T> legacyCatalogConfig(String catalogConfig) {
      if (catalogConfig.startsWith(SAFE_CATALOG_CONFIG_PREFIX)) {
        throw new IllegalArgumentException(
            "Unsafe catalog configs are not expected to start with " + SAFE_CATALOG_CONFIG_PREFIX);
      }
      this.catalogConfigUnsafe = Optional.of(catalogConfig);
      return this;
    }

    private void validateOrThrow() {
      if (key == null || description == null || defaultValue == null) {
        throw new IllegalArgumentException("key, description, and defaultValue are required");
      }
      if (key.contains(".")) {
        throw new IllegalArgumentException("key cannot contain `.`");
      }
    }

    public FeatureConfiguration<T> buildFeatureConfiguration() {
      validateOrThrow();
      FeatureConfiguration<T> config =
          new FeatureConfiguration<>(
              key, description, defaultValue, catalogConfig, catalogConfigUnsafe);
      PolarisConfiguration.registerConfiguration(config);
      return config;
    }

    public BehaviorChangeConfiguration<T> buildBehaviorChangeConfiguration() {
      validateOrThrow();
      BehaviorChangeConfiguration<T> config =
          new BehaviorChangeConfiguration<>(
              key, description, defaultValue, catalogConfig, catalogConfigUnsafe);
      PolarisConfiguration.registerConfiguration(config);
      return config;
    }
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }
}
