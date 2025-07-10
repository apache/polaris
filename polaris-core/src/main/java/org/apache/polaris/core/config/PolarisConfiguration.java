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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

  private static final List<PolarisConfiguration<?>> allConfigurations = new ArrayList<>();

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
    for (PolarisConfiguration<?> existingConfiguration : allConfigurations) {
      if (existingConfiguration.key.equals(configuration.key)) {
        throw new IllegalArgumentException(
            String.format("Config '%s' is already in use", configuration.key));
      } else {
        var configs =
            Stream.of(
                    configuration.catalogConfigImpl,
                    configuration.catalogConfigUnsafeImpl,
                    existingConfiguration.catalogConfigImpl,
                    existingConfiguration.catalogConfigUnsafeImpl)
                .flatMap(Optional::stream)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        for (var entry : configs.entrySet()) {
          if (entry.getValue() > 1) {
            throw new IllegalArgumentException(
                String.format("Catalog config %s is already in use", entry.getKey()));
          }
        }
      }
    }
    allConfigurations.add(configuration);
  }

  /** Returns a list of all PolarisConfigurations that have been registered */
  public static List<PolarisConfiguration<?>> getAllConfigurations() {
    return List.copyOf(allConfigurations);
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
     * Used to support backwards compatability before there were reserved properties. Usage of this
     * method should be removed over time.
     *
     * @deprecated Use {@link #catalogConfig()} instead.
     */
    @Deprecated
    public Builder<T> catalogConfigUnsafe(String catalogConfig) {
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
      if (catalogConfig.isPresent() || catalogConfigUnsafe.isPresent()) {
        throw new IllegalArgumentException(
            "catalog configs are not valid for behavior change configs");
      }
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
