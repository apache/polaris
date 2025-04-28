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

import org.apache.polaris.core.context.CallContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An ABC for Polaris configurations that alter the service's behavior
 *
 * @param <T> The type of the configuration
 */
public abstract class PolarisConfiguration<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisConfiguration.class);

  public final String key;
  public final String description;
  public final T defaultValue;
  private final Optional<String> catalogConfigImpl;
  private final Class<T> typ;
  private final Optional<Function<T, Boolean>> validation;

  @SuppressWarnings("unchecked")
  protected PolarisConfiguration(
      String key,
      String description,
      T defaultValue,
      Optional<String> catalogConfig,
      Optional<Function<T, Boolean>> validation) {
    this.key = key;
    this.description = description;
    this.defaultValue = defaultValue;
    this.catalogConfigImpl = catalogConfig;
    this.typ = (Class<T>) defaultValue.getClass();
    this.validation = validation;

    // Force validation:
    apply(defaultValue);
  }

  public boolean hasCatalogConfig() {
    return catalogConfigImpl.isPresent();
  }

  public String catalogConfig() {
    return catalogConfigImpl.orElseThrow(
        () ->
            new IllegalStateException(
                "Attempted to read a catalog config key from a configuration that doesn't have one."));
  }

  T apply(Object value) {
    T result = this.typ.cast(value);
    validate(result);
    return result;
  }

  private void validate(T value) {
    this.validation.ifPresent(
        v -> {
          if (!v.apply(value)) {
            throw new IllegalArgumentException(
                String.format("Configuration %s has invalid value %s", key, defaultValue));
          }
        });
  }

  public static class Builder<T> {
    private String key;
    private String description;
    private T defaultValue;
    private Optional<String> catalogConfig = Optional.empty();
    private Optional<Function<T, Boolean>> validation = Optional.empty();

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
      this.catalogConfig = Optional.of(catalogConfig);
      return this;
    }

    public Builder<T> validation(Function<T, Boolean> validation) {
      this.validation = Optional.of(validation);
      return this;
    }

    public FeatureConfiguration<T> buildFeatureConfiguration() {
      if (key == null || description == null || defaultValue == null) {
        throw new IllegalArgumentException("key, description, and defaultValue are required");
      }
      return new FeatureConfiguration<>(key, description, defaultValue, catalogConfig, validation);
    }

    public BehaviorChangeConfiguration<T> buildBehaviorChangeConfiguration() {
      if (key == null || description == null || defaultValue == null) {
        throw new IllegalArgumentException("key, description, and defaultValue are required");
      }
      if (catalogConfig.isPresent()) {
        throw new IllegalArgumentException(
            "catalogConfig is not valid for behavior change configs");
      }
      return new BehaviorChangeConfiguration<>(key, description, defaultValue, catalogConfig, validation);
    }
  }

  /**
   * Returns the value of a `PolarisConfiguration`, or the default if it cannot be loaded. This
   * method does not need to be used when a `CallContext` is already available
   */
  public static <T> T loadConfig(PolarisConfiguration<T> configuration) {
    var callContext = CallContext.getCurrentContext();
    if (callContext == null) {
      LOGGER.warn(
          String.format(
              "Unable to load current call context; using %s = %s",
              configuration.key, configuration.defaultValue));
      return configuration.defaultValue;
    }
    return callContext
        .getPolarisCallContext()
        .getConfigurationStore()
        .getConfiguration(callContext.getPolarisCallContext(), configuration);
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }
}
