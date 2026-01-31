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
import java.util.Map;
import org.apache.polaris.core.context.RealmContext;
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
   * Retrieve the current value for a configuration key for a given realm. May be null if not set.
   *
   * @param realmContext the realm context
   * @param configName the name of the configuration key to check
   * @return the current value set for the configuration key for the given realm, or null if not set
   * @param <T> the type of the configuration value
   */
  default <T> @Nullable T getConfiguration(@Nonnull RealmContext realmContext, String configName) {
    return null;
  }

  /**
   * Retrieve the current value for a configuration key for the given realm. If not set, return the
   * non-null default value.
   *
   * @param realmContext the realm context
   * @param configName the name of the configuration key to check
   * @param defaultValue the default value if the configuration key has no value
   * @return the current value or the supplied default value
   * @param <T> the type of the configuration value
   * @deprecated Use {@link RealmConfig}.
   */
  @SuppressWarnings({"DeprecatedIsStillUsed", "removal"})
  @Deprecated(forRemoval = true)
  default <T> @Nonnull T getConfiguration(
      @Nonnull RealmContext realmContext, String configName, @Nonnull T defaultValue) {
    return asRealmConfig(realmContext).getConfig(configName, defaultValue);
  }

  /**
   * Retrieve the current value for a configuration.
   *
   * @param realmContext the current realm context
   * @param config the configuration to load
   * @return the current value set for the configuration key or null if not set
   * @param <T> the type of the configuration value
   */
  default <T> @Nonnull T getConfiguration(
      @Nonnull RealmContext realmContext, PolarisConfiguration<T> config) {
    return asRealmConfig(realmContext).getConfig(config);
  }

  /**
   * Retrieve the current value for a configuration, overriding with a catalog config if it is
   * present.
   *
   * <p>Prefer using {@link #getConfiguration(RealmContext, Map, PolarisConfiguration)} when the
   * catalog properties are already available or are needed repeatedly to prevent unnecessary JSON
   * deserialization.
   *
   * @param realmContext the current realm context
   * @param catalogEntity the catalog to check for an override
   * @param config the configuration to load
   * @return the current value set for the configuration key or null if not set
   * @param <T> the type of the configuration value
   */
  default <T> @Nonnull T getConfiguration(
      @Nonnull RealmContext realmContext,
      @Nonnull CatalogEntity catalogEntity,
      PolarisConfiguration<T> config) {
    return getConfiguration(realmContext, catalogEntity.getPropertiesAsMap(), config);
  }

  /**
   * Retrieve the current value for a configuration, overriding with a catalog config if it is
   * present.
   *
   * @param realmContext the current realm context
   * @param catalogProperties the catalog configuration to check for an override
   * @param config the configuration to load
   * @return the current value set for the configuration key or null if not set
   * @param <T> the type of the configuration value
   */
  default <T> @Nonnull T getConfiguration(
      @Nonnull RealmContext realmContext,
      @Nonnull Map<String, String> catalogProperties,
      PolarisConfiguration<T> config) {
    return asRealmConfig(realmContext).getConfig(config, catalogProperties);
  }

  private RealmConfig asRealmConfig(RealmContext realmContext) {
    return new RealmConfigImpl(this::getConfiguration, realmContext);
  }
}
