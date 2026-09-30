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
package org.apache.polaris.persistence.nosql.cassandra;

import static com.datastax.oss.driver.api.core.config.TypedDriverOption.AUTH_PROVIDER_CLASS;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.AUTH_PROVIDER_PASSWORD;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.AUTH_PROVIDER_USER_NAME;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.CLOUD_SECURE_CONNECT_BUNDLE;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.CONFIG_RELOAD_INTERVAL;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.REQUEST_TIMEOUT;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.NonNull;

/** Applies Cassandra Java driver options supplied through {@link CassandraConfiguration}. */
final class CassandraDriverOptions {
  private static final Set<DriverOption> POLARIS_MANAGED_OPTIONS =
      Set.of(REQUEST_TIMEOUT.getRawOption());
  private static final Set<DriverOption> DRIVER_AUTH_OPTIONS =
      Set.of(
          AUTH_PROVIDER_CLASS.getRawOption(),
          AUTH_PROVIDER_USER_NAME.getRawOption(),
          AUTH_PROVIDER_PASSWORD.getRawOption());

  private CassandraDriverOptions() {}

  static void apply(
      CassandraConfiguration configuration,
      boolean cdiAuthenticationProviderConfigured,
      OptionsMap options) {
    apply(
        configuration.driverOptions(),
        configuration.tlsConfigurationName().isPresent(),
        hasAuthenticationSettings(configuration) || cdiAuthenticationProviderConfigured,
        options);
  }

  static void apply(Map<String, String> configuredOptions, OptionsMap options) {
    apply(configuredOptions, false, false, options);
  }

  private static void apply(
      Map<String, String> configuredOptions,
      boolean quarkusTlsConfigured,
      boolean polarisAuthConfigured,
      OptionsMap optionsMap) {
    if (configuredOptions.isEmpty()) {
      return;
    }

    var optionsByPath = coreOptionsByPath();

    var canonicalOptions =
        canonicalize(configuredOptions, quarkusTlsConfigured, polarisAuthConfigured, optionsByPath);

    applySmallRyeConfigToOptionsMap(optionsByPath, canonicalOptions, optionsMap);
  }

  /**
   * Helper function that uses SmallRye-Config's options value behavior, especially for lists and
   * maps, for the configured Cassandra Java Driver options to configure those via the C* Java
   * Driver's {@link OptionsMap}.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void applySmallRyeConfigToOptionsMap(
      Map<String, TypedDriverOption<?>> optionsByPath,
      Map<String, String> canonicalOptions,
      OptionsMap optionsMap) {
    var config =
        new SmallRyeConfigBuilder()
            .addDiscoveredConverters()
            .withSources(
                new PropertiesConfigSource(canonicalOptions, "Cassandra Java driver options"))
            .build();

    for (Map.Entry<String, TypedDriverOption<?>> entry : optionsByPath.entrySet()) {
      if (canonicalOptions.containsKey(entry.getKey())) {
        var configuredValue = configuredValue(config, entry.getKey(), entry.getValue());
        optionsMap.put((TypedDriverOption) entry.getValue(), configuredValue);
      }
    }
  }

  private static Object configuredValue(
      SmallRyeConfig config, String path, TypedDriverOption<?> option) {
    Type type = option.getExpectedType().getType();
    if (type instanceof Class<?> valueType) {
      return config.getValue(path, valueType);
    }
    if (type instanceof ParameterizedType parameterizedType) {
      Type rawType = parameterizedType.getRawType();
      Type[] typeArguments = parameterizedType.getActualTypeArguments();
      if (rawType.equals(List.class)
          && typeArguments.length == 1
          && typeArguments[0] instanceof Class<?>) {
        return config.getValues(path, (Class<?>) typeArguments[0]);
      }
      if (rawType.equals(Map.class)
          && typeArguments.length == 2
          && typeArguments[0].equals(String.class)
          && typeArguments[1].equals(String.class)) {
        return config.getValues(path, String.class, String.class);
      }
    }
    throw new IllegalArgumentException(
        "Unsupported type for Cassandra Java driver option '%s': %s".formatted(path, type));
  }

  private static boolean hasAuthenticationSettings(CassandraConfiguration configuration) {
    return configuration.authUsername().isPresent()
        || configuration.authPassword().isPresent()
        || configuration.authAuthorizationId().isPresent()
        || !CassandraConfiguration.DEFAULT_AUTHENTICATION_PROVIDER_NAME.equals(
            configuration.authProviderName());
  }

  private static Map<String, String> canonicalize(
      Map<String, String> configuredOptions,
      boolean quarkusTlsConfigured,
      boolean polarisAuthConfigured,
      Map<String, TypedDriverOption<?>> optionsByPath) {
    var optionsByNormalizedPath = createOptionsByNormalizedPath(optionsByPath);

    var canonicalOptions = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : configuredOptions.entrySet()) {
      var path = entry.getKey();

      var option = optionsByPath.get(path);
      if (option == null) {
        option = optionsByNormalizedPath.get(normalize(path));
      }
      if (option == null) {
        throw new IllegalArgumentException("Unknown Cassandra Java driver option: " + path);
      }

      var previous = canonicalOptions.put(option.getRawOption().getPath(), entry.getValue());
      if (previous != null) {
        throw new IllegalArgumentException(
            "Cassandra Java driver option '%s' is configured more than once."
                .formatted(option.getRawOption().getPath()));
      }

      if (POLARIS_MANAGED_OPTIONS.contains(option.getRawOption())) {
        throw new IllegalArgumentException(
            "Cassandra Java driver option '%s' is configured directly by Polaris.".formatted(path));
      }
      if (option.getRawOption().equals(CONFIG_RELOAD_INTERVAL.getRawOption())) {
        throw new IllegalArgumentException(
            "Cassandra Java driver option '%s' is unsupported with in-memory driver configuration."
                .formatted(path));
      }
      if (option.getRawOption().getPath().startsWith("advanced.metrics.")) {
        throw new IllegalArgumentException(
            "Cassandra Java driver metrics options are configured directly by Polaris.");
      }
      if (quarkusTlsConfigured
          && (option.getRawOption().getPath().startsWith("advanced.ssl-engine-factory.")
              || option.getRawOption().equals(CLOUD_SECURE_CONNECT_BUNDLE.getRawOption()))) {
        throw new IllegalArgumentException(
            "Cassandra Java driver TLS options cannot be combined with a Quarkus TLS configuration.");
      }
      if (polarisAuthConfigured && DRIVER_AUTH_OPTIONS.contains(option.getRawOption())) {
        throw new IllegalArgumentException(
            "Cassandra Java driver authentication options cannot be combined with Polaris auth settings.");
      }
    }
    return canonicalOptions;
  }

  private static @NonNull HashMap<String, TypedDriverOption<?>> createOptionsByNormalizedPath(
      Map<String, TypedDriverOption<?>> optionsByPath) {
    var optionsByNormalizedPath = new HashMap<String, TypedDriverOption<?>>();
    for (TypedDriverOption<?> option : optionsByPath.values()) {
      var normalizedPath = normalize(option.getRawOption().getPath());
      var previous = optionsByNormalizedPath.putIfAbsent(normalizedPath, option);
      if (previous != null && !previous.getRawOption().equals(option.getRawOption())) {
        throw new IllegalStateException(
            "Ambiguous Cassandra Java driver option paths: '%s' and '%s'."
                .formatted(previous.getRawOption().getPath(), option.getRawOption().getPath()));
      }
    }
    return optionsByNormalizedPath;
  }

  private static String normalize(String path) {
    return path.replace('-', '.');
  }

  private static Map<String, TypedDriverOption<?>> coreOptionsByPath() {
    var optionsByPath = new HashMap<String, TypedDriverOption<?>>();
    for (TypedDriverOption<?> option : TypedDriverOption.builtInValues()) {
      if (option.getRawOption() instanceof DefaultDriverOption) {
        optionsByPath.put(option.getRawOption().getPath(), option);
      }
    }
    return optionsByPath;
  }
}
