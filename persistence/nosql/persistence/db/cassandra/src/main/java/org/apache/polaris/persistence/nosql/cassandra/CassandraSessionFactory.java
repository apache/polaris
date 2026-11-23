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

import static com.datastax.oss.driver.api.core.config.TypedDriverOption.REQUEST_TIMEOUT;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import java.time.Duration;
import java.util.Optional;

/** Builds Cassandra Java-driver sessions from Polaris configuration. */
public final class CassandraSessionFactory {
  private CassandraSessionFactory() {}

  public static OptionsMap driverOptions(CassandraConfiguration configuration) {
    return driverOptions(configuration, false);
  }

  public static OptionsMap driverOptions(
      CassandraConfiguration configuration, boolean cdiAuthenticationProviderConfigured) {
    var options = OptionsMap.driverDefaults();
    configureDriverOptions(configuration, cdiAuthenticationProviderConfigured, options);
    return options;
  }

  /** Applies Polaris configuration over driver defaults supplied by the caller. */
  public static void configureDriverOptions(
      CassandraConfiguration configuration,
      boolean cdiAuthenticationProviderConfigured,
      OptionsMap options) {
    validateAuthentication(configuration);
    CassandraDriverOptions.apply(configuration, cdiAuthenticationProviderConfigured, options);
    options.put(REQUEST_TIMEOUT, requestTimeout(configuration));
  }

  public static CqlSessionBuilder cqlSessionBuilder(
      CassandraConfiguration configuration, OptionsMap options) {
    if (!CassandraConfiguration.DEFAULT_AUTHENTICATION_PROVIDER_NAME.equals(
        configuration.authProviderName())) {
      throw new IllegalArgumentException(
          "Cassandra auth.provider-name requires the Quarkus CDI integration.");
    }
    var sessionBuilder = cqlSessionBuilder(options);
    plainTextAuthProvider(configuration).ifPresent(sessionBuilder::withAuthProvider);
    return sessionBuilder;
  }

  public static CqlSessionBuilder cqlSessionBuilder(OptionsMap options) {
    return CqlSession.builder().withConfigLoader(DriverConfigLoader.fromMap(options));
  }

  public static Duration requestTimeout(CassandraConfiguration configuration) {
    return configuration
        .requestTimeout()
        .orElseGet(() ->
            configuration.ddlTimeout().compareTo(configuration.dmlTimeout()) >= 0
                ? configuration.ddlTimeout()
                : configuration.dmlTimeout());
  }

  public static Optional<AuthProvider> plainTextAuthProvider(CassandraConfiguration configuration) {
    validateAuthentication(configuration);
    if (configuration.authUsername().isEmpty()) {
      return Optional.empty();
    }
    String username = configuration.authUsername().orElseThrow();
    String password = configuration.authPassword().orElseThrow();
    return Optional.of(
        configuration
            .authAuthorizationId()
            .<AuthProvider>map(authorizationId ->
                new ProgrammaticPlainTextAuthProvider(username, password, authorizationId))
            .orElseGet(() -> new ProgrammaticPlainTextAuthProvider(username, password)));
  }

  public static void validateAuthentication(CassandraConfiguration configuration) {
    if (configuration.authUsername().isPresent() != configuration.authPassword().isPresent()) {
      throw new IllegalArgumentException(
          "Cassandra auth.username and auth.password must either both be configured or both be absent.");
    }
    if (configuration.authAuthorizationId().isPresent() && configuration.authUsername().isEmpty()) {
      throw new IllegalArgumentException(
          "Cassandra auth.authorization-id requires auth.username and auth.password.");
    }
  }
}
