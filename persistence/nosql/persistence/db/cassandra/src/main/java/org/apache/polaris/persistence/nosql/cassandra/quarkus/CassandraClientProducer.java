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
package org.apache.polaris.persistence.nosql.cassandra.quarkus;

import static com.datastax.oss.driver.api.core.config.TypedDriverOption.METRICS_FACTORY_CLASS;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.METRICS_ID_GENERATOR_CLASS;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.METRICS_ID_GENERATOR_PREFIX;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.METRICS_NODE_ENABLED;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.METRICS_SESSION_ENABLED;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.internal.core.metrics.TaggingMetricIdGenerator;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.tls.TlsConfiguration;
import io.quarkus.tls.TlsConfigurationRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import java.util.List;
import java.util.Optional;
import org.apache.polaris.persistence.nosql.cassandra.CassandraConfiguration;
import org.apache.polaris.persistence.nosql.cassandra.CassandraSessionFactory;

@ApplicationScoped
class CassandraClientProducer {
  static final String DEFAULT_AUTHENTICATION_PROVIDER_NAME =
      CassandraConfiguration.DEFAULT_AUTHENTICATION_PROVIDER_NAME;

  private static final String MICROMETER_METRICS_FACTORY =
      "com.datastax.oss.driver.internal.metrics.micrometer.MicrometerMetricsFactory";

  @Produces
  @ApplicationScoped
  CqlSession cqlSession(
      CassandraConfiguration configuration,
      TlsConfigurationRegistry tlsConfigurationRegistry,
      @Any Instance<MeterRegistry> meterRegistry,
      @Any Instance<AuthProvider> authenticationProviders) {
    var authenticationProvider =
        resolveAuthenticationProvider(configuration, authenticationProviders);
    var options = OptionsMap.driverDefaults();
    if (meterRegistry.isResolvable()) {
      configureMetrics(options);
    }
    CassandraSessionFactory.configureDriverOptions(
        configuration, authenticationProvider.isPresent(), options);

    var clientBuilder = CassandraSessionFactory.cqlSessionBuilder(options);
    authenticationProvider.ifPresent(clientBuilder::withAuthProvider);

    configuration
        .tlsConfigurationName()
        .ifPresent(name ->
            clientBuilder.withSslEngineFactory(
                sslEngineFactory(configuration, tlsConfigurationRegistry, name)));

    if (meterRegistry.isResolvable()) {
      clientBuilder.withMetricRegistry(meterRegistry.get());
    }

    return clientBuilder.build();
  }

  @Produces
  @ApplicationScoped
  @CassandraAuthentication(DEFAULT_AUTHENTICATION_PROVIDER_NAME)
  AuthProvider defaultAuthenticationProvider(CassandraConfiguration configuration) {
    return CassandraSessionFactory.plainTextAuthProvider(configuration)
        .orElseThrow(() ->
            new IllegalArgumentException(
                "The default Cassandra authentication provider requires both auth.username and auth.password."));
  }

  void closeDefaultAuthenticationProvider(
      @Disposes @CassandraAuthentication(DEFAULT_AUTHENTICATION_PROVIDER_NAME)
          AuthProvider authenticationProvider) {
    try {
      authenticationProvider.close();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to close the default Cassandra authentication provider.", e);
    }
  }

  void closeCqlSession(@Disposes CqlSession cqlSession) {
    cqlSession.close();
  }

  static Optional<AuthProvider> resolveAuthenticationProvider(
      CassandraConfiguration configuration, Instance<AuthProvider> providers) {
    CassandraSessionFactory.validateAuthentication(configuration);

    String selectedName = configuration.authProviderName();
    if (DEFAULT_AUTHENTICATION_PROVIDER_NAME.equals(selectedName)
        && configuration.authUsername().isEmpty()) {
      return Optional.empty();
    }

    if (!DEFAULT_AUTHENTICATION_PROVIDER_NAME.equals(selectedName)
        && configuration.authUsername().isPresent()) {
      throw new IllegalArgumentException(
          "Cassandra auth.username and auth.password can only be used with the default authentication provider.");
    }

    Instance<AuthProvider> selected =
        providers.select(CassandraAuthentication.Literal.of(selectedName));
    if (selected.isUnsatisfied()) {
      throw new IllegalArgumentException(
          "No Cassandra authentication provider named '%s' is available.".formatted(selectedName));
    }
    if (selected.isAmbiguous()) {
      throw new IllegalArgumentException(
          "Cassandra authentication provider '%s' is ambiguous.".formatted(selectedName));
    }
    return Optional.of(new NonClosingAuthProvider(selected.get()));
  }

  private static QuarkusTlsSslEngineFactory sslEngineFactory(
      CassandraConfiguration configuration,
      TlsConfigurationRegistry tlsConfigurationRegistry,
      String name) {
    TlsConfiguration tlsConfiguration =
        tlsConfigurationRegistry
            .get(name)
            .orElseThrow(() ->
                new IllegalArgumentException(
                    "Quarkus TLS configuration '%s' does not exist.".formatted(name)));
    try {
      return new QuarkusTlsSslEngineFactory(
          tlsConfiguration.createSSLContext(),
          tlsConfiguration.getSSLOptions(),
          configuration.hostnameValidation());
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to create SSL context from Quarkus TLS configuration '%s'.".formatted(name), e);
    }
  }

  static void configureMetrics(OptionsMap options) {
    options.put(METRICS_FACTORY_CLASS, MICROMETER_METRICS_FACTORY);
    options.put(METRICS_ID_GENERATOR_CLASS, TaggingMetricIdGenerator.class.getName());
    options.put(METRICS_ID_GENERATOR_PREFIX, "cassandra");
    options.put(METRICS_SESSION_ENABLED, List.of("connected-nodes", "cql-requests"));
    options.put(
        METRICS_NODE_ENABLED,
        List.of(
            "pool.open-connections",
            "errors.request.write-timeouts",
            "errors.request.read-timeouts",
            "errors.request.unavailables",
            "errors.request.others",
            "errors.connection.init"));
  }
}
