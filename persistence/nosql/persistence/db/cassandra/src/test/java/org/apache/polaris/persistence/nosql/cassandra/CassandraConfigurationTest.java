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

import static java.util.stream.Collectors.toMap;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraBackendConfig.DEFAULT_DDL_TIMEOUT;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraBackendConfig.DEFAULT_DML_TIMEOUT;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraBackendConfig.DEFAULT_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import io.smallrye.config.EnvConfigSource;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

class CassandraConfigurationTest {

  @Test
  void mapsDriverAndPolarisOptions() {
    var configuration =
        configuration(
            Map.of(
                "driver.basic.contact-points",
                "cassandra-a.example:9042,cassandra-b.example:9142",
                "driver.basic.load-balancing-policy.local-datacenter",
                "dc1",
                "driver.advanced.connection.connect-timeout",
                "15s",
                "request-timeout",
                "45s",
                "auth.username",
                "polaris",
                "auth.password",
                "secret",
                "auth.authorization-id",
                "proxy-user",
                "auth.provider-name",
                "default",
                "tls-configuration-name",
                "cassandra",
                "tls-hostname-validation",
                "false"));

    assertThat(configuration.keyspace()).isEqualTo("polaris");
    assertThat(configuration.requestTimeout()).contains(Duration.ofSeconds(45));
    assertThat(configuration.authUsername()).contains("polaris");
    assertThat(configuration.authPassword()).contains("secret");
    assertThat(configuration.authAuthorizationId()).contains("proxy-user");
    assertThat(configuration.authProviderName()).isEqualTo("default");
    assertThat(configuration.tlsConfigurationName()).contains("cassandra");
    assertThat(configuration.hostnameValidation()).isFalse();

    var options = CassandraSessionFactory.driverOptions(configuration);
    assertThat(options.get(TypedDriverOption.CONTACT_POINTS))
        .containsExactly("cassandra-a.example:9042", "cassandra-b.example:9142");
    assertThat(options.get(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER)).isEqualTo("dc1");
    assertThat(options.get(TypedDriverOption.CONNECTION_CONNECT_TIMEOUT))
        .isEqualTo(Duration.ofSeconds(15));
  }

  @Test
  void mapsPolarisTimeoutAndTlsPropertyNames() {
    var configuration =
        configuration(
            Map.of(
                "request-timeout", "45s",
                "tls-hostname-validation", "false",
                "tls-configuration-name", "cassandra"));
    assertThat(configuration.requestTimeout()).contains(Duration.ofSeconds(45));
    assertThat(configuration.hostnameValidation()).isFalse();
    assertThat(
            CassandraSessionFactory.driverOptions(configuration)
                .get(TypedDriverOption.REQUEST_TIMEOUT))
        .isEqualTo(Duration.ofSeconds(45));
  }

  @Test
  void mapsEmptyMetricListFromProperties() {
    var configuration = configuration(Map.of("driver.advanced.metrics.node.enabled", ""));
    assertThat(configuration.driverOptions()).containsEntry("advanced.metrics.node.enabled", "");
    assertThat(
            CassandraSessionFactory.driverOptions(configuration)
                .get(TypedDriverOption.METRICS_NODE_ENABLED))
        .isEmpty();
  }

  @Test
  void requestTimeoutDefaultsToLongerPersistenceTimeout() {
    var configuration = configuration(Map.of("ddl-timeout", "10s", "dml-timeout", "20s"));

    assertThat(CassandraSessionFactory.requestTimeout(configuration))
        .isEqualTo(Duration.ofSeconds(20));
  }

  @Test
  void mapsAndAppliesAdditionalDriverOptions() {
    var configuration =
        configuration(
            Map.of(
                "driver.basic.request.page-size",
                "500",
                "driver.advanced.connection.init-query-timeout",
                "15s",
                "driver.advanced.ssl-engine-factory.cipher-suites",
                "TLS_AES_128_GCM_SHA256,TLS_AES_256_GCM_SHA384"));

    assertThat(configuration.driverOptions())
        .containsEntry("basic.request.page-size", "500")
        .containsEntry("advanced.connection.init-query-timeout", "15s")
        .containsEntry(
            "advanced.ssl-engine-factory.cipher-suites",
            "TLS_AES_128_GCM_SHA256,TLS_AES_256_GCM_SHA384");

    var options = CassandraSessionFactory.driverOptions(configuration);
    assertThat(options.get(TypedDriverOption.REQUEST_PAGE_SIZE)).isEqualTo(500);
    assertThat(options.get(TypedDriverOption.CONNECTION_INIT_QUERY_TIMEOUT))
        .isEqualTo(Duration.ofSeconds(15));
    assertThat(options.get(TypedDriverOption.SSL_CIPHER_SUITES))
        .containsExactly("TLS_AES_128_GCM_SHA256", "TLS_AES_256_GCM_SHA384");
  }

  @ParameterizedTest
  @CsvSource({
    "basic.request.timeout,10s,configured directly by Polaris",
    "advanced.not-a-driver-option,value,Unknown Cassandra Java driver option",
    "advanced-metrics-factory-class,example.MetricsFactory,configured directly by Polaris"
  })
  void rejectsPolarisManagedOrUnknownDriverOptions(String path, String value, String message) {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> CassandraDriverOptions.apply(Map.of(path, value), OptionsMap.driverDefaults()))
        .withMessageContaining(message);
  }

  @Test
  void standaloneFactoryRejectsQuarkusTlsConfiguration() {
    var configuration =
        CassandraConfiguration.BuildableCassandraConfiguration.builder()
            .tlsConfigurationName("cassandra")
            .build();

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new CassandraBackendFactory().buildConfiguration(configuration))
        .withMessageContaining("requires the Quarkus CDI integration");
  }

  @ParameterizedTest
  @CsvSource({
    "advanced.ssl-engine-factory.cipher-suites,TLS_AES_128_GCM_SHA256",
    "basic.cloud.secure-connect-bundle,secure-connect.zip"
  })
  void rejectsDriverTlsOptionsWithQuarkusTlsConfiguration(String option, String value) {
    var configuration =
        configuration(Map.of("tls-configuration-name", "cassandra", "driver." + option, value));

    assertThatIllegalArgumentException()
        .isThrownBy(() -> CassandraSessionFactory.driverOptions(configuration))
        .withMessageContaining("cannot be combined with a Quarkus TLS configuration");
  }

  @Test
  void mapsDriverOptionsFromEnvironmentVariables() {
    var configuration =
        new SmallRyeConfigBuilder()
            .withMapping(CassandraConfiguration.class)
            .addDiscoveredConverters()
            .withSources(
                new EnvConfigSource(
                    Map.of(
                        "POLARIS_PERSISTENCE_NOSQL_CASSANDRA_DRIVER_ADVANCED_CONNECTION_INIT_QUERY_TIMEOUT",
                        "15s"),
                    300))
            .build()
            .getConfigMapping(CassandraConfiguration.class);

    assertThat(
            CassandraSessionFactory.driverOptions(configuration)
                .get(TypedDriverOption.CONNECTION_INIT_QUERY_TIMEOUT))
        .isEqualTo(Duration.ofSeconds(15));
  }

  @Test
  void rejectsDriverAuthOptionsWithPolarisAuthSettings() {
    var configuration =
        configuration(
            Map.of(
                "auth.username",
                "polaris",
                "auth.password",
                "secret",
                "driver.advanced.auth-provider.class",
                "example.AuthProvider"));

    assertThatIllegalArgumentException()
        .isThrownBy(() -> CassandraSessionFactory.driverOptions(configuration))
        .withMessageContaining("cannot be combined with Polaris auth settings");
  }

  @Test
  void rejectsDriverAuthOptionsWithCdiAuthenticationProvider() {
    var configuration =
        configuration(Map.of("driver.advanced.auth-provider.class", "example.AuthProvider"));

    assertThatIllegalArgumentException()
        .isThrownBy(() -> CassandraSessionFactory.driverOptions(configuration, true))
        .withMessageContaining("cannot be combined with Polaris auth settings");
  }

  @Test
  void buildableConfigurationSuppliesDefaults() {
    var configuration = CassandraConfiguration.BuildableCassandraConfiguration.builder().build();

    assertThat(configuration.consistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
    assertThat(configuration.serialConsistencyLevel())
        .isEqualTo(DefaultConsistencyLevel.LOCAL_SERIAL);
    assertThat(configuration.keyspace()).isEqualTo(DEFAULT_KEYSPACE);
    assertThat(configuration.hostnameValidation()).isTrue();
    assertThat(configuration.requestTimeout()).isEmpty();
    assertThat(configuration.authUsername()).isEmpty();
    assertThat(configuration.authPassword()).isEmpty();
    assertThat(configuration.authAuthorizationId()).isEmpty();
    assertThat(configuration.authProviderName()).isEqualTo("default");
    assertThat(configuration.driverOptions()).isEmpty();
    assertThat(configuration.ddlTimeout()).isEqualTo(Duration.parse(DEFAULT_DDL_TIMEOUT));
    assertThat(configuration.dmlTimeout()).isEqualTo(Duration.parse(DEFAULT_DML_TIMEOUT));
  }

  @Test
  void mapsPolarisConsistencyLevels() {
    var configuration =
        configuration(
            Map.of(
                "consistency-level", "QUORUM",
                "serial-consistency-level", "SERIAL"));
    assertThat(configuration.consistencyLevel()).isEqualTo(DefaultConsistencyLevel.QUORUM);
    assertThat(configuration.serialConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.SERIAL);
    CassandraSessionFactory.driverOptions(configuration);
  }

  @ParameterizedTest
  @EnumSource(
      value = DefaultConsistencyLevel.class,
      names = {"LOCAL_QUORUM", "QUORUM", "ALL"},
      mode = EnumSource.Mode.EXCLUDE)
  void rejectsUnsupportedRegularConsistency(DefaultConsistencyLevel level) {
    var configuration =
        CassandraConfiguration.BuildableCassandraConfiguration.builder()
            .consistencyLevel(level)
            .build();
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CassandraSessionFactory.driverOptions(configuration))
        .withMessageContaining("consistency-level must be");
  }

  @ParameterizedTest
  @EnumSource(
      value = DefaultConsistencyLevel.class,
      names = {"LOCAL_SERIAL", "SERIAL"},
      mode = EnumSource.Mode.EXCLUDE)
  void rejectsNonSerialConsistency(DefaultConsistencyLevel level) {
    var configuration =
        CassandraConfiguration.BuildableCassandraConfiguration.builder()
            .serialConsistencyLevel(level)
            .build();
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CassandraSessionFactory.driverOptions(configuration))
        .withMessageContaining("serial-consistency-level must be");
  }

  private static CassandraConfiguration configuration(Map<String, String> properties) {
    return new SmallRyeConfigBuilder()
        .withMapping(CassandraConfiguration.class)
        .addDiscoveredConverters()
        .withSources(
            new PropertiesConfigSource(
                properties.entrySet().stream()
                    .collect(
                        toMap(
                            entry -> "polaris.persistence.nosql.cassandra." + entry.getKey(),
                            Map.Entry::getValue)),
                "test"))
        .build()
        .getConfigMapping(CassandraConfiguration.class);
  }
}
