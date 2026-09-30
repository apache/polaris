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

import static org.apache.polaris.persistence.nosql.cassandra.CassandraBackendConfig.DEFAULT_DDL_TIMEOUT;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraBackendConfig.DEFAULT_DML_TIMEOUT;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraBackendConfig.DEFAULT_KEYSPACE;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * Polaris persistence, Cassandra backend-specific configuration.
 *
 * <p>Configure Java-driver settings under {@code polaris.persistence.nosql.cassandra.driver}, using
 * exact driver option paths. For example, {@code
 * driver.basic.contact-points=cassandra.example:9042} configures the driver option {@code
 * basic.contact-points}.
 *
 * <p>For Quarkus TLS or mTLS, set {@code tls-configuration-name} and configure the selected {@code
 * quarkus.tls.<name>.*} TLS Registry configuration. This mapping does not contain certificate,
 * private-key, trust CA, protocol, or cipher-suite settings. A driver {@code application.conf}
 * resource is neither needed nor consulted.
 *
 * <p>Standalone users can configure the connection settings in this mapping, but cannot use {@code
 * tls-configuration-name}, which requires Quarkus's TLS Registry.
 *
 * <p>Additional Cassandra Java driver options can be configured under {@code driver}, using the
 * exact driver option path. For example, {@code driver.basic.request.consistency=LOCAL_QUORUM}
 * configures the driver option {@code basic.request.consistency}. This is an in-memory driver
 * configuration; a driver {@code application.conf} is not used. The Polaris-owned request timeout
 * and metrics settings cannot be configured through this subsection. When {@code
 * tls-configuration-name} is set, configure TLS settings exclusively through the Quarkus TLS
 * Registry rather than this subsection. Native driver authentication options can be used only when
 * the default {@code auth.provider-name} is selected and {@code auth.username}, {@code
 * auth.password}, and {@code auth.authorization-id} are absent.
 */
@ConfigMapping(prefix = "polaris.persistence.nosql.cassandra")
@StaticInitSafe
public interface CassandraConfiguration {

  String DEFAULT_AUTHENTICATION_PROVIDER_NAME = "default";

  /** The Cassandra keyspace containing the Polaris persistence tables. */
  @WithDefault(DEFAULT_KEYSPACE)
  String keyspace();

  /**
   * Whether to verify that the Cassandra server certificate identifies the contacted host.
   *
   * <p>Disabling this option is unsafe and should not be used in production.
   */
  @WithName("advanced.ssl-engine-factory.hostname-validation")
  @WithDefault("true")
  boolean hostnameValidation();

  /**
   * Session-wide Cassandra request timeout. Defaults to the larger of {@code ddl-timeout} and
   * {@code dml-timeout} when omitted.
   */
  @WithName("request.timeout")
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> requestTimeout();

  /**
   * The named CDI authentication provider to use in Quarkus deployments.
   *
   * <p>Defaults to {@code default}. With the default provider, Polaris uses username and password
   * when both are configured, or leaves authentication to the native driver when they are absent.
   */
  @WithName("auth.provider-name")
  @WithDefault(DEFAULT_AUTHENTICATION_PROVIDER_NAME)
  String authProviderName();

  /** Plaintext authentication username, used only with the {@code default} provider. */
  @WithName("auth.username")
  Optional<String> authUsername();

  /** Plaintext authentication password, used only with the {@code default} provider. */
  @WithName("auth.password")
  Optional<String> authPassword();

  /**
   * Optional plaintext authentication authorization ID, used only with the {@code default}
   * provider.
   *
   * <p>This option is effective only with servers that support proxy authentication. Apache
   * Cassandra ignores it.
   */
  @WithName("auth.authorization-id")
  Optional<String> authAuthorizationId();

  /** Additional Cassandra Java driver options, keyed by their exact driver option paths. */
  @WithName("driver")
  Map<String, String> driverOptions();

  /** The named Quarkus TLS Registry configuration to use for TLS or mTLS. */
  Optional<String> tlsConfigurationName();

  /** Timeout used when creating tables. */
  @WithDefault(DEFAULT_DDL_TIMEOUT)
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Duration ddlTimeout();

  /** Timeout used for queries and updates. */
  @WithDefault(DEFAULT_DML_TIMEOUT)
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Duration dmlTimeout();

  /** Buildable Cassandra configuration, primarily for programmatic backend setup and tests. */
  @PolarisImmutable
  interface BuildableCassandraConfiguration extends CassandraConfiguration {
    static ImmutableBuildableCassandraConfiguration.Builder builder() {
      return ImmutableBuildableCassandraConfiguration.builder();
    }

    @Override
    @Value.Default
    default String keyspace() {
      return DEFAULT_KEYSPACE;
    }

    @Override
    @Value.Default
    default boolean hostnameValidation() {
      return true;
    }

    @Override
    @Value.Default
    default String authProviderName() {
      return DEFAULT_AUTHENTICATION_PROVIDER_NAME;
    }

    @Override
    @Value.Default
    default Map<String, String> driverOptions() {
      return Map.of();
    }

    @Override
    @Value.Default
    default Duration ddlTimeout() {
      return Duration.parse(DEFAULT_DDL_TIMEOUT);
    }

    @Override
    @Value.Default
    default Duration dmlTimeout() {
      return Duration.parse(DEFAULT_DML_TIMEOUT);
    }
  }
}
