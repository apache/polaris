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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CassandraDriverOptionsTest {
  @Test
  void mapsPolarisTimeoutAndTlsPropertyNames() {
    var configuration =
        new SmallRyeConfigBuilder()
            .withMapping(CassandraConfiguration.class)
            .addDiscoveredConverters()
            .withSources(
                new PropertiesConfigSource(
                    Map.of(
                        "polaris.persistence.nosql.cassandra.request-timeout", "45s",
                        "polaris.persistence.nosql.cassandra.tls-hostname-validation", "false",
                        "polaris.persistence.nosql.cassandra.tls-configuration-name", "cassandra"),
                    "test"))
            .build()
            .getConfigMapping(CassandraConfiguration.class);
    assertThat(configuration.requestTimeout()).contains(Duration.ofSeconds(45));
    assertThat(configuration.hostnameValidation()).isFalse();
    assertThat(
            CassandraSessionFactory.driverOptions(configuration)
                .get(TypedDriverOption.REQUEST_TIMEOUT))
        .isEqualTo(Duration.ofSeconds(45));
  }

  @Test
  void mapsEmptyMetricListFromProperties() {
    var configuration =
        new SmallRyeConfigBuilder()
            .withMapping(CassandraConfiguration.class)
            .addDiscoveredConverters()
            .withSources(
                new PropertiesConfigSource(
                    Map.of(
                        "polaris.persistence.nosql.cassandra.driver.advanced.metrics.node.enabled",
                        ""),
                    "test"))
            .build()
            .getConfigMapping(CassandraConfiguration.class);
    assertThat(configuration.driverOptions()).containsEntry("advanced.metrics.node.enabled", "");
    assertThat(
            CassandraSessionFactory.driverOptions(configuration)
                .get(TypedDriverOption.METRICS_NODE_ENABLED))
        .isEmpty();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "basic.request.consistency", "basic.request.serial-consistency",
        "basic-request-consistency", "basic-request-serial-consistency",
        "advanced.metrics.factory.class", "advanced.metrics.id-generator.class",
        "advanced.metrics.id-generator.prefix"
      })
  void rejectsPolarisManagedDriverOptions(String path) {
    var configuration =
        CassandraConfiguration.BuildableCassandraConfiguration.builder()
            .driverOptions(Map.of(path, "value"))
            .build();
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CassandraSessionFactory.driverOptions(configuration))
        .withMessageContaining("configured directly by Polaris");
  }

  @Test
  void allowsMetricHistogramTuning() {
    var configuration =
        CassandraConfiguration.BuildableCassandraConfiguration.builder()
            .driverOptions(Map.of("advanced.metrics.session.cql-requests.highest-latency", "10s"))
            .build();
    assertThat(
            CassandraSessionFactory.driverOptions(configuration)
                .get(TypedDriverOption.METRICS_SESSION_CQL_REQUESTS_HIGHEST))
        .isEqualTo(Duration.ofSeconds(10));
  }
}
