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

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.microprofile.health.HealthCheckResponse.Status.DOWN;
import static org.eclipse.microprofile.health.HealthCheckResponse.Status.UP;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import io.vertx.core.net.SSLOptions;
import jakarta.enterprise.inject.Instance;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;
import org.apache.polaris.persistence.nosql.api.backend.BackendConfiguration;
import org.apache.polaris.persistence.nosql.cassandra.CassandraBackendFactory;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CassandraQuarkusSupportTest {
  private final BackendConfiguration backendConfiguration = mock(BackendConfiguration.class);

  private final Instance<CqlSession> cqlSession = mock();

  private final CqlSession session = mock(CqlSession.class);
  private final CassandraAsyncReadinessHealthCheck healthCheck =
      new CassandraAsyncReadinessHealthCheck(backendConfiguration, cqlSession);

  @BeforeEach
  void selectCassandra() {
    when(backendConfiguration.backend()).thenReturn(Optional.of(CassandraBackendFactory.NAME));
    when(cqlSession.isResolvable()).thenReturn(true);
    when(cqlSession.get()).thenReturn(session);
  }

  @Test
  void tlsEngineAppliesRegistryProtocolsAndCipherSuites() throws Exception {
    var sslContext = SSLContext.getDefault();
    var supportedEngine = sslContext.createSSLEngine();
    var protocol = supportedEngine.getSupportedProtocols()[0];
    var cipherSuite = supportedEngine.getSupportedCipherSuites()[0];
    var sslOptions =
        new SSLOptions()
            .setEnabledSecureTransportProtocols(Set.of(protocol))
            .addEnabledCipherSuite(cipherSuite);
    EndPoint endPoint = mock(EndPoint.class);
    when(endPoint.resolve()).thenReturn(InetSocketAddress.createUnresolved("localhost", 9042));

    var engine =
        new QuarkusTlsSslEngineFactory(sslContext, sslOptions, true).newSslEngine(endPoint);

    assertThat(engine.getEnabledProtocols()).containsExactly(protocol);
    assertThat(engine.getEnabledCipherSuites()).containsExactly(cipherSuite);
  }

  @Test
  void healthCheckIsSuccessfulNoOpWhenCassandraIsNotSelected() {
    when(backendConfiguration.backend()).thenReturn(Optional.empty());

    assertThat(checkHealth().getStatus()).isEqualTo(UP);
    verifyNoInteractions(cqlSession);
  }

  @Test
  void healthCheckReportsDownWhenCassandraSessionIsUnavailable() {
    when(cqlSession.isResolvable()).thenReturn(false);

    var response = checkHealth();

    assertThat(response.getStatus()).isEqualTo(DOWN);
    assertThat(response.getData()).contains(Map.of("reason", "Cassandra session is unavailable"));
    verify(cqlSession).isResolvable();
  }

  @ParameterizedTest
  @MethodSource("healthQueryResults")
  void healthCheckReportsQueryResult(
      CompletableFuture<AsyncResultSet> result, HealthCheckResponse.Status status, String reason) {
    when(session.executeAsync("SELECT release_version FROM system.local")).thenReturn(result);

    var response = checkHealth();

    assertThat(response.getStatus()).isEqualTo(status);
    if (reason == null) {
      assertThat(response.getData()).isEmpty();
    } else {
      assertThat(response.getData()).contains(Map.of("reason", reason));
    }
  }

  static Stream<Arguments> healthQueryResults() {
    var empty = mock(AsyncResultSet.class);
    var nonempty = mock(AsyncResultSet.class);
    when(nonempty.one()).thenReturn(mock(Row.class));
    return Stream.of(
        Arguments.of(
            CompletableFuture.failedFuture(new RuntimeException("unavailable")),
            DOWN,
            "Cassandra health query failed"),
        Arguments.of(
            CompletableFuture.completedFuture(empty), DOWN, "system.local returned no row"),
        Arguments.of(CompletableFuture.completedFuture(nonempty), UP, null));
  }

  private HealthCheckResponse checkHealth() {
    return healthCheck.call().await().atMost(Duration.ofSeconds(5));
  }
}
