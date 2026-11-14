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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import io.vertx.core.net.SSLOptions;
import jakarta.enterprise.inject.Instance;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.net.ssl.SSLContext;
import org.apache.polaris.persistence.nosql.api.backend.BackendConfiguration;
import org.apache.polaris.persistence.nosql.cassandra.CassandraBackendFactory;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.junit.jupiter.api.Test;

class CassandraQuarkusSupportTest {

  @Test
  void tlsEngineAppliesRegistryProtocolsAndCipherSuites() throws Exception {
    var sslContext = SSLContext.getDefault();
    var supportedEngine = sslContext.createSSLEngine();
    var protocol = supportedEngine.getSupportedProtocols()[0];
    var cipherSuite = supportedEngine.getSupportedCipherSuites()[0];
    var sslOptions =
        new SSLOptions()
            .setEnabledSecureTransportProtocols(java.util.Set.of(protocol))
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
    BackendConfiguration backendConfiguration = mock(BackendConfiguration.class);
    @SuppressWarnings("unchecked")
    Instance<CqlSession> cqlSession = mock(Instance.class);
    when(backendConfiguration.backend()).thenReturn(Optional.empty());

    var response =
        new CassandraAsyncReadinessHealthCheck(backendConfiguration, cqlSession)
            .call()
            .await()
            .indefinitely();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
    verifyNoInteractions(cqlSession);
  }

  @Test
  void healthCheckReportsDownWhenCassandraQueryFails() {
    BackendConfiguration backendConfiguration = mock(BackendConfiguration.class);
    @SuppressWarnings("unchecked")
    Instance<CqlSession> cqlSession = mock(Instance.class);
    CqlSession session = mock(CqlSession.class);
    when(backendConfiguration.backend()).thenReturn(Optional.of(CassandraBackendFactory.NAME));
    when(cqlSession.isResolvable()).thenReturn(true);
    when(cqlSession.get()).thenReturn(session);
    when(session.executeAsync("SELECT release_version FROM system.local"))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("unavailable")));

    var response =
        new CassandraAsyncReadinessHealthCheck(backendConfiguration, cqlSession)
            .call()
            .await()
            .indefinitely();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
    assertThat(response.getData())
        .contains(java.util.Map.of("reason", "Cassandra health query failed"));
  }

  @Test
  void healthCheckReportsDownWhenCassandraQueryReturnsNoRow() {
    BackendConfiguration backendConfiguration = mock(BackendConfiguration.class);
    @SuppressWarnings("unchecked")
    Instance<CqlSession> cqlSession = mock(Instance.class);
    CqlSession session = mock(CqlSession.class);
    AsyncResultSet resultSet = mock(AsyncResultSet.class);
    when(backendConfiguration.backend()).thenReturn(Optional.of(CassandraBackendFactory.NAME));
    when(cqlSession.isResolvable()).thenReturn(true);
    when(cqlSession.get()).thenReturn(session);
    when(session.executeAsync("SELECT release_version FROM system.local"))
        .thenReturn(CompletableFuture.completedFuture(resultSet));

    var response =
        new CassandraAsyncReadinessHealthCheck(backendConfiguration, cqlSession)
            .call()
            .await()
            .indefinitely();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
    assertThat(response.getData())
        .contains(java.util.Map.of("reason", "system.local returned no row"));
  }

  @Test
  void healthCheckReportsDownWhenCassandraSessionIsUnavailable() {
    BackendConfiguration backendConfiguration = mock(BackendConfiguration.class);
    @SuppressWarnings("unchecked")
    Instance<CqlSession> cqlSession = mock(Instance.class);
    when(backendConfiguration.backend()).thenReturn(Optional.of(CassandraBackendFactory.NAME));
    when(cqlSession.isResolvable()).thenReturn(false);

    var response =
        new CassandraAsyncReadinessHealthCheck(backendConfiguration, cqlSession)
            .call()
            .await()
            .indefinitely();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
    assertThat(response.getData())
        .contains(java.util.Map.of("reason", "Cassandra session is unavailable"));
    verify(cqlSession).isResolvable();
  }

  @Test
  void healthCheckReportsUpWhenCassandraQuerySucceeds() {
    BackendConfiguration backendConfiguration = mock(BackendConfiguration.class);
    @SuppressWarnings("unchecked")
    Instance<CqlSession> cqlSession = mock(Instance.class);
    CqlSession session = mock(CqlSession.class);
    AsyncResultSet resultSet = mock(AsyncResultSet.class);
    when(backendConfiguration.backend()).thenReturn(Optional.of(CassandraBackendFactory.NAME));
    when(cqlSession.isResolvable()).thenReturn(true);
    when(cqlSession.get()).thenReturn(session);
    when(session.executeAsync("SELECT release_version FROM system.local"))
        .thenReturn(CompletableFuture.completedFuture(resultSet));
    when(resultSet.one()).thenReturn(mock());

    var response =
        new CassandraAsyncReadinessHealthCheck(backendConfiguration, cqlSession)
            .call()
            .await()
            .indefinitely();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
  }
}
