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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.Authenticator;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.Test;

class CassandraClientProducerTest {

  @Test
  void leavesAuthenticationUnconfiguredWhenNoAuthenticationSettingsArePresent() {
    var configuration = configuration().build();
    @SuppressWarnings("unchecked")
    Instance<AuthProvider> providers = mock(Instance.class);

    assertThat(CassandraClientProducer.resolveAuthenticationProvider(configuration, providers))
        .isEmpty();
    verifyNoInteractions(providers);
  }

  @Test
  void selectsDefaultProviderForUsernameAndPassword() throws Exception {
    var configuration = configuration().authUsername("polaris").authPassword("secret").build();
    @SuppressWarnings("unchecked")
    Instance<AuthProvider> providers = mock(Instance.class);
    @SuppressWarnings("unchecked")
    Instance<AuthProvider> selected = mock(Instance.class);
    AuthProvider provider = mock(AuthProvider.class);
    when(providers.select(CassandraAuthentication.Literal.of("default"))).thenReturn(selected);
    when(selected.get()).thenReturn(provider);

    var resolved =
        CassandraClientProducer.resolveAuthenticationProvider(configuration, providers)
            .orElseThrow();

    assertThat(resolved).isInstanceOf(NonClosingAuthProvider.class);
    verify(providers).select(CassandraAuthentication.Literal.of("default"));
    resolved.close();
    verifyNoMoreInteractions(provider);
  }

  @Test
  void selectsNamedCustomProvider() throws Exception {
    var configuration = configuration().authProviderName("custom").build();
    @SuppressWarnings("unchecked")
    Instance<AuthProvider> providers = mock(Instance.class);
    @SuppressWarnings("unchecked")
    Instance<AuthProvider> selected = mock(Instance.class);
    AuthProvider provider = mock(AuthProvider.class);
    when(providers.select(CassandraAuthentication.Literal.of("custom"))).thenReturn(selected);
    when(selected.get()).thenReturn(provider);

    var resolved =
        CassandraClientProducer.resolveAuthenticationProvider(configuration, providers)
            .orElseThrow();

    assertThat(resolved).isInstanceOf(NonClosingAuthProvider.class);
    verify(providers).select(CassandraAuthentication.Literal.of("custom"));
    resolved.close();
    verifyNoMoreInteractions(provider);
  }

  @Test
  void delegatesAuthenticationWithoutClosingTheCdiProvider() {
    AuthProvider provider = mock(AuthProvider.class);
    EndPoint endPoint = mock(EndPoint.class);
    Authenticator authenticator = mock(Authenticator.class);
    when(provider.newAuthenticator(endPoint, "PasswordAuthenticator")).thenReturn(authenticator);
    var wrapper = new NonClosingAuthProvider(provider);

    assertThat(wrapper.newAuthenticator(endPoint, "PasswordAuthenticator")).isSameAs(authenticator);
    wrapper.onMissingChallenge(endPoint);
    wrapper.close();

    verify(provider).newAuthenticator(endPoint, "PasswordAuthenticator");
    verify(provider).onMissingChallenge(endPoint);
    verifyNoMoreInteractions(provider);
  }

  @Test
  void closesDefaultProviderWhenCdiDestroysIt() throws Exception {
    AuthProvider provider = mock(AuthProvider.class);

    new CassandraClientProducer().closeDefaultAuthenticationProvider(provider);

    verify(provider).close();
  }

  @Test
  void createsPlainTextProviderWithAuthorizationId() {
    var configuration =
        configuration()
            .authUsername("polaris")
            .authPassword("secret")
            .authAuthorizationId("proxy")
            .build();

    assertThat(new CassandraClientProducer().defaultAuthenticationProvider(configuration))
        .isInstanceOf(ProgrammaticPlainTextAuthProvider.class);
  }

  @Test
  void rejectsPartialPlainTextCredentials() {
    var configuration = configuration().authUsername("polaris").build();
    @SuppressWarnings("unchecked")
    Instance<AuthProvider> providers = mock(Instance.class);

    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> CassandraClientProducer.resolveAuthenticationProvider(configuration, providers))
        .withMessageContaining("must either both be configured or both be absent");
  }

  @Test
  void rejectsPlainTextCredentialsWithCustomProvider() {
    var configuration =
        configuration()
            .authProviderName("custom")
            .authUsername("polaris")
            .authPassword("secret")
            .build();
    @SuppressWarnings("unchecked")
    Instance<AuthProvider> providers = mock(Instance.class);

    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> CassandraClientProducer.resolveAuthenticationProvider(configuration, providers))
        .withMessageContaining("only be used with the default authentication provider");
  }

  @Test
  void rejectsMissingNamedProvider() {
    var configuration = configuration().authProviderName("custom").build();
    @SuppressWarnings("unchecked")
    Instance<AuthProvider> providers = mock(Instance.class);
    @SuppressWarnings("unchecked")
    Instance<AuthProvider> selected = mock(Instance.class);
    when(providers.select(CassandraAuthentication.Literal.of("custom"))).thenReturn(selected);
    when(selected.isUnsatisfied()).thenReturn(true);

    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> CassandraClientProducer.resolveAuthenticationProvider(configuration, providers))
        .withMessageContaining("No Cassandra authentication provider named 'custom' is available");
  }

  @Test
  void rejectsAmbiguousNamedProvider() {
    var configuration = configuration().authProviderName("custom").build();
    @SuppressWarnings("unchecked")
    Instance<AuthProvider> providers = mock(Instance.class);
    @SuppressWarnings("unchecked")
    Instance<AuthProvider> selected = mock(Instance.class);
    when(providers.select(CassandraAuthentication.Literal.of("custom"))).thenReturn(selected);
    when(selected.isAmbiguous()).thenReturn(true);

    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> CassandraClientProducer.resolveAuthenticationProvider(configuration, providers))
        .withMessageContaining("Cassandra authentication provider 'custom' is ambiguous");
  }

  private static ImmutableBuildableCassandraConfiguration.Builder configuration() {
    return CassandraConfiguration.BuildableCassandraConfiguration.builder();
  }
}
