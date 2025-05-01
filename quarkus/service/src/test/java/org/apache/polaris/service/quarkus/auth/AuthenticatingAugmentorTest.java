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
package org.apache.polaris.service.quarkus.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import java.security.Principal;
import java.util.Optional;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.service.auth.Authenticator;
import org.apache.polaris.service.auth.PrincipalAuthInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AuthenticatingAugmentorTest {

  private AuthenticatingAugmentor augmentor;
  private Authenticator<PrincipalAuthInfo, AuthenticatedPolarisPrincipal> authenticator;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setup() {
    authenticator = mock(Authenticator.class);
    augmentor = new AuthenticatingAugmentor(authenticator);
  }

  @Test
  public void testAugmentAnonymousIdentity() {
    // Given
    SecurityIdentity anonymousIdentity =
        QuarkusSecurityIdentity.builder().setAnonymous(true).build();

    // When
    Uni<SecurityIdentity> result = augmentor.augment(anonymousIdentity, null);

    // Then
    assertThat(result.await().indefinitely()).isSameAs(anonymousIdentity);
  }

  @Test
  public void testAugmentMissingCredential() {
    // Given
    Principal nonPolarisPrincipal = mock(Principal.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder().setPrincipal(nonPolarisPrincipal).build();

    // When/Then
    assertThatThrownBy(
            () -> augmentor.augment(identity, Uni.createFrom()::item).await().indefinitely())
        .isInstanceOf(AuthenticationFailedException.class)
        .hasMessage("No token credential available");
  }

  @Test
  public void testAugmentAuthenticationFailure() {
    // Given
    Principal nonPolarisPrincipal = mock(Principal.class);
    QuarkusPrincipalAuthInfo credential = mock(QuarkusPrincipalAuthInfo.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder()
            .setPrincipal(nonPolarisPrincipal)
            .addCredential(credential)
            .build();

    when(authenticator.authenticate(credential)).thenReturn(Optional.empty());

    // When/Then
    assertThatThrownBy(
            () -> augmentor.augment(identity, Uni.createFrom()::item).await().indefinitely())
        .isInstanceOf(AuthenticationFailedException.class)
        .hasCauseInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void testAugmentRuntimeException() {
    // Given
    Principal nonPolarisPrincipal = mock(Principal.class);
    QuarkusPrincipalAuthInfo credential = mock(QuarkusPrincipalAuthInfo.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder()
            .setPrincipal(nonPolarisPrincipal)
            .addCredential(credential)
            .build();

    RuntimeException exception = new NotAuthorizedException("Authentication error");
    when(authenticator.authenticate(credential)).thenThrow(exception);

    // When/Then
    assertThatThrownBy(
            () -> augmentor.augment(identity, Uni.createFrom()::item).await().indefinitely())
        .isInstanceOf(AuthenticationFailedException.class)
        .hasCause(exception);
  }

  @Test
  public void testAugmentSuccessfulAuthentication() {
    // Given
    AuthenticatedPolarisPrincipal polarisPrincipal = mock(AuthenticatedPolarisPrincipal.class);
    when(polarisPrincipal.getName()).thenReturn("user1");
    QuarkusPrincipalAuthInfo credential = mock(QuarkusPrincipalAuthInfo.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder()
            .setPrincipal(polarisPrincipal)
            .addCredential(credential)
            .build();

    when(authenticator.authenticate(credential)).thenReturn(Optional.of(polarisPrincipal));

    // When
    SecurityIdentity result =
        augmentor.augment(identity, Uni.createFrom()::item).await().indefinitely();

    // Then
    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isSameAs(polarisPrincipal);
    assertThat(result.getPrincipal().getName()).isEqualTo("user1");
  }
}
