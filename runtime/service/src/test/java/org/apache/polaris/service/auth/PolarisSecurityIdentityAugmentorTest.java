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
package org.apache.polaris.service.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import java.security.Principal;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributes;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.service.auth.external.OidcIdentityPreparer;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PolarisSecurityIdentityAugmentorTest {

  private PolarisSecurityIdentityAugmentor augmentor;
  private Authenticator authenticator;
  private OidcIdentityPreparer oidcIdentityPreparer;

  @BeforeEach
  public void setup() {
    authenticator = mock(Authenticator.class);
    oidcIdentityPreparer = mock(OidcIdentityPreparer.class);
    // By default, the preparer is a pass-through; individual tests override for OIDC identities
    when(oidcIdentityPreparer.prepare(any())).thenAnswer(i -> i.getArgument(0));
    augmentor = new PolarisSecurityIdentityAugmentor(authenticator, oidcIdentityPreparer);
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
  public void testAugmentAuthenticationFailure() {
    // Given
    Principal nonPolarisPrincipal = mock(Principal.class);
    PolarisCredential credential = mock(PolarisCredential.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder()
            .setPrincipal(nonPolarisPrincipal)
            .addCredential(credential)
            .build();

    AuthenticationFailedException exception =
        new AuthenticationFailedException("Authentication error");
    when(authenticator.authenticate(identity)).thenThrow(exception);

    // When/Then
    assertThatThrownBy(
            () -> augmentor.augment(identity, Uni.createFrom()::item).await().indefinitely())
        .isSameAs(exception);
  }

  @Test
  public void testAugmentSuccessfulAuthentication() {
    // Given
    PolarisPrincipal polarisPrincipal =
        PolarisPrincipal.of(
            "user1",
            ImmutableAttributeMap.builder()
                .put(PolarisPrincipalAttributes.JWT_ATTRIBUTE_KEY, "token")
                .build(),
            Set.of("role1", "role2"));
    PolarisCredential credential = mock(PolarisCredential.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder()
            .setPrincipal(polarisPrincipal)
            .addCredential(credential)
            .addAttribute("attr1", "value1")
            .build();

    when(authenticator.authenticate(identity)).thenReturn(polarisPrincipal);

    // When
    SecurityIdentity result =
        augmentor.augment(identity, Uni.createFrom()::item).await().indefinitely();

    // Then
    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isSameAs(polarisPrincipal);
    // principal attributes should not be merged
    assertThat(result.getAttributes()).containsOnlyKeys("attr1");
  }

  @Test
  public void testAugmentOidcIdentity() {
    // Given
    JsonWebToken oidcPrincipal = mock(JsonWebToken.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder().setPrincipal(oidcPrincipal).build();
    PolarisCredential credential = mock(PolarisCredential.class);
    SecurityIdentity preparedIdentity =
        QuarkusSecurityIdentity.builder()
            .setPrincipal(oidcPrincipal)
            .addCredential(credential)
            .build();
    PolarisPrincipal polarisPrincipal =
        PolarisPrincipal.of("user1", ImmutableAttributeMap.builder().build(), Set.of("role1"));

    when(oidcIdentityPreparer.prepare(identity)).thenReturn(preparedIdentity);
    when(authenticator.authenticate(preparedIdentity)).thenReturn(polarisPrincipal);

    // When
    SecurityIdentity result =
        augmentor.augment(identity, Uni.createFrom()::item).await().indefinitely();

    // Then
    assertThat(result.getPrincipal()).isSameAs(polarisPrincipal);
    assertThat(result.getRoles()).containsExactlyInAnyOrder("role1");
  }
}
