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
import java.util.Set;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.service.auth.ActiveRolesProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ActiveRolesAugmentorTest {

  private ActiveRolesAugmentor augmentor;
  private ActiveRolesProvider activeRolesProvider;

  @BeforeEach
  public void setup() {
    activeRolesProvider = mock(ActiveRolesProvider.class);
    augmentor = new ActiveRolesAugmentor(activeRolesProvider);
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
  public void testAugmentNonPolarisPrincipal() {
    // Given
    Principal nonPolarisPrincipal = mock(Principal.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder().setPrincipal(nonPolarisPrincipal).build();

    // When/Then
    assertThatThrownBy(
            () -> augmentor.augment(identity, Uni.createFrom()::item).await().indefinitely())
        .isInstanceOf(AuthenticationFailedException.class)
        .hasMessage("No Polaris principal found");
  }

  @ParameterizedTest
  @ValueSource(strings = {"role1", "role1,role2", "role1,role2,role3"})
  public void testAugmentWithValidRoles(String rolesString) {
    // Given
    Set<String> roles = Set.of(rolesString.split(","));
    AuthenticatedPolarisPrincipal polarisPrincipal = mock(AuthenticatedPolarisPrincipal.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder().setPrincipal(polarisPrincipal).build();

    when(activeRolesProvider.getActiveRoles(polarisPrincipal)).thenReturn(roles);

    // When
    SecurityIdentity result =
        augmentor.augment(identity, Uni.createFrom()::item).await().indefinitely();

    // Then
    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isSameAs(polarisPrincipal);
    assertThat(result.getRoles()).containsExactlyInAnyOrderElementsOf(roles);
  }

  @Test
  public void testAugmentWithEmptyRoles() {
    // Given
    Set<String> roles = Set.of();
    AuthenticatedPolarisPrincipal polarisPrincipal = mock(AuthenticatedPolarisPrincipal.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder().setPrincipal(polarisPrincipal).build();

    when(activeRolesProvider.getActiveRoles(polarisPrincipal)).thenReturn(roles);

    // When
    SecurityIdentity result =
        augmentor.augment(identity, Uni.createFrom()::item).await().indefinitely();

    // Then
    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isSameAs(polarisPrincipal);
    assertThat(result.getRoles()).isEmpty();
  }
}
