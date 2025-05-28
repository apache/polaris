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
package org.apache.polaris.service.quarkus.auth.external;

import static org.apache.polaris.service.quarkus.auth.external.OidcTenantResolvingAugmentor.TENANT_CONFIG_ATTRIBUTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Instance;
import java.security.Principal;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.polaris.service.quarkus.auth.external.OidcTenantConfiguration.PrincipalMapper;
import org.apache.polaris.service.quarkus.auth.external.OidcTenantConfiguration.PrincipalRolesMapper;
import org.apache.polaris.service.quarkus.auth.external.PrincipalAuthInfoAugmentor.OidcPrincipalAuthInfo;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PrincipalAuthInfoAugmentorTest {

  private PrincipalAuthInfoAugmentor augmentor;
  private org.apache.polaris.service.quarkus.auth.external.mapping.PrincipalMapper principalMapper;
  private org.apache.polaris.service.quarkus.auth.external.mapping.PrincipalRolesMapper
      principalRolesMapper;
  private OidcTenantConfiguration config;

  @BeforeEach
  public void setup() {
    principalMapper =
        mock(org.apache.polaris.service.quarkus.auth.external.mapping.PrincipalMapper.class);
    principalRolesMapper =
        mock(org.apache.polaris.service.quarkus.auth.external.mapping.PrincipalRolesMapper.class);
    config = mock(OidcTenantConfiguration.class);
    when(config.principalMapper()).thenReturn(mock(PrincipalMapper.class));
    when(config.principalRolesMapper()).thenReturn(mock(PrincipalRolesMapper.class));
    when(config.principalMapper().type()).thenReturn("default");
    when(config.principalRolesMapper().type()).thenReturn("default");
    @SuppressWarnings("unchecked")
    Instance<org.apache.polaris.service.quarkus.auth.external.mapping.PrincipalMapper>
        principalMappers = mock(Instance.class);
    when(principalMappers.select(Identifier.Literal.of("default"))).thenReturn(principalMappers);
    when(principalMappers.get()).thenReturn(principalMapper);
    @SuppressWarnings("unchecked")
    Instance<org.apache.polaris.service.quarkus.auth.external.mapping.PrincipalRolesMapper>
        principalRoleMappers = mock(Instance.class);
    when(principalRoleMappers.select(Identifier.Literal.of("default")))
        .thenReturn(principalRoleMappers);
    when(principalRoleMappers.get()).thenReturn(principalRolesMapper);
    augmentor = new PrincipalAuthInfoAugmentor(principalMappers, principalRoleMappers);
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
  public void testAugmentNonOidcPrincipal() {
    // Given
    Principal nonOidcPrincipal = mock(Principal.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder().setPrincipal(nonOidcPrincipal).build();

    // When
    Uni<SecurityIdentity> result = augmentor.augment(identity, null);

    // Then
    assertThat(result.await().indefinitely()).isSameAs(identity);
  }

  @Test
  public void testAugmentOidcPrincipal() {
    // Given
    JsonWebToken oidcPrincipal = mock(JsonWebToken.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder()
            .setPrincipal(oidcPrincipal)
            .addRole("ROLE1")
            .addAttribute(TENANT_CONFIG_ATTRIBUTE, config)
            .build();

    when(principalMapper.mapPrincipalId(identity)).thenReturn(OptionalLong.of(123L));
    when(principalMapper.mapPrincipalName(identity)).thenReturn(Optional.of("root"));
    when(principalRolesMapper.mapPrincipalRoles(identity)).thenReturn(Set.of("MAPPED_ROLE1"));

    // When
    SecurityIdentity result =
        augmentor.augment(identity, Uni.createFrom()::item).await().indefinitely();

    // Then
    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isSameAs(oidcPrincipal);
    assertThat(result.getCredential(OidcPrincipalAuthInfo.class))
        .isEqualTo(new OidcPrincipalAuthInfo(123L, "root", Set.of("MAPPED_ROLE1")));
    // the identity roles should not change, since this is done by the ActiveRolesAugmentor
    assertThat(result.getRoles()).containsExactlyInAnyOrder("ROLE1");
  }
}
