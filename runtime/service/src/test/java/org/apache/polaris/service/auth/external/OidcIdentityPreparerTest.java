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
package org.apache.polaris.service.auth.external;

import static org.apache.polaris.service.auth.external.OidcIdentityPreparer.getOidcTenantConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.inject.Instance;
import java.security.Principal;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration;
import org.apache.polaris.service.auth.CredentialMode;
import org.apache.polaris.service.auth.PolarisCredential;
import org.apache.polaris.service.auth.external.mapping.PrincipalMapper;
import org.apache.polaris.service.auth.external.mapping.PrincipalRolesMapper;
import org.apache.polaris.service.auth.external.tenant.OidcTenantConfiguration;
import org.apache.polaris.service.auth.external.tenant.OidcTenantResolver;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OidcIdentityPreparerTest {

  private OidcIdentityPreparer preparer;
  private OidcTenantResolver resolver;
  private AuthenticationRealmConfiguration authConfig;
  private PrincipalMapper principalMapper;
  private PrincipalRolesMapper principalRolesMapper;
  private OidcTenantConfiguration config;

  @BeforeEach
  public void setup() {
    resolver = mock(OidcTenantResolver.class);
    authConfig = mock(AuthenticationRealmConfiguration.class);
    principalMapper = mock(PrincipalMapper.class);
    principalRolesMapper = mock(PrincipalRolesMapper.class);
    config = mock(OidcTenantConfiguration.class);
    when(config.principalMapper()).thenReturn(mock(OidcTenantConfiguration.PrincipalMapper.class));
    when(config.principalRolesMapper())
        .thenReturn(mock(OidcTenantConfiguration.PrincipalRolesMapper.class));
    when(config.principalMapper().type()).thenReturn("default");
    when(config.principalRolesMapper().type()).thenReturn("default");
    @SuppressWarnings("unchecked")
    Instance<PrincipalMapper> principalMappers = mock(Instance.class);
    when(principalMappers.select(Identifier.Literal.of("default"))).thenReturn(principalMappers);
    when(principalMappers.get()).thenReturn(principalMapper);
    @SuppressWarnings("unchecked")
    Instance<PrincipalRolesMapper> principalRoleMappers = mock(Instance.class);
    when(principalRoleMappers.select(Identifier.Literal.of("default")))
        .thenReturn(principalRoleMappers);
    when(principalRoleMappers.get()).thenReturn(principalRolesMapper);
    preparer =
        new OidcIdentityPreparer(authConfig, resolver, principalMappers, principalRoleMappers);
  }

  @Test
  public void testPrepareNonOidcIdentity() {
    // Given
    Principal nonOidcPrincipal = mock(Principal.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder().setPrincipal(nonOidcPrincipal).build();

    // When
    SecurityIdentity result = preparer.prepare(identity);

    // Then: identity is returned unchanged
    assertThat(result).isSameAs(identity);
  }

  @Test
  public void testPrepareInternalPrincipal() {
    // Given
    when(authConfig.credentialMode()).thenReturn(CredentialMode.INTERNAL);
    JsonWebToken oidcPrincipal = mock(JsonWebToken.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder().setPrincipal(oidcPrincipal).addRole("ROLE1").build();
    when(resolver.resolveConfig(identity)).thenReturn(config);

    // The resolver is called with the original identity; mappers are called with the identity
    // that has the config attribute set. Use argument matchers to capture the prepared identity.
    when(principalMapper.mapPrincipalId(
            org.mockito.ArgumentMatchers.argThat(id -> config.equals(getOidcTenantConfig(id)))))
        .thenReturn(OptionalLong.of(123L));
    when(principalMapper.mapPrincipalName(
            org.mockito.ArgumentMatchers.argThat(id -> config.equals(getOidcTenantConfig(id)))))
        .thenReturn(Optional.of("root"));
    when(principalRolesMapper.mapPrincipalRoles(
            org.mockito.ArgumentMatchers.argThat(id -> config.equals(getOidcTenantConfig(id)))))
        .thenReturn(Set.of("MAPPED_ROLE1"));

    // When
    SecurityIdentity result = preparer.prepare(identity);

    // Then
    assertThat(result.getPrincipal()).isSameAs(oidcPrincipal);
    assertThat(getOidcTenantConfig(result)).isSameAs(config);
    assertThat(result.getCredential(PolarisCredential.class))
        .isEqualTo(PolarisCredential.of(123L, "root", Set.of("MAPPED_ROLE1")));
    // roles are not changed here; that is done by PolarisSecurityIdentityAugmentor
    assertThat(result.getRoles()).containsExactlyInAnyOrder("ROLE1");
  }

  @Test
  public void testPrepareExternalPrincipal() {
    // Given
    when(authConfig.credentialMode()).thenReturn(CredentialMode.EXTERNAL);
    JsonWebToken oidcPrincipal = mock(JsonWebToken.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder().setPrincipal(oidcPrincipal).addRole("ROLE1").build();
    when(resolver.resolveConfig(identity)).thenReturn(config);
    when(principalMapper.mapPrincipalName(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Optional.of("alice"));
    when(principalRolesMapper.mapPrincipalRoles(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Set.of("MAPPED_ROLE1"));

    // When
    SecurityIdentity result = preparer.prepare(identity);

    // Then
    assertThat(result.getPrincipal()).isSameAs(oidcPrincipal);
    assertThat(result.getCredential(PolarisCredential.class))
        .isEqualTo(PolarisCredential.ofExternal("alice", Set.of("MAPPED_ROLE1")));
    assertThat(result.getRoles()).containsExactlyInAnyOrder("ROLE1");
  }

  @Test
  public void testPrepareExternalPrincipalWithoutName() {
    // Given: external mode but the principal mapper cannot resolve a name
    when(authConfig.credentialMode()).thenReturn(CredentialMode.EXTERNAL);
    JsonWebToken oidcPrincipal = mock(JsonWebToken.class);
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder().setPrincipal(oidcPrincipal).build();
    when(resolver.resolveConfig(identity)).thenReturn(config);
    when(principalMapper.mapPrincipalName(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Optional.empty());
    when(principalRolesMapper.mapPrincipalRoles(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Set.of("MAPPED_ROLE1"));

    // When: the preparer builds a credential with a null name; DefaultAuthenticator rejects it
    // later
    SecurityIdentity result = preparer.prepare(identity);

    // Then
    assertThat(result.getCredential(PolarisCredential.class))
        .isEqualTo(PolarisCredential.ofExternal(null, Set.of("MAPPED_ROLE1")));
  }
}
