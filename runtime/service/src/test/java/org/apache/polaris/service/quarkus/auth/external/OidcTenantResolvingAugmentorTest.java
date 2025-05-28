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

import static org.apache.polaris.service.quarkus.auth.external.OidcTenantResolvingAugmentor.getOidcTenantConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import java.security.Principal;
import org.apache.polaris.service.quarkus.auth.external.tenant.OidcTenantResolver;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OidcTenantResolvingAugmentorTest {

  private OidcTenantResolvingAugmentor augmentor;
  private OidcTenantResolver resolver;

  @BeforeEach
  public void setup() {
    resolver = mock(OidcTenantResolver.class);
    augmentor = new OidcTenantResolvingAugmentor(resolver);
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
            .addRole("PRINCIPAL_ROLE:ALL")
            .build();

    OidcTenantConfiguration config = mock(OidcTenantConfiguration.class);
    when(resolver.resolveConfig(identity)).thenReturn(config);

    // When
    SecurityIdentity result =
        augmentor.augment(identity, Uni.createFrom()::item).await().indefinitely();

    // Then
    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isSameAs(oidcPrincipal);
    assertThat(result.getRoles()).containsExactlyInAnyOrder("PRINCIPAL_ROLE:ALL");
    assertThat(getOidcTenantConfig(result)).isSameAs(config);
  }
}
