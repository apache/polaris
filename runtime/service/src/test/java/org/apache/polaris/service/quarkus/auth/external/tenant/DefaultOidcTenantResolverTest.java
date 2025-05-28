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
package org.apache.polaris.service.quarkus.auth.external.tenant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import io.quarkus.oidc.runtime.OidcUtils;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.service.quarkus.auth.external.OidcConfiguration;
import org.apache.polaris.service.quarkus.auth.external.OidcTenantConfiguration;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultOidcTenantResolverTest {

  private DefaultOidcTenantResolver resolver;
  private OidcConfiguration polarisOidcConfig;

  @BeforeEach
  public void setup() {
    polarisOidcConfig = mock(OidcConfiguration.class);
    resolver = new DefaultOidcTenantResolver(polarisOidcConfig);
  }

  @Test
  public void testResolveConfigWithMatchingTenant() {
    // Given
    String tenantId = "tenant1";
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder()
            .setPrincipal(mock(JsonWebToken.class))
            .addAttribute(OidcUtils.TENANT_ID_ATTRIBUTE, tenantId)
            .build();

    Map<String, OidcTenantConfiguration> polarisTenants = new HashMap<>();
    OidcTenantConfiguration polarisTenantConfig = mock(OidcTenantConfiguration.class);
    polarisTenants.put(tenantId, polarisTenantConfig);

    doReturn(polarisTenants).when(polarisOidcConfig).tenants();

    // When
    OidcTenantConfiguration result = resolver.resolveConfig(identity);

    // Then
    assertThat(result).isSameAs(polarisTenantConfig);
  }

  @Test
  public void testResolveConfigWithoutMatchingTenant() {
    // Given
    String tenantId = "tenant1";
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder()
            .setPrincipal(mock(JsonWebToken.class))
            .addAttribute(OidcUtils.TENANT_ID_ATTRIBUTE, tenantId)
            .build();

    Map<String, OidcTenantConfiguration> polarisTenants = new HashMap<>();
    OidcTenantConfiguration polarisTenantConfig = mock(OidcTenantConfiguration.class);
    polarisTenants.put(OidcConfiguration.DEFAULT_TENANT_KEY, polarisTenantConfig);

    doReturn(polarisTenants).when(polarisOidcConfig).tenants();

    // When
    OidcTenantConfiguration result = resolver.resolveConfig(identity);

    // Then
    assertThat(result).isSameAs(polarisTenantConfig);
  }

  @Test
  public void testResolveConfigWithDefaultTenant() {
    // Given
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder()
            .setPrincipal(mock(JsonWebToken.class))
            .addAttribute(OidcUtils.TENANT_ID_ATTRIBUTE, OidcUtils.DEFAULT_TENANT_ID)
            .build();

    Map<String, OidcTenantConfiguration> polarisTenants = new HashMap<>();
    OidcTenantConfiguration polarisTenantConfig = mock(OidcTenantConfiguration.class);
    polarisTenants.put(OidcConfiguration.DEFAULT_TENANT_KEY, polarisTenantConfig);

    doReturn(polarisTenants).when(polarisOidcConfig).tenants();

    // When
    OidcTenantConfiguration result = resolver.resolveConfig(identity);

    // Then
    assertThat(result).isSameAs(polarisTenantConfig);
  }

  @Test
  public void testResolveConfigWithoutTenant() {
    // Given
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder().setPrincipal(mock(JsonWebToken.class)).build();

    Map<String, OidcTenantConfiguration> polarisTenants = new HashMap<>();
    OidcTenantConfiguration polarisTenantConfig = mock(OidcTenantConfiguration.class);
    polarisTenants.put(OidcConfiguration.DEFAULT_TENANT_KEY, polarisTenantConfig);

    doReturn(polarisTenants).when(polarisOidcConfig).tenants();

    // When
    OidcTenantConfiguration result = resolver.resolveConfig(identity);

    // Then
    assertThat(result).isSameAs(polarisTenantConfig);
  }
}
