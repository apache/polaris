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

import static org.apache.polaris.service.auth.AuthenticationConfiguration.DEFAULT_REALM_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import org.apache.polaris.service.auth.AuthenticationType;
import org.apache.polaris.service.quarkus.auth.external.OidcConfiguration;
import org.apache.polaris.service.quarkus.auth.external.OidcTenantConfiguration;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(AuthenticationConfigurationTest.Profile.class)
public class AuthenticationConfigurationTest {

  @Inject QuarkusAuthenticationConfiguration authConfig;
  @Inject OidcConfiguration oidcConfig;

  @Test
  void smokeTest() {
    Map<String, QuarkusAuthenticationRealmConfiguration> realms = authConfig.realms();
    assertThat(realms).hasSize(3);

    assertThat(realms.get(DEFAULT_REALM_KEY).type()).isEqualTo(AuthenticationType.MIXED);
    assertThat(realms.get(DEFAULT_REALM_KEY).authenticator().type()).isEqualTo("custom");
    assertThat(realms.get(DEFAULT_REALM_KEY).activeRolesProvider().type()).isEqualTo("custom");
    assertThat(realms.get(DEFAULT_REALM_KEY).tokenBroker().type()).isEqualTo("custom");
    assertThat(realms.get(DEFAULT_REALM_KEY).tokenService().type()).isEqualTo("custom");

    assertThat(realms.get("realm1").type()).isEqualTo(AuthenticationType.INTERNAL);
    assertThat(realms.get("realm1").authenticator().type()).isEqualTo("default");
    assertThat(realms.get("realm1").activeRolesProvider().type()).isEqualTo("default");
    assertThat(realms.get("realm1").tokenBroker().type()).isEqualTo("default");
    assertThat(realms.get("realm1").tokenService().type()).isEqualTo("default");

    assertThat(realms.get("realm2").type()).isEqualTo(AuthenticationType.EXTERNAL);
    assertThat(realms.get("realm2").authenticator().type()).isEqualTo("default");
    assertThat(realms.get("realm2").activeRolesProvider().type()).isEqualTo("default");
    assertThat(realms.get("realm2").tokenBroker().type()).isEqualTo("rsa-key-pair");
    assertThat(realms.get("realm2").tokenService().type()).isEqualTo("default");

    Map<String, OidcTenantConfiguration> tenants = oidcConfig.tenants();
    assertThat(tenants).hasSize(2);

    assertThat(tenants.get(DEFAULT_REALM_KEY).principalMapper().type()).isEqualTo("custom");
    assertThat(tenants.get(DEFAULT_REALM_KEY).principalRolesMapper().type()).isEqualTo("custom");

    assertThat(tenants.get("tenant1").principalMapper().type()).isEqualTo("default");
    assertThat(tenants.get("tenant1").principalRolesMapper().type()).isEqualTo("default");
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()

          // Polaris Auth config

          // Default realm: mixed auth with custom impls
          .put("polaris.authentication.type", "mixed")
          .put("polaris.authentication.authenticator.type", "custom")
          .put("polaris.authentication.active-roles-provider.type", "custom")
          .put("polaris.authentication.token-broker.type", "custom")
          .put("polaris.authentication.token-service.type", "custom")
          // realm1: internal auth with default impls
          .put("polaris.authentication.realm1.type", "internal")
          .put("polaris.authentication.realm1.authenticator.type", "default")
          .put("polaris.authentication.realm1.active-roles-provider.type", "default")
          .put("polaris.authentication.realm1.token-broker.type", "default")
          .put("polaris.authentication.realm1.token-service.type", "default")
          // realm2: external auth
          .put("polaris.authentication.realm2.type", "external")

          // Polaris OIDC config

          // Default tenant: custom principal mapper and roles mapper
          .put("polaris.oidc.principal-mapper.type", "custom")
          .put("polaris.oidc.principal-roles-mapper.type", "custom")
          // tenant1: default principal mapper and roles mapper
          .put("polaris.oidc.tenant1.principal-mapper.type", "default")
          .put("polaris.oidc.tenant1.principal-roles-mapper.type", "default")
          .build();
    }
  }
}
