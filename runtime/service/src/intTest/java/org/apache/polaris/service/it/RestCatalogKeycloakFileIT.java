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
package org.apache.polaris.service.it;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import org.apache.polaris.service.it.env.ClientPrincipal;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.test.PolarisRestCatalogFileIntegrationTest;
import org.apache.polaris.test.commons.keycloak.KeycloakAccess;
import org.apache.polaris.test.commons.keycloak.KeycloakProfile;

@QuarkusIntegrationTest
@TestProfile(KeycloakProfile.class)
public class RestCatalogKeycloakFileIT extends PolarisRestCatalogFileIntegrationTest {

  KeycloakAccess keycloak;

  @Override
  protected ClientPrincipal createTestPrincipal(
      PolarisClient client, String principalName, String principalRole) {
    ClientPrincipal principal = super.createTestPrincipal(client, principalName, principalRole);
    keycloak.createRole(principalRole);
    keycloak.createUser(principalName);
    keycloak.assignRoleToUser(principalRole, principalName);
    keycloak.createServiceAccount(
        principal.credentials().clientId(), principal.credentials().clientSecret());
    return principal;
  }

  @Override
  protected String obtainToken(PolarisClient client, ClientPrincipal principal) {
    // Use password grant type to obtain a token that is tied to the principal user,
    // not just to the service account.
    Map<String, String> request =
        Map.of(
            "grant_type", "password",
            "client_id", principal.credentials().clientId(),
            "client_secret", principal.credentials().clientSecret(),
            "username", principal.principalName(),
            "password", KeycloakAccess.USER_PASSWORD);
    return client.obtainToken(keycloak.getIssuerUrl(), keycloak.getTokenPath(), request);
  }

  @Override
  protected void cleanUp(PolarisClient client, String adminToken) {
    ManagementApi managementApi = client.managementApi(adminToken);
    managementApi.listPrincipals().stream()
        .filter(p -> client.ownedName(p.getName()))
        .forEach(
            p -> {
              keycloak.deleteUser(p.getName());
              keycloak.deleteServiceAccount(p.getClientId());
            });
    managementApi.listPrincipalRoles().stream()
        .filter(r -> client.ownedName(r.getName()))
        .forEach(role -> keycloak.deleteRole(role.getName()));
    super.cleanUp(client, adminToken);
  }
}
