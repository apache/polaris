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
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.service.it.env.ClientPrincipal;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.test.PolarisRestCatalogFileIntegrationTest;
import org.apache.polaris.test.keycloak.Keycloak;
import org.apache.polaris.test.keycloak.KeycloakAccess;
import org.apache.polaris.test.keycloak.KeycloakTestResource;

@QuarkusIntegrationTest
@TestProfile(RestCatalogKeycloakFileIT.Profile.class)
public class RestCatalogKeycloakFileIT extends PolarisRestCatalogFileIntegrationTest {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "quarkus.oidc.tenant-enabled", "true",
          "quarkus.oidc.client-id", "polaris",
          "polaris.authentication.type", "external",
          "polaris.oidc.principal-mapper.name-claim-path", KeycloakAccess.PRINCIPAL_NAME_CLAIM,
          "polaris.oidc.principal-roles-mapper.filter", "PRINCIPAL_ROLE:.*");
    }

    @Override
    public List<TestResourceEntry> testResources() {
      Optional<String> clients =
          RootCredentialsSet.fromEnvironment().credentials().values().stream()
              .map(creds -> creds.clientId() + "=" + creds.clientSecret())
              .reduce((a, b) -> a + "," + b);
      return List.of(
          new TestResourceEntry(
              KeycloakTestResource.class,
              Map.of(
                  KeycloakTestResource.ROLES_ARG, "PRINCIPAL_ROLE:ALL",
                  KeycloakTestResource.USERS_ARG, "root=s3cr3t",
                  KeycloakTestResource.GRANTS_ARG, "root=PRINCIPAL_ROLE:ALL",
                  KeycloakTestResource.CLIENTS_ARG, clients.orElse(""))));
    }
  }

  @Keycloak KeycloakAccess keycloak;

  @Override
  protected ClientPrincipal createTestPrincipal(
      PolarisClient client, String principalName, String principalRole) {
    ClientPrincipal principal = super.createTestPrincipal(client, principalName, principalRole);
    keycloak.createRole("PRINCIPAL_ROLE:" + principalRole);
    keycloak.createUser(principalName, "s3cr3t");
    keycloak.assignRoleToUser("PRINCIPAL_ROLE:" + principalRole, principalName);
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
            "password", "s3cr3t");
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
        .forEach(role -> keycloak.deleteRole("PRINCIPAL_ROLE:" + role.getName()));
    super.cleanUp(client, adminToken);
  }
}
