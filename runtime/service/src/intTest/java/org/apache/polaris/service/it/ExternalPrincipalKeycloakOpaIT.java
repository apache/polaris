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

import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.service.it.env.CatalogApi;
import org.apache.polaris.service.it.env.ClientPrincipal;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.it.test.PolarisRestCatalogFileIntegrationTest;
import org.apache.polaris.test.keycloak.Keycloak;
import org.apache.polaris.test.keycloak.KeycloakAccess;
import org.apache.polaris.test.keycloak.KeycloakTestResource;
import org.apache.polaris.test.opa.OpaTestResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * End-to-end test for external principals: a principal authenticated via Keycloak (OIDC) that does
 * NOT exist in the Polaris metastore, authorized by an external OPA authorizer.
 */
@QuarkusIntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestProfile(ExternalPrincipalKeycloakOpaIT.Profile.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
public class ExternalPrincipalKeycloakOpaIT extends PolarisRestCatalogFileIntegrationTest {

  private static final String DENIED_USER = "denied_user";

  private static final String OPA_POLICY =
      """
      package polaris.authz

      default allow := false

      allow if {
        input.actor.principal != "%s"
      }
      """
          .formatted(DENIED_USER);

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.ofEntries(
          Map.entry("quarkus.oidc.tenant-enabled", "true"),
          Map.entry("quarkus.oidc.client-id", "polaris"),
          Map.entry("polaris.authentication.type", "external"),
          Map.entry("polaris.authentication.principal-mode", "external"),
          Map.entry(
              "polaris.oidc.principal-mapper.name-claim-path", KeycloakAccess.PRINCIPAL_NAME_CLAIM),
          Map.entry("polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"", "[\"FILE\"]"),
          Map.entry("polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"", "true"),
          Map.entry("polaris.readiness.ignore-severe-issues", "true"));
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
                  KeycloakTestResource.CLIENTS_ARG, clients.orElse(""))),
          new TestResourceEntry(
              OpaTestResource.class, Map.of(OpaTestResource.REGO_POLICY_ARG, OPA_POLICY)));
    }
  }

  @Keycloak KeycloakAccess keycloak;

  @Override
  protected ClientPrincipal createTestPrincipal(
      PolarisClient client, String principalName, String principalRole) {
    keycloak.createRole(principalRole);
    keycloak.createUser(principalName, "s3cr3t");
    keycloak.assignRoleToUser(principalRole, principalName);
    ClientPrincipal principal = super.createTestPrincipal(client, principalName, principalRole);
    keycloak.createServiceAccount(
        principal.credentials().clientId(), principal.credentials().clientSecret());
    return principal;
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

  @Override
  protected String obtainToken(PolarisClient client, ClientPrincipal principal) {
    return obtainToken(
        principal.principalName(),
        "s3cr3t",
        principal.credentials().clientId(),
        principal.credentials().clientSecret());
  }

  private String obtainToken(
      String username, String password, String clientId, String clientSecret) {
    // Use password grant type to obtain a token that is tied to the principal user,
    // not just to the service account.
    return keycloak.getToken(
        Map.of(
            "grant_type", "password",
            "client_id", clientId,
            "client_secret", clientSecret,
            "username", username,
            "password", password));
  }

  @Test
  void externalPrincipalDeniedByOpaIsForbidden(PolarisApiEndpoints endpoints) throws Exception {
    String password = "s3cr3t2";
    String clientId = "denied_user_client";
    String clientSecret = "S3CR3T2";
    keycloak.createUser(DENIED_USER, password);
    keycloak.createServiceAccount(clientId, clientSecret);
    try (var client = polarisClient(endpoints)) {
      String token = obtainToken(DENIED_USER, password, clientId, clientSecret);
      CatalogApi catalogApi = client.catalogApi(token);
      @SuppressWarnings("resource")
      String catalogName = catalog().properties().get("warehouse");
      try (Response response =
          catalogApi.request("v1/{cat}/namespaces", Map.of("cat", catalogName), Map.of()).get()) {
        assertThat(response.getStatus()).isEqualTo(FORBIDDEN.getStatusCode());
      }
    } finally {
      keycloak.deleteUser("denied_user");
      keycloak.deleteServiceAccount(clientId);
    }
  }
}
