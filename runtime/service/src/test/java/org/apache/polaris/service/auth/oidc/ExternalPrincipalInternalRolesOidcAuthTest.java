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
package org.apache.polaris.service.auth.oidc;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.oidc.server.OidcWiremockTestResource;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.auth.DefaultAuthenticator;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * OIDC tests for <em>external</em> principals with <em>internal</em> roles: an OIDC-authenticated
 * caller that has no backing entity in the Polaris metastore, but whose roles name principal roles
 * managed in Polaris ({@code credential-mode=external-with-internal-roles}), authorized by the
 * built-in {@code internal} authorizer via the grants made to those principal roles.
 */
@QuarkusTest
@TestProfile(ExternalPrincipalInternalRolesOidcAuthTest.Profile.class)
public class ExternalPrincipalInternalRolesOidcAuthTest extends AbstractOidcAuthTest {

  private static final String SERVICE_ADMIN_ROLE =
      DefaultAuthenticator.PRINCIPAL_ROLE_PREFIX + "service_admin";
  private static final String PRINCIPAL_ROLE = "analyst";
  private static final String CATALOG = "analytics";
  private static final String CATALOG_ROLE = "namespace_lister";
  private static final String INTERNAL_PRINCIPAL = "internal-user";

  public static class Profile extends AbstractOidcAuthTest.Profile {

    @Override
    public Map<String, String> getConfigOverrides() {
      ImmutableMap.Builder<String, String> config = ImmutableMap.builder();
      config.putAll(super.getConfigOverrides());
      config.put("polaris.authentication.credential-mode", "external-with-internal-roles");
      config.put("polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"", "true");
      config.put("polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"", "[\"FILE\"]");
      return config.build();
    }
  }

  private PolarisApiEndpoints endpoints;
  private PolarisClient client;

  @BeforeAll
  void createRolesAndGrants(PolarisApiEndpoints endpoints) {
    this.endpoints = endpoints;
    client = PolarisClient.polarisClient(endpoints);
    // No principal is created for the external callers: only principal roles, and the grants made
    // to them, are managed in Polaris.
    ManagementApi managementApi = managementApiAsAdmin();
    managementApi.createPrincipalRole(PRINCIPAL_ROLE);
    managementApi.createCatalog(
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(CATALOG)
            .setProperties(CatalogProperties.builder("file:///tmp/" + CATALOG).build())
            .setStorageConfigInfo(
                FileStorageConfigInfo.builder()
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
                    .setAllowedLocations(List.of("file:///tmp/" + CATALOG))
                    .build())
            .build());
    managementApi.createCatalogRole(CATALOG, CATALOG_ROLE);
    managementApi.addGrant(
        CATALOG,
        CATALOG_ROLE,
        new CatalogGrant(CatalogPrivilege.NAMESPACE_LIST, GrantResource.TypeEnum.CATALOG));
    managementApi.grantCatalogRoleToPrincipalRole(
        PRINCIPAL_ROLE, CATALOG, new CatalogRole(CATALOG_ROLE));
    managementApi.createPrincipal(INTERNAL_PRINCIPAL);
  }

  @AfterAll
  void deleteRolesAndGrants() throws Exception {
    if (client != null) {
      ManagementApi managementApi = managementApiAsAdmin();
      managementApi.deletePrincipal(INTERNAL_PRINCIPAL);
      managementApi.dropCatalog(CATALOG);
      managementApi.deletePrincipalRole(PRINCIPAL_ROLE);
      client.close();
    }
  }

  private ManagementApi managementApiAsAdmin() {
    String adminToken =
        OidcWiremockTestResource.getAccessToken("admin", Set.of(SERVICE_ADMIN_ROLE));
    return client.managementApi(adminToken);
  }

  private String listNamespaces(PolarisApiEndpoints endpoints) {
    return endpoints.catalogApiEndpoint() + "/v1/" + CATALOG + "/namespaces";
  }

  @Test
  void allowedExternalPrincipal() {
    // The service_admin principal role grants access to the management API
    asUser(endpoints, "alice", Set.of(SERVICE_ADMIN_ROLE))
        .get(listCatalogs(endpoints))
        .then()
        .statusCode(200);
    // The analyst principal role grants access to the catalog, via its catalog role
    asUser(endpoints, "bob", Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_PREFIX + PRINCIPAL_ROLE))
        .get(listNamespaces(endpoints))
        .then()
        .statusCode(200);
  }

  @Test
  void unauthenticatedExternalPrincipal() {
    // Invalid OIDC token (not signed by the WireMock OIDC server)
    asUser(endpoints, "bad-token").get(listCatalogs(endpoints)).then().statusCode(401);
    // The pseudo-role ALL cannot be expanded, since external principals have no grants
    asUser(endpoints, "alice", Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_ALL))
        .get(listCatalogs(endpoints))
        .then()
        .statusCode(401);
  }

  @Test
  void unauthorizedExternalPrincipal() {
    // Roles that do not exist in Polaris are ignored
    asUser(endpoints, "alice", Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_PREFIX + "unknown"))
        .get(listCatalogs(endpoints))
        .then()
        .statusCode(403);
    // The analyst principal role has no grants on the management API
    asUser(endpoints, "bob", Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_PREFIX + PRINCIPAL_ROLE))
        .get(listCatalogs(endpoints))
        .then()
        .statusCode(403);
    // An external principal named "root" is not the root principal
    asUser(endpoints, "root", Set.of(SERVICE_ADMIN_ROLE))
        .contentType("application/json")
        .body("{}")
        .post(endpoints.managementApiEndpoint() + "/v1/principals/" + INTERNAL_PRINCIPAL + "/reset")
        .then()
        .statusCode(403);
  }
}
