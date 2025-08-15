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
package org.apache.polaris.service.it.env;

import static jakarta.ws.rs.core.Response.Status.CREATED;
import static jakarta.ws.rs.core.Response.Status.NO_CONTENT;
import static jakarta.ws.rs.core.Response.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.CatalogRoles;
import org.apache.polaris.core.admin.model.Catalogs;
import org.apache.polaris.core.admin.model.CreatePrincipalRequest;
import org.apache.polaris.core.admin.model.GrantCatalogRoleRequest;
import org.apache.polaris.core.admin.model.GrantPrincipalRoleRequest;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.GrantResources;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.PrincipalRoles;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.Principals;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.entity.PolarisEntityConstants;

/**
 * A simple, non-exhaustive set of helper methods for accessing the Polaris Management API.
 *
 * @see PolarisClient#managementApi(String)
 */
public class ManagementApi extends PolarisRestApi {
  public ManagementApi(Client client, PolarisApiEndpoints endpoints, String authToken, URI uri) {
    super(client, endpoints, authToken, uri);
  }

  public PrincipalWithCredentials createPrincipalWithRole(String principalName, String roleName) {
    PrincipalWithCredentials credentials = createPrincipal(principalName);
    createPrincipalRole(roleName);
    assignPrincipalRole(principalName, roleName);
    return credentials;
  }

  public PrincipalWithCredentials createPrincipal(String name) {
    try (Response createPResponse =
        request("v1/principals").post(Entity.json(new Principal(name)))) {
      assertThat(createPResponse).returns(CREATED.getStatusCode(), Response::getStatus);
      return createPResponse.readEntity(PrincipalWithCredentials.class);
    }
  }

  public PrincipalWithCredentials createPrincipal(CreatePrincipalRequest request) {
    try (Response createPResponse = request("v1/principals").post(Entity.json(request))) {
      assertThat(createPResponse).returns(CREATED.getStatusCode(), Response::getStatus);
      return createPResponse.readEntity(PrincipalWithCredentials.class);
    }
  }

  public void createPrincipalRole(String name) {
    createPrincipalRole(new PrincipalRole(name));
  }

  public void createPrincipalRole(PrincipalRole role) {
    try (Response createPrResponse = request("v1/principal-roles").post(Entity.json(role))) {
      assertThat(createPrResponse).returns(CREATED.getStatusCode(), Response::getStatus);
    }
  }

  public void assignPrincipalRole(String principalName, String roleName) {
    try (Response assignPrResponse =
        request("v1/principals/{prince}/principal-roles", Map.of("prince", principalName))
            .put(Entity.json(new GrantPrincipalRoleRequest(new PrincipalRole(roleName))))) {
      assertThat(assignPrResponse).returns(CREATED.getStatusCode(), Response::getStatus);
    }
  }

  public void createCatalogRole(String catalogName, String catalogRoleName) {
    try (Response response =
        request("v1/catalogs/{cat}/catalog-roles", Map.of("cat", catalogName))
            .post(Entity.json(new CatalogRole(catalogRoleName)))) {
      assertThat(response.getStatus()).isEqualTo(CREATED.getStatusCode());
    }
  }

  public void addGrant(String catalogName, String catalogRoleName, GrantResource grant) {
    try (Response response =
        request(
                "v1/catalogs/{cat}/catalog-roles/{role}/grants",
                Map.of("cat", catalogName, "role", catalogRoleName))
            .put(Entity.json(grant))) {
      assertThat(response).returns(CREATED.getStatusCode(), Response::getStatus);
    }
  }

  public void revokeGrant(String catalogName, String catalogRoleName, GrantResource grant) {
    try (Response response =
        request(
                "v1/catalogs/{cat}/catalog-roles/{role}/grants",
                Map.of("cat", catalogName, "role", catalogRoleName))
            .post(Entity.json(grant))) {
      assertThat(response).returns(CREATED.getStatusCode(), Response::getStatus);
    }
  }

  public void grantCatalogRoleToPrincipalRole(
      String principalRoleName, String catalogName, CatalogRole catalogRole) {
    try (Response response =
        request(
                "v1/principal-roles/{role}/catalog-roles/{cat}",
                Map.of("role", principalRoleName, "cat", catalogName))
            .put(Entity.json(new GrantCatalogRoleRequest(catalogRole)))) {
      assertThat(response).returns(CREATED.getStatusCode(), Response::getStatus);
    }
  }

  public GrantResources listGrants(String catalogName, String catalogRoleName) {
    try (Response response =
        request(
                "v1/catalogs/{cat}/catalog-roles/{role}/grants",
                Map.of("cat", catalogName, "role", catalogRoleName))
            .get()) {
      assertThat(response).returns(OK.getStatusCode(), Response::getStatus);
      return response.readEntity(GrantResources.class);
    }
  }

  public void createCatalog(String principalRoleName, Catalog catalog) {
    createCatalog(catalog);

    // Create a new CatalogRole that has CATALOG_MANAGE_CONTENT and CATALOG_MANAGE_ACCESS
    String catalogRoleName = "custom-admin";
    createCatalogRole(catalog.getName(), catalogRoleName);

    CatalogGrant grantResource =
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG);
    Map<String, String> catalogVars = Map.of("cat", catalog.getName(), "role", catalogRoleName);
    try (Response response =
        request("v1/catalogs/{cat}/catalog-roles/{role}/grants", catalogVars)
            .put(Entity.json(grantResource))) {
      assertThat(response.getStatus()).isEqualTo(CREATED.getStatusCode());
    }

    CatalogGrant grantAccessResource =
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_ACCESS, GrantResource.TypeEnum.CATALOG);
    try (Response response =
        request("v1/catalogs/{cat}/catalog-roles/{role}/grants", catalogVars)
            .put(Entity.json(grantAccessResource))) {
      assertThat(response.getStatus()).isEqualTo(CREATED.getStatusCode());
    }

    // Assign this new CatalogRole to the service_admin PrincipalRole
    try (Response response =
        request(
                "v1/principal-roles/{role}/catalog-roles/{cat}",
                Map.of("role", principalRoleName, "cat", catalog.getName()))
            .put(Entity.json(new CatalogRole(catalogRoleName)))) {
      assertThat(response.getStatus()).isEqualTo(CREATED.getStatusCode());
    }
  }

  public void createCatalog(Catalog catalog) {
    try (Response response = request("v1/catalogs").post(Entity.json(catalog))) {
      assertThat(response.getStatus()).isEqualTo(CREATED.getStatusCode());
    }
  }

  public Catalog getCatalog(String name) {
    try (Response response = request("v1/catalogs/{name}", Map.of("name", name)).get()) {
      assertThat(response.getStatus()).isEqualTo(OK.getStatusCode());
      return response.readEntity(Catalog.class);
    }
  }

  public void updateCatalog(Catalog catalog, Map<String, String> catalogProps) {
    try (Response response =
        request("v1/catalogs/{name}", Map.of("name", catalog.getName()))
            .put(
                Entity.json(
                    new UpdateCatalogRequest(
                        catalog.getEntityVersion(),
                        catalogProps,
                        catalog.getStorageConfigInfo())))) {
      assertThat(response.getStatus()).isEqualTo(OK.getStatusCode());
    }
  }

  public void deleteCatalog(String catalogName) {
    try (Response response = request("v1/catalogs/{cat}", Map.of("cat", catalogName)).delete()) {
      assertThat(response.getStatus()).isEqualTo(NO_CONTENT.getStatusCode());
    }
  }

  public CatalogRole getCatalogRole(String catalogName, String roleName) {
    try (Response response =
        request(
                "v1/catalogs/{cat}/catalog-roles/{role}",
                Map.of("cat", catalogName, "role", roleName))
            .get()) {
      assertThat(response.getStatus()).isEqualTo(OK.getStatusCode());
      return response.readEntity(CatalogRole.class);
    }
  }

  public List<CatalogRole> listCatalogRoles(String catalogName) {
    try (Response response =
        request("v1/catalogs/{cat}/catalog-roles", Map.of("cat", catalogName)).get()) {
      assertThat(response.getStatus()).isEqualTo(OK.getStatusCode());
      return response.readEntity(CatalogRoles.class).getRoles();
    }
  }

  public List<Principal> listPrincipals() {
    try (Response response = request("v1/principals").get()) {
      assertThat(response.getStatus()).isEqualTo(OK.getStatusCode());
      return response.readEntity(Principals.class).getPrincipals();
    }
  }

  public List<PrincipalRole> listPrincipalRoles() {
    try (Response response = request("v1/principal-roles").get()) {
      assertThat(response.getStatus()).isEqualTo(OK.getStatusCode());
      return response.readEntity(PrincipalRoles.class).getRoles();
    }
  }

  public List<Catalog> listCatalogs() {
    try (Response response = request("v1/catalogs").get()) {
      assertThat(response.getStatus()).isEqualTo(OK.getStatusCode());
      return response.readEntity(Catalogs.class).getCatalogs();
    }
  }

  public void deleteCatalogRole(String catalogName, CatalogRole role) {
    deleteCatalogRole(catalogName, role.getName());
  }

  public void deleteCatalogRole(String catalogName, String roleName) {
    try (Response response =
        request(
                "v1/catalogs/{cat}/catalog-roles/{role}",
                Map.of("cat", catalogName, "role", roleName))
            .delete()) {
      assertThat(response.getStatus()).isEqualTo(NO_CONTENT.getStatusCode());
    }
  }

  public void deletePrincipal(Principal principal) {
    deletePrincipal(principal.getName());
  }

  public void deletePrincipal(String principalName) {
    try (Response response =
        request("v1/principals/{name}", Map.of("name", principalName)).delete()) {
      assertThat(response.getStatus()).isEqualTo(NO_CONTENT.getStatusCode());
    }
  }

  public void deletePrincipalRole(PrincipalRole role) {
    try (Response response =
        request("v1/principal-roles/{name}", Map.of("name", role.getName())).delete()) {
      assertThat(response.getStatus()).isEqualTo(NO_CONTENT.getStatusCode());
    }
  }

  public void dropCatalog(String catalogName) {
    listCatalogRoles(catalogName).stream()
        .filter(cr -> !cr.getName().equals(PolarisEntityConstants.getNameOfCatalogAdminRole()))
        .forEach(role -> deleteCatalogRole(catalogName, role));

    deleteCatalog(catalogName);
  }
}
