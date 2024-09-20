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
package org.apache.polaris.service.catalog;

import static java.lang.String.format;
import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Response;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.polaris.core.admin.model.*;
import org.apache.polaris.service.types.NotificationRequest;

public class PolarisTestClient {
  private final Client client;
  private final String baseUrl;
  private final String token;
  private final String realm;

  public PolarisTestClient(Client client, int port, String token, String realm) {
    this.client = client;
    this.baseUrl = "http://localhost:" + port;
    this.token = token;
    this.realm = realm;
  }

  public Response createCatalog(Catalog catalog) {
    return configure(client.target(format("%s/api/management/v1/catalogs", baseUrl)))
        .post(Entity.json(catalog));
  }

  public Response listCatalogs() {
    return configure(client.target(format("%s/api/management/v1/catalogs", baseUrl))).get();
  }

  public Response updateCatalog(String catalog, UpdateCatalogRequest updateCatalogRequest) {
    return configure(client.target(format("%s/api/management/v1/catalogs/%s", baseUrl, catalog)))
        .put(Entity.json(updateCatalogRequest));
  }

  public Response getCatalog(String catalog) {
    return configure(client.target(format("%s/api/management/v1/catalogs/%s", baseUrl, catalog)))
        .get();
  }

  public Response deleteCatalog(String catalog) {
    return configure(client.target(format("%s/api/management/v1/catalogs/" + catalog, baseUrl)))
        .delete();
  }

  public Response createNamespace(String catalog, String namespace) {
    return configure(client.target(format("%s/api/catalog/v1/%s/namespaces", baseUrl, catalog)))
        .post(
            Entity.json(
                CreateNamespaceRequest.builder().withNamespace(Namespace.of(namespace)).build()));
  }

  public Response listNamespaces(String catalog) {
    return configure(client.target(format("%s/api/catalog/v1/%s/namespaces", baseUrl, catalog)))
        .get();
  }

  public Response deleteNamespace(String catalog, String namespace) {
    return configure(
            client.target(
                format("%s/api/catalog/v1/%s/namespaces/%s", baseUrl, catalog, namespace)))
        .delete();
  }

  public Response getTable(String catalog, String namespace, String table) {
    return configure(
            client.target(
                format(
                    "%s/api/catalog/v1/%s/namespaces/%s/tables/%s",
                    baseUrl, catalog, namespace, table)))
        .get();
  }

  public Response registerTable(
      String catalog, String namespace, String table, String metadataLocation) {
    return configure(
            client.target(
                format("%s/api/catalog/v1/%s/namespaces/%s/register", baseUrl, catalog, namespace)))
        .post(
            Entity.json(
                ImmutableRegisterTableRequest.builder()
                    .name(table)
                    .metadataLocation(metadataLocation)
                    .build()));
  }

  public Response createPrincipal(Principal principal) {
    return configure(client.target(format("%s/api/management/v1/principals", baseUrl)))
        .post(Entity.json(principal));
  }

  public Response createPrincipal(Principal principal, Boolean credentialRotationRequired) {
    return configure(client.target(format("%s/api/management/v1/principals", baseUrl)))
        .post(Entity.json(new CreatePrincipalRequest(principal, credentialRotationRequired)));
  }

  public Response listPrincipals() {
    return configure(client.target(format("%s/api/management/v1/principals", baseUrl))).get();
  }

  public Response listPrincipals(String principalRole) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/principal-roles/%s/principals", baseUrl, principalRole)))
        .get();
  }

  public Response getPrincipal(String principal) {
    return configure(
            client.target(format("%s/api/management/v1/principals/%s", baseUrl, principal)))
        .get();
  }

  public Response updatePrincipal(String principal, UpdatePrincipalRequest updatePrincipalRequest) {
    return configure(
            client.target(format("%s/api/management/v1/principals/%s", baseUrl, principal)))
        .put(Entity.json(updatePrincipalRequest));
  }

  public Response deletePrincipal(String principal) {
    return configure(
            client.target(format("%s/api/management/v1/principals/%s", baseUrl, principal)))
        .delete();
  }

  public Response createPrincipalRole(PrincipalRole principalRole) {
    return configure(client.target(format("%s/api/management/v1/principal-roles", baseUrl)))
        .post(Entity.json(principalRole));
  }

  public Response listPrincipalRoles() {
    return configure(client.target(format("%s/api/management/v1/principal-roles", baseUrl))).get();
  }

  public Response listPrincipalRoles(String principal) {
    return configure(
            client.target(
                format("%s/api/management/v1/principals/%s/principal-roles", baseUrl, principal)))
        .get();
  }

  public Response listPrincipalRoles(String catalog, String catalogRole) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/%s/principal-roles",
                    baseUrl, catalog, catalogRole)))
        .get();
  }

  public Response getPrincipalRole(String principalRole) {
    return configure(
            client.target(
                format("%s/api/management/v1/principal-roles/%s", baseUrl, principalRole)))
        .get();
  }

  public Response updatePrincipalRole(
      String principalRole, UpdatePrincipalRoleRequest updatePrincipalRoleRequest) {
    return configure(
            client.target(
                format("%s/api/management/v1/principal-roles/%s", baseUrl, principalRole)))
        .put(Entity.json(updatePrincipalRoleRequest));
  }

  public Response assignPrincipalRole(String principal, PrincipalRole principalRole) {
    return configure(
            client.target(
                format("%s/api/management/v1/principals/%s/principal-roles", baseUrl, principal)))
        .put(Entity.json(principalRole));
  }

  public Response grantPrincipalRole(
      String principal, GrantPrincipalRoleRequest grantPrincipalRoleRequest) {
    return configure(
            client.target(
                format("%s/api/management/v1/principals/%s/principal-roles", baseUrl, principal)))
        .put(Entity.json(grantPrincipalRoleRequest));
  }

  public Response revokePrincipalRole(String principal, String principalRole) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/principals/%s/principal-roles/%s",
                    baseUrl, principal, principalRole)))
        .delete();
  }

  public Response deletePrincipalRole(String principalRole) {
    return configure(
            client.target(
                format("%s/api/management/v1/principal-roles/%s", baseUrl, principalRole)))
        .delete();
  }

  public Response createCatalogRole(String catalog, CatalogRole catalogRole) {
    return configure(
            client.target(
                format("%s/api/management/v1/catalogs/%s/catalog-roles", baseUrl, catalog)))
        .post(Entity.json(catalogRole));
  }

  public Response listCatalogRoles(String catalog) {
    return configure(
            client.target(
                format("%s/api/management/v1/catalogs/%s/catalog-roles", baseUrl, catalog)))
        .get();
  }

  public Response listCatalogRoles(String principalRole, String catalogProle) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/principal-roles/%s/catalog-roles/%s",
                    baseUrl, principalRole, catalogProle)))
        .get();
  }

  public Response getCatalogRole(String catalog, String catalogRole) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/%s",
                    baseUrl, catalog, catalogRole)))
        .get();
  }

  public Response updateCatalogRole(
      String catalog, String catalogRole, UpdateCatalogRoleRequest request) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/%s",
                    baseUrl, catalog, catalogRole)))
        .put(Entity.json(request));
  }

  public Response grantCatalogRoleToPrincipalRole(
      String principalRole, String catalogRole, CatalogRole role) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/principal-roles/%s/catalog-roles/%s",
                    baseUrl, principalRole, catalogRole)))
        .put(Entity.json(role));
  }

  public Response grantPrivilegeToCatalogRole(
      String catalog, String catalogRole, GrantResource grantResource) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/%s/grants",
                    baseUrl, catalog, catalogRole)))
        .put(Entity.json(new AddGrantRequest(grantResource)));
  }

  public Response grantCatalogRole(String catalog, String catalogRole, CatalogGrant catalogGrant) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/%s/grants",
                    baseUrl, catalog, catalogRole)))
        .put(Entity.json(catalogGrant));
  }

  public Response deleteCatalogRole(String catalog, String catalogRole) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/%s",
                    baseUrl, catalog, catalogRole)))
        .delete();
  }

  public Response revokeCatalogRole(String principalRole, String catalog, String catalogRole) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/principal-roles/%s/catalog-roles/%s/%s",
                    baseUrl, principalRole, catalog, catalogRole)))
        .delete();
  }

  public Response grantResource(String catalog, String catalogRole, GrantResource grantResource) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/%s/grants",
                    baseUrl, catalog, catalogRole)))
        .put(Entity.json(grantResource));
  }

  public Response listGrants(String catalog, String catalogRole) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/%s/grants",
                    baseUrl, catalog, catalogRole)))
        .get();
  }

  public Response rotateCredential(String principal) {
    return configure(
            client.target(format("%s/api/management/v1/principals/%s/rotate", baseUrl, principal)))
        .post(Entity.json(""));
  }

  public Response sendNotification(
      String catalog, String namespace, String table, NotificationRequest notificationRequest) {
    return configure(
            client.target(
                format(
                    "%s/api/catalog/v1/%s/namespaces/%s/tables/%s/notifications",
                    baseUrl, catalog, namespace, table)))
        .post(Entity.json(notificationRequest));
  }

  private Invocation.Builder configure(WebTarget target) {
    return target
        .request("application/json")
        .header("Authorization", "BEARER " + token)
        .header(REALM_PROPERTY_KEY, realm);
  }
}
