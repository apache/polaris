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

  public Response updateCatalog(String catalog, UpdateCatalogRequest updateCatalogRequest) {
    return configure(client.target(format("%s/api/management/v1/catalogs/%s", baseUrl, catalog)))
        .put(Entity.json(updateCatalogRequest));
  }

  public Response getCatalog(String catalog) {
    return configure(client.target(format("%s/api/management/v1/catalogs/%s", baseUrl, catalog)))
        .get();
  }

  public Response dropCatalog(String catalog) {
    return configure(client.target(format("%s/api/management/v1/catalogs/" + catalog, baseUrl)))
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

  public Response createCatalogRole(String catalog, CatalogRole catalogRole) {
    return configure(
            client.target(
                format("%s/api/management/v1/catalogs/%s/catalog-roles", baseUrl, catalog)))
        .post(Entity.json(catalogRole));
  }

  public Response assignCatalogRole(String catalog, String principalRole, CatalogRole catalogRole) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/principal-roles/%s/catalog-roles/%s",
                    baseUrl, principalRole, catalog)))
        .put(Entity.json(catalogRole));
  }

  public Response getCatalogRole(String catalog, String catalogRole) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/%s",
                    baseUrl, catalog, catalogRole)))
        .get();
  }

  public Response createPrincipalRole(PrincipalRole principalRole) {
    return configure(client.target(format("%s/api/management/v1/principal", baseUrl)))
        .post(Entity.json(principalRole));
  }

  public Response createPrincipal(Principal principal) {
    return configure(client.target(format("%s/api/management/v1/principal", baseUrl)))
        .post(Entity.json(principal));
  }

  public Response grantPrincipalRole(
      String principal, GrantPrincipalRoleRequest grantPrincipalRoleRequest) {
    return configure(
            client.target(
                format("%s/api/management/v1/principals/%s/principal-roles", baseUrl, principal)))
        .put(Entity.json(grantPrincipalRoleRequest));
  }

  public Response dropPrincipalRole(String principalRole) {
    return configure(
            client.target(
                format("%s/api/management/v1/principal-roles/%s", baseUrl, principalRole)))
        .delete();
  }

  public Response dropPrincipal(String principal) {
    return configure(
            client.target(format("%s/api/management/v1/principals/%s", baseUrl, principal)))
        .delete();
  }

  public Response grantCatalog(String catalog, String catalogRole, CatalogGrant catalogGrant) {
    return configure(
            client.target(
                format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/%s/grants",
                    baseUrl, catalog, catalogRole)))
        .put(Entity.json(catalogGrant));
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
