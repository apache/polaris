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

import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.service.auth.BasePolarisAuthenticator;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.SnowmanCredentialsExtension;

/** Test utilities for catalog tests */
public class TestUtil {
  /** Performs createSnowmanManagedCatalog() on a Dropwizard instance of Polaris */
  public static RESTCatalog createSnowmanManagedCatalog(
      DropwizardAppExtension<PolarisApplicationConfig> EXT,
      PolarisConnectionExtension.PolarisToken adminToken,
      SnowmanCredentialsExtension.SnowmanCredentials snowmanCredentials,
      String realm,
      Catalog catalog,
      Map<String, String> extraProperties) {
    return createSnowmanManagedCatalog(
        EXT.client(),
        String.format("http://localhost:%d", EXT.getLocalPort()),
        adminToken,
        snowmanCredentials,
        realm,
        catalog,
        extraProperties);
  }

  /**
   * Creates a catalog and grants the snowman principal from SnowmanCredentialsExtension permission
   * to manage it.
   *
   * @return A client to interact with the catalog.
   */
  public static RESTCatalog createSnowmanManagedCatalog(
      Client client,
      String baseUrl,
      PolarisConnectionExtension.PolarisToken adminToken,
      SnowmanCredentialsExtension.SnowmanCredentials snowmanCredentials,
      String realm,
      Catalog catalog,
      Map<String, String> extraProperties) {
    String currentCatalogName = catalog.getName();
    try (Response response =
        client
            .target(String.format("%s/api/management/v1/catalogs", baseUrl))
            .request("application/json")
            .header("Authorization", "Bearer " + adminToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(catalog))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Create a new CatalogRole that has CATALOG_MANAGE_CONTENT and CATALOG_MANAGE_ACCESS
    CatalogRole newRole = new CatalogRole("custom-admin");
    try (Response response =
        client
            .target(
                String.format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles", baseUrl, currentCatalogName))
            .request("application/json")
            .header("Authorization", "Bearer " + adminToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(newRole))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    CatalogGrant grantResource =
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG);
    try (Response response =
        client
            .target(
                String.format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/custom-admin/grants",
                    baseUrl, currentCatalogName))
            .request("application/json")
            .header("Authorization", "Bearer " + adminToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .put(Entity.json(grantResource))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    CatalogGrant grantAccessResource =
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_ACCESS, GrantResource.TypeEnum.CATALOG);
    try (Response response =
        client
            .target(
                String.format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/custom-admin/grants",
                    baseUrl, currentCatalogName))
            .request("application/json")
            .header("Authorization", "Bearer " + adminToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .put(Entity.json(grantAccessResource))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Assign this new CatalogRole to the service_admin PrincipalRole
    try (Response response =
        client
            .target(
                String.format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/custom-admin",
                    baseUrl, currentCatalogName))
            .request("application/json")
            .header("Authorization", "Bearer " + adminToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      CatalogRole catalogRole = response.readEntity(CatalogRole.class);
      try (Response assignResponse =
          client
              .target(
                  String.format(
                      "%s/api/management/v1/principal-roles/%s/catalog-roles/%s",
                      baseUrl,
                      snowmanCredentials.identifier().principalRoleName(),
                      currentCatalogName))
              .request("application/json")
              .header("Authorization", "Bearer " + adminToken.token())
              .header(REALM_PROPERTY_KEY, realm)
              .put(Entity.json(catalogRole))) {
        assertThat(assignResponse)
            .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
      }
    }

    SessionCatalog.SessionContext context = SessionCatalog.SessionContext.createEmpty();
    RESTCatalog restCatalog =
        new RESTCatalog(
            context,
            (config) -> HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build());

    ImmutableMap.Builder<String, String> propertiesBuilder =
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, baseUrl + "/api/catalog")
            .put(
                OAuth2Properties.CREDENTIAL,
                snowmanCredentials.clientId() + ":" + snowmanCredentials.clientSecret())
            .put(OAuth2Properties.SCOPE, BasePolarisAuthenticator.PRINCIPAL_ROLE_ALL)
            .put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO")
            .put("warehouse", currentCatalogName)
            .put("header." + REALM_PROPERTY_KEY, realm)
            .putAll(extraProperties);
    restCatalog.initialize("polaris", propertiesBuilder.buildKeepingLast());
    return restCatalog;
  }
}
