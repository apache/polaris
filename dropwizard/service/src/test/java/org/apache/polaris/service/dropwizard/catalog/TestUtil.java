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
package org.apache.polaris.service.dropwizard.catalog;

import static org.apache.polaris.service.context.DefaultRealmContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.net.URI;
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
import org.apache.polaris.service.dropwizard.test.PolarisIntegrationTestFixture;
import org.apache.polaris.service.dropwizard.test.TestEnvironment;

/** Test utilities for catalog tests */
public class TestUtil {

  /**
   * Creates a catalog and grants the snowman principal permission to manage it.
   *
   * @return A client to interact with the catalog.
   */
  public static RESTCatalog createSnowmanManagedCatalog(
      TestEnvironment testEnv,
      PolarisIntegrationTestFixture fixture,
      Catalog catalog,
      Map<String, String> extraProperties) {
    return createSnowmanManagedCatalog(
        testEnv, fixture, testEnv.baseUri(), fixture.realm, catalog, extraProperties);
  }

  /**
   * Creates a catalog and grants the snowman principal permission to manage it.
   *
   * @return A client to interact with the catalog.
   */
  public static RESTCatalog createSnowmanManagedCatalog(
      TestEnvironment testEnv,
      PolarisIntegrationTestFixture fixture,
      URI baseUri,
      String realm,
      Catalog catalog,
      Map<String, String> extraProperties) {
    String currentCatalogName = catalog.getName();
    try (Response response =
        fixture
            .client
            .target(String.format("%s/api/management/v1/catalogs", baseUri))
            .request("application/json")
            .header("Authorization", "Bearer " + fixture.adminToken)
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(catalog))) {
      assertStatusCode(response, Response.Status.CREATED.getStatusCode());
    }

    // Create a new CatalogRole that has CATALOG_MANAGE_CONTENT and CATALOG_MANAGE_ACCESS
    CatalogRole newRole = new CatalogRole("custom-admin");
    try (Response response =
        fixture
            .client
            .target(
                String.format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles", baseUri, currentCatalogName))
            .request("application/json")
            .header("Authorization", "Bearer " + fixture.adminToken)
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(newRole))) {
      assertStatusCode(response, Response.Status.CREATED.getStatusCode());
    }
    CatalogGrant grantResource =
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG);
    try (Response response =
        fixture
            .client
            .target(
                String.format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/custom-admin/grants",
                    baseUri, currentCatalogName))
            .request("application/json")
            .header("Authorization", "Bearer " + fixture.adminToken)
            .header(REALM_PROPERTY_KEY, realm)
            .put(Entity.json(grantResource))) {
      assertStatusCode(response, Response.Status.CREATED.getStatusCode());
    }
    CatalogGrant grantAccessResource =
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_ACCESS, GrantResource.TypeEnum.CATALOG);
    try (Response response =
        fixture
            .client
            .target(
                String.format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/custom-admin/grants",
                    baseUri, currentCatalogName))
            .request("application/json")
            .header("Authorization", "Bearer " + fixture.adminToken)
            .header(REALM_PROPERTY_KEY, realm)
            .put(Entity.json(grantAccessResource))) {
      assertStatusCode(response, Response.Status.CREATED.getStatusCode());
    }

    // Assign this new CatalogRole to the service_admin PrincipalRole
    try (Response response =
        fixture
            .client
            .target(
                String.format(
                    "%s/api/management/v1/catalogs/%s/catalog-roles/custom-admin",
                    baseUri, currentCatalogName))
            .request("application/json")
            .header("Authorization", "Bearer " + fixture.adminToken)
            .header(REALM_PROPERTY_KEY, realm)
            .get()) {
      assertStatusCode(response, Response.Status.OK.getStatusCode());

      CatalogRole catalogRole = response.readEntity(CatalogRole.class);
      try (Response assignResponse =
          fixture
              .client
              .target(
                  String.format(
                      "%s/api/management/v1/principal-roles/%s/catalog-roles/%s",
                      baseUri,
                      fixture.snowmanCredentials.identifier().principalRoleName(),
                      currentCatalogName))
              .request("application/json")
              .header("Authorization", "Bearer " + fixture.adminToken)
              .header(REALM_PROPERTY_KEY, realm)
              .put(Entity.json(catalogRole))) {
        assertStatusCode(assignResponse, Response.Status.CREATED.getStatusCode());
      }
    }

    SessionCatalog.SessionContext context = SessionCatalog.SessionContext.createEmpty();
    RESTCatalog restCatalog =
        new RESTCatalog(
            context,
            (config) -> HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build());

    ImmutableMap.Builder<String, String> propertiesBuilder =
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, baseUri + "/api/catalog")
            .put(
                OAuth2Properties.CREDENTIAL,
                fixture.snowmanCredentials.clientId()
                    + ":"
                    + fixture.snowmanCredentials.clientSecret())
            .put(OAuth2Properties.SCOPE, BasePolarisAuthenticator.PRINCIPAL_ROLE_ALL)
            .put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO")
            .put("warehouse", currentCatalogName)
            .put("header." + REALM_PROPERTY_KEY, realm)
            .putAll(extraProperties);
    restCatalog.initialize("polaris", propertiesBuilder.buildKeepingLast());
    return restCatalog;
  }

  /**
   * Asserts that the response has the expected status code, with a fail message if the assertion
   * fails. The response entity is buffered so it can be read multiple times.
   *
   * @param response The response to check
   * @param expectedStatusCode The expected status code
   */
  private static void assertStatusCode(Response response, int expectedStatusCode) {
    // Buffer the entity so we can read it multiple times
    response.bufferEntity();

    assertThat(response)
        .withFailMessage(
            "Expected status code %s but got %s with message: %s",
            expectedStatusCode, response.getStatus(), response.readEntity(String.class))
        .returns(expectedStatusCode, Response::getStatus);
  }
}
