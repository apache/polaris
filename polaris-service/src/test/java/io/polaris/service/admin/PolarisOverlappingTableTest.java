/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.service.admin;

import static io.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static io.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.polaris.core.admin.model.Catalog;
import io.polaris.core.admin.model.CatalogProperties;
import io.polaris.core.admin.model.CreateCatalogRequest;
import io.polaris.core.admin.model.FileStorageConfigInfo;
import io.polaris.core.admin.model.StorageConfigInfo;
import io.polaris.service.PolarisApplication;
import io.polaris.service.config.PolarisApplicationConfig;
import io.polaris.service.test.PolarisConnectionExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({DropwizardExtensionsSupport.class, PolarisConnectionExtension.class})
public class PolarisOverlappingTableTest {
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          // Bind to random port to support parallelism
          ConfigOverride.config("server.applicationConnectors[0].port", "0"),
          ConfigOverride.config("server.adminConnectors[0].port", "0"),
          // Block overlapping table paths globally:
          ConfigOverride.config(
              "featureConfiguration.ENFORCE_GLOBALLY_UNIQUE_TABLE_LOCATIONS", "true"),
          // The value of this parameter is irrelevant because of
          // ENFORCE_GLOBALLY_UNIQUE_TABLE_LOCATIONS
          ConfigOverride.config("featureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP", "true"));
  private static String userToken;
  private static String realm;
  private static String catalog;
  private static String namespace;
  private static final String baseLocation = "file:///tmp/PolarisOverlappingTableTest";

  @BeforeAll
  public static void setup(PolarisConnectionExtension.PolarisToken adminToken) {
    userToken = adminToken.token();
    realm = PolarisConnectionExtension.getTestRealm(PolarisServiceImplIntegrationTest.class);
    catalog = String.format("catalog_%s", UUID.randomUUID().toString());
    StorageConfigInfo config =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .build();
    Catalog catalogObject =
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            catalog,
            new CatalogProperties(String.format("%s/%s", baseLocation, catalog)),
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            1,
            config);
    try (Response response =
        request("management/v1/catalogs")
            .post(Entity.json(new CreateCatalogRequest(catalogObject)))) {
      if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
        throw new IllegalStateException(
            "Failed to create catalog: " + response.readEntity(String.class));
      }
    }

    namespace = "ns";
    CreateNamespaceRequest createNamespaceRequest =
        CreateNamespaceRequest.builder().withNamespace(Namespace.of(namespace)).build();
    try (Response response =
        request(String.format("catalog/v1/%s/namespaces", catalog))
            .post(Entity.json(createNamespaceRequest))) {
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new IllegalStateException(
            "Failed to create namespace: " + response.readEntity(String.class));
      }
    }
  }

  private Response createTable(String location) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder()
            .withName("table_" + UUID.randomUUID().toString())
            .withLocation(location)
            .withSchema(SCHEMA)
            .build();
    String prefix = String.format("catalog/v1/%s/namespaces/%s/tables", catalog, namespace);
    try (Response response = request(prefix).post(Entity.json(createTableRequest))) {
      String responseBody = response.readEntity(String.class);
      return response;
    }
  }

  private static Invocation.Builder request(String prefix) {
    return EXT.client()
        .target(String.format("http://localhost:%d/api/%s", EXT.getLocalPort(), prefix))
        .request("application/json")
        .header("Authorization", "Bearer " + userToken)
        .header(REALM_PROPERTY_KEY, realm);
  }

  @Test
  public void testBasicOverlappingTables() {
    // Original table
    assertThat(createTable(String.format("%s/%s/%s/table_1", baseLocation, catalog, namespace)))
        .returns(Response.Status.OK.getStatusCode(), Response::getStatus);

    // Unrelated path
    assertThat(createTable(String.format("%s/%s/%s/table_2", baseLocation, catalog, namespace)))
        .returns(Response.Status.OK.getStatusCode(), Response::getStatus);

    // Trailing slash makes this not overlap with table_1
    assertThat(createTable(String.format("%s/%s/%s/table_100", baseLocation, catalog, namespace)))
        .returns(Response.Status.OK.getStatusCode(), Response::getStatus);

    // Repeat location
    assertThat(createTable(String.format("%s/%s/%s/table_100", baseLocation, catalog, namespace)))
        .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);

    // Parent of existing location
    assertThat(createTable(String.format("%s/%s/%s", baseLocation, catalog, namespace)))
        .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);

    // Child of existing location
    assertThat(createTable(String.format("%s/%s/%s/table_100/child", baseLocation, catalog, namespace)))
        .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);

    // Outside the namespace
    assertThat(createTable(String.format("%s/%s", baseLocation, catalog)))
        .returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);

    // Outside the catalog
    assertThat(createTable(String.format("%s", baseLocation)))
        .returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
  }
}
