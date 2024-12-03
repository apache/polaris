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
package org.apache.polaris.service.admin;

import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.PolarisApplication;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.PolarisRealm;
import org.apache.polaris.service.test.TestEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith({
  DropwizardExtensionsSupport.class,
  TestEnvironmentExtension.class,
  PolarisConnectionExtension.class
})
public class PolarisOverlappingTableTest {
  private static final DropwizardAppExtension<PolarisApplicationConfig> BASE_EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          // Bind to random port to support parallelism
          ConfigOverride.config("server.applicationConnectors[0].port", "0"),
          ConfigOverride.config("server.adminConnectors[0].port", "0"),
          // Enforce table location constraints
          ConfigOverride.config("featureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION", "false"),
          ConfigOverride.config("featureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP", "false"));

  private static final DropwizardAppExtension<PolarisApplicationConfig> LAX_EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          // Bind to random port to support parallelism
          ConfigOverride.config("server.applicationConnectors[0].port", "0"),
          ConfigOverride.config("server.adminConnectors[0].port", "0"),
          // Relax table location constraints
          ConfigOverride.config("featureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION", "true"),
          ConfigOverride.config("featureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP", "true"));

  private static PolarisConnectionExtension.PolarisToken adminToken;
  private static String userToken;
  private static String realm;
  private static String namespace;
  private static final String baseLocation = "file:///tmp/PolarisOverlappingTableTest";

  private static final CatalogWrapper defaultCatalog = new CatalogWrapper("default");
  private static final CatalogWrapper laxCatalog = new CatalogWrapper("lax");
  private static final CatalogWrapper strictCatalog = new CatalogWrapper("strict");

  /** Used to define a parameterized test config */
  protected record TestConfig(
      DropwizardAppExtension<PolarisApplicationConfig> extension,
      CatalogWrapper catalogWrapper,
      Response.Status response) {
    public String catalog() {
      return catalogWrapper.catalog;
    }

    private String extensionName() {
      return (extension
              .getConfiguration()
              .findService(PolarisConfigurationStore.class)
              .getConfiguration(null, PolarisConfiguration.ALLOW_TABLE_LOCATION_OVERLAP))
          ? "lax"
          : "strict";
    }

    /** Extract the first component of the catalog name; e.g. `default` from `default_123_xyz` */
    private String catalogShortName() {
      int firstComponentEnd = catalog().indexOf('_');
      if (firstComponentEnd != -1) {
        return catalog().substring(0, firstComponentEnd);
      } else {
        return catalog();
      }
    }

    @Override
    public String toString() {
      return String.format(
          "extension=%s, catalog=%s, status=%s",
          extensionName(), catalogShortName(), response.toString());
    }
  }

  /* Used to wrap a catalog name, so the TestConfig's final `catalog` field can be updated */
  protected static class CatalogWrapper {
    public String catalog;

    public CatalogWrapper(String catalog) {
      this.catalog = catalog;
    }

    @Override
    public String toString() {
      return catalog;
    }
  }

  @BeforeEach
  public void setup(
      PolarisConnectionExtension.PolarisToken adminToken, @PolarisRealm String polarisRealm) {
    userToken = adminToken.token();
    realm = polarisRealm;
    defaultCatalog.catalog = String.format("default_catalog_%s", UUID.randomUUID().toString());
    laxCatalog.catalog = String.format("lax_catalog_%s", UUID.randomUUID().toString());
    strictCatalog.catalog = String.format("strict_catalog_%s", UUID.randomUUID().toString());
    for (var EXT : List.of(BASE_EXT, LAX_EXT)) {
      for (var c : List.of(defaultCatalog, laxCatalog, strictCatalog)) {
        CatalogProperties.Builder propertiesBuilder =
            CatalogProperties.builder()
                .setDefaultBaseLocation(String.format("%s/%s", baseLocation, c));
        if (!c.equals(defaultCatalog)) {
          propertiesBuilder
              .addProperty(
                  PolarisConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
                  String.valueOf(c.equals(laxCatalog)))
              .addProperty(
                  PolarisConfiguration.ALLOW_TABLE_LOCATION_OVERLAP.catalogConfig(),
                  String.valueOf(c.equals(laxCatalog)));
        }
        StorageConfigInfo config =
            FileStorageConfigInfo.builder()
                .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
                .build();
        Catalog catalogObject =
            new Catalog(
                Catalog.TypeEnum.INTERNAL,
                c.catalog,
                propertiesBuilder.build(),
                1725487592064L,
                1725487592064L,
                1,
                config);
        try (Response response =
            request(EXT, "management/v1/catalogs")
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
            request(EXT, String.format("catalog/v1/%s/namespaces", c))
                .post(Entity.json(createNamespaceRequest))) {
          if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new IllegalStateException(
                "Failed to create namespace: " + response.readEntity(String.class));
          }
        }
      }
    }
  }

  private Response createTable(
      DropwizardAppExtension<PolarisApplicationConfig> extension, String catalog, String location) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder()
            .withName("table_" + UUID.randomUUID().toString())
            .withLocation(location)
            .withSchema(SCHEMA)
            .build();
    String prefix = String.format("catalog/v1/%s/namespaces/%s/tables", catalog, namespace);
    try (Response response = request(extension, prefix).post(Entity.json(createTableRequest))) {
      return response;
    }
  }

  private static Invocation.Builder request(
      DropwizardAppExtension<PolarisApplicationConfig> extension, String prefix) {
    return extension
        .client()
        .target(String.format("http://localhost:%d/api/%s", extension.getLocalPort(), prefix))
        .request("application/json")
        .header("Authorization", "Bearer " + userToken)
        .header(REALM_PROPERTY_KEY, realm);
  }

  private static Stream<TestConfig> getTestConfigs() {
    return Stream.of(
        new TestConfig(BASE_EXT, defaultCatalog, Response.Status.FORBIDDEN),
        new TestConfig(BASE_EXT, strictCatalog, Response.Status.FORBIDDEN),
        new TestConfig(BASE_EXT, laxCatalog, Response.Status.OK),
        new TestConfig(LAX_EXT, defaultCatalog, Response.Status.OK),
        new TestConfig(LAX_EXT, strictCatalog, Response.Status.FORBIDDEN),
        new TestConfig(LAX_EXT, laxCatalog, Response.Status.OK));
  }

  @ParameterizedTest
  @MethodSource("getTestConfigs")
  @DisplayName("Test restrictions on table locations")
  void testTableLocationRestrictions(TestConfig config) {
    // Original table
    assertThat(
            createTable(
                config.extension,
                config.catalog(),
                String.format("%s/%s/%s/table_1", baseLocation, config.catalog(), namespace)))
        .returns(Response.Status.OK.getStatusCode(), Response::getStatus);

    // Unrelated path
    assertThat(
            createTable(
                config.extension,
                config.catalog(),
                String.format("%s/%s/%s/table_2", baseLocation, config.catalog(), namespace)))
        .returns(Response.Status.OK.getStatusCode(), Response::getStatus);

    // Trailing slash makes this not overlap with table_1
    assertThat(
            createTable(
                config.extension,
                config.catalog(),
                String.format("%s/%s/%s/table_100", baseLocation, config.catalog(), namespace)))
        .returns(Response.Status.OK.getStatusCode(), Response::getStatus);

    // Repeat location
    assertThat(
            createTable(
                config.extension,
                config.catalog(),
                String.format("%s/%s/%s/table_100", baseLocation, config.catalog(), namespace)))
        .returns(config.response.getStatusCode(), Response::getStatus);

    // Parent of existing location
    assertThat(
            createTable(
                config.extension,
                config.catalog(),
                String.format("%s/%s/%s", baseLocation, config.catalog(), namespace)))
        .returns(config.response.getStatusCode(), Response::getStatus);

    // Child of existing location
    assertThat(
            createTable(
                config.extension,
                config.catalog(),
                String.format(
                    "%s/%s/%s/table_100/child", baseLocation, config.catalog(), namespace)))
        .returns(config.response.getStatusCode(), Response::getStatus);

    // Outside the namespace
    assertThat(
            createTable(
                config.extension,
                config.catalog(),
                String.format("%s/%s", baseLocation, config.catalog())))
        .returns(config.response.getStatusCode(), Response::getStatus);

    // Outside the catalog
    assertThat(createTable(config.extension, config.catalog(), String.format("%s", baseLocation)))
        .returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
  }
}
