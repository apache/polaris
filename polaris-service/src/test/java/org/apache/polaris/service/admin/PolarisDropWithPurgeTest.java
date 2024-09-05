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

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.PolarisApplication;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith({DropwizardExtensionsSupport.class, PolarisConnectionExtension.class})
public class PolarisDropWithPurgeTest {
  private static final DropwizardAppExtension<PolarisApplicationConfig> BASE_EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          // Bind to random port to support parallelism
          ConfigOverride.config("server.applicationConnectors[0].port", "0"),
          ConfigOverride.config("server.adminConnectors[0].port", "0"),
          // Drop with purge disabled at the extension level
          ConfigOverride.config("featureConfiguration.DROP_WITH_PURGE_ENABLED", "true"));

  private static PolarisConnectionExtension.PolarisToken adminToken;
  private static String userToken;
  private static String realm;
  private static String defaultCatalog;
  private static String strictCatalog;
  private static String namespace;
  private static final String baseLocation = "file:///tmp/PolarisDropWithPurgeTest";

  private static Catalog createCatalog(String catalog, Map<String, String> configs) {
    StorageConfigInfo config =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .build();
    CatalogProperties.Builder propertiesBuilder =
        CatalogProperties.builder()
            .setDefaultBaseLocation(String.format("%s/%s", baseLocation, catalog));
    for (Map.Entry<String, String> configEntry : configs.entrySet()) {
      propertiesBuilder.addProperty(configEntry.getKey(), configEntry.getValue());
    }
    return new Catalog(
        Catalog.TypeEnum.INTERNAL,
        catalog,
        propertiesBuilder.build(),
        1725487592064L,
        1725487592064L,
        1,
        config);
  }

  private static void setupCatalog(Catalog catalogObject) {
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
        request(String.format("catalog/v1/%s/namespaces", catalogObject.getName()))
            .post(Entity.json(createNamespaceRequest))) {
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new IllegalStateException(
            "Failed to create namespace: " + response.readEntity(String.class));
      }
    }
  }

  @BeforeAll
  public static void setup(PolarisConnectionExtension.PolarisToken adminToken) {
    userToken = adminToken.token();
    realm = PolarisConnectionExtension.getTestRealm(PolarisServiceImplIntegrationTest.class);
    defaultCatalog = String.format("default_catalog_%s", UUID.randomUUID().toString());
    strictCatalog = String.format("strict_catalog_%s", UUID.randomUUID().toString());

    setupCatalog(createCatalog(defaultCatalog, Map.of()));
    setupCatalog(
        createCatalog(
            strictCatalog,
            Map.of(PolarisConfiguration.DROP_WITH_PURGE_ENABLED.catalogConfig(), "false")));
  }

  private String createTable(String catalog) {
    String name = "table_" + UUID.randomUUID().toString();
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder().withName(name).withSchema(SCHEMA).build();
    String prefix = String.format("catalog/v1/%s/namespaces/%s/tables", catalog, namespace);
    try (Response response = request(prefix).post(Entity.json(createTableRequest))) {
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new IllegalStateException("Failed to create table: " + name);
      }
      return name;
    }
  }

  private Response dropTable(String catalog, String name, boolean purge) {
    String prefix =
        String.format("catalog/v1/%s/namespaces/%s/tables/%s", catalog, namespace, name);
    try (Response response =
        BASE_EXT
            .client()
            .target(String.format("http://localhost:%d/api/%s", BASE_EXT.getLocalPort(), prefix))
            .queryParam("purgeRequested", purge)
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm)
            .delete()) {
      return response;
    }
  }

  private static Invocation.Builder request(String prefix) {
    return BASE_EXT
        .client()
        .target(String.format("http://localhost:%d/api/%s", BASE_EXT.getLocalPort(), prefix))
        .request("application/json")
        .header("Authorization", "Bearer " + userToken)
        .header(REALM_PROPERTY_KEY, realm);
  }

  /** Used to define a parameterized test config */
  protected record TestConfig(String name, String catalog, Response.Status purgeResult) {

    @Override
    public String toString() {
      return name;
    }
  }

  private static Stream<TestConfig> getTestConfigs() {
    return Stream.of(
        new TestConfig("default catalog", defaultCatalog, Response.Status.OK)); //,
       //  new TestConfig("strict catalog", strictCatalog, Response.Status.FORBIDDEN));
  }

  @ParameterizedTest
  @MethodSource("getTestConfigs")
  void testDropTable(TestConfig config) {

    // Drop a table without purge
    Assertions.assertThat(dropTable(config.catalog, createTable(config.catalog), false))
        .returns(Response.Status.OK.getStatusCode(), Response::getStatus);

    // Drop a table twice:
    String t1 = createTable(config.catalog);
    Assertions.assertThat(dropTable(config.catalog, t1, false))
        .returns(Response.Status.OK.getStatusCode(), Response::getStatus);
    Assertions.assertThat(dropTable(config.catalog, t1, false))
        .returns(Response.Status.NOT_FOUND.getStatusCode(), Response::getStatus);

    // Drop a table with purge
    Assertions.assertThat(dropTable(config.catalog, createTable(config.catalog), true))
        .returns(config.purgeResult.getStatusCode(), Response::getStatus);
  }
}
