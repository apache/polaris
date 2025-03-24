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
package org.apache.polaris.service.it.test;

import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.iceberg.view.ViewProperties;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IcebergHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Import the full core Iceberg catalog tests by hitting the REST service via the RESTCatalog
 * client.
 *
 * @implSpec This test expects the server to be configured with {@link
 *     org.apache.polaris.core.config.FeatureConfiguration#SUPPORTED_CATALOG_STORAGE_TYPES} set to
 *     the appropriate storage type.
 */
@ExtendWith(PolarisIntegrationTestExtension.class)
public abstract class PolarisRestCatalogViewIntegrationBase extends ViewCatalogTests<RESTCatalog> {

  private static ClientCredentials adminCredentials;
  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ManagementApi managementApi;

  private RESTCatalog restCatalog;
  private String defaultBaseLocation;

  @BeforeAll
  static void setup(PolarisApiEndpoints apiEndpoints, ClientCredentials credentials) {
    adminCredentials = credentials;
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    managementApi = client.managementApi(credentials);
  }

  @AfterAll
  static void close() throws Exception {
    client.close();
  }

  @BeforeEach
  public void before(TestInfo testInfo) {
    Assumptions.assumeFalse(shouldSkip());

    String principalName = client.newEntityName("snowman-rest");
    String principalRoleName = client.newEntityName("rest-admin");
    PrincipalWithCredentials principalCredentials =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);

    Method method = testInfo.getTestMethod().orElseThrow();
    String catalogName = client.newEntityName(method.getName());

    StorageConfigInfo storageConfig = getStorageConfigInfo();
    defaultBaseLocation =
        storageConfig.getAllowedLocations().getFirst()
            + "/"
            + System.getenv("USER")
            + "/path/to/data";

    CatalogProperties props =
        CatalogProperties.builder(defaultBaseLocation)
            .addProperty(
                CatalogEntity.REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY, "file:")
            .addProperty(FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(), "true")
            .addProperty(
                FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "true")
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(props)
            .setStorageConfigInfo(storageConfig)
            .build();
    managementApi.createCatalog(principalRoleName, catalog);

    restCatalog =
        IcebergHelper.restCatalog(
            client,
            endpoints,
            principalCredentials,
            catalogName,
            Map.of(
                org.apache.iceberg.CatalogProperties.VIEW_DEFAULT_PREFIX + "key1",
                "catalog-default-key1",
                org.apache.iceberg.CatalogProperties.VIEW_DEFAULT_PREFIX + "key2",
                "catalog-default-key2"));
  }

  @AfterEach
  public void cleanUp() {
    client.cleanUp(adminCredentials);
  }

  /**
   * @return The catalog's storage config.
   */
  protected abstract StorageConfigInfo getStorageConfigInfo();

  /**
   * @return Whether the tests should be skipped, for example due to environment variables not being
   *     specified.
   */
  protected abstract boolean shouldSkip();

  @Override
  protected RESTCatalog catalog() {
    return restCatalog;
  }

  @Override
  protected org.apache.iceberg.catalog.Catalog tableCatalog() {
    return restCatalog;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  @Override
  protected boolean overridesRequestedLocation() {
    return true;
  }

  /**
   * Overrides the test from the superclass because the tempDir used there is not accessible or
   * included in the allowed locations.
   */
  @Override
  @Test
  public void createViewWithCustomMetadataLocation() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    String location = Paths.get(defaultBaseLocation).toString();
    String customLocation = Paths.get(defaultBaseLocation, "custom-location").toString();

    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withDefaultCatalog(catalog().name())
            .withQuery("spark", "select * from ns.tbl")
            .withProperty(ViewProperties.WRITE_METADATA_LOCATION, customLocation)
            .withLocation(location)
            .create();

    assertThat(view).isNotNull();
    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();
    assertThat(view.properties()).containsEntry("write.metadata.path", customLocation);
    assertThat(((BaseView) view).operations().current().metadataFileLocation())
        .isNotNull()
        .startsWith(customLocation);
  }
}
