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

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IcebergHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.assertj.core.configuration.PreferredAssumptionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

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
  static {
    Assumptions.setPreferredAssumptionException(PreferredAssumptionException.JUNIT5);
  }

  public static Map<String, String> DEFAULT_REST_CATALOG_CONFIG =
      Map.of(
          org.apache.iceberg.CatalogProperties.VIEW_DEFAULT_PREFIX + "key1", "catalog-default-key1",
          org.apache.iceberg.CatalogProperties.VIEW_DEFAULT_PREFIX + "key2", "catalog-default-key2",
          org.apache.iceberg.CatalogProperties.VIEW_DEFAULT_PREFIX + "key3", "catalog-default-key3",
          org.apache.iceberg.CatalogProperties.VIEW_OVERRIDE_PREFIX + "key3",
              "catalog-override-key3",
          org.apache.iceberg.CatalogProperties.VIEW_OVERRIDE_PREFIX + "key4",
              "catalog-override-key4");

  private static ClientCredentials adminCredentials;
  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ManagementApi managementApi;

  private RESTCatalog restCatalog;

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
    Assumptions.assumeThat(shouldSkip()).isFalse();

    String principalName = client.newEntityName("snowman-rest");
    String principalRoleName = client.newEntityName("rest-admin");
    PrincipalWithCredentials principalCredentials =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);

    Method method = testInfo.getTestMethod().orElseThrow();
    String catalogName = client.newEntityName(method.getName());

    StorageConfigInfo storageConfig = getStorageConfigInfo();
    String defaultBaseLocation =
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
            .addProperty(FeatureConfiguration.DROP_WITH_PURGE_ENABLED.catalogConfig(), "true")
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
            client, endpoints, principalCredentials, catalogName, DEFAULT_REST_CATALOG_CONFIG);
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

  @Override
  @Test
  public void createViewWithCustomMetadataLocation() {
    Assertions.assertThatThrownBy(super::createViewWithCustomMetadataLocation)
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Forbidden: Invalid locations");
  }

  @Test
  public void createViewWithCustomMetadataLocationUsingPolaris(@TempDir Path tempDir) {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    String location = Paths.get(tempDir.toUri().toString()).toString();
    String customLocation = Paths.get(tempDir.toUri().toString(), "custom-location").toString();
    String customLocation2 = Paths.get(tempDir.toUri().toString(), "custom-location2").toString();
    String customLocationChild =
        Paths.get(tempDir.toUri().toString(), "custom-location/child").toString();

    catalog()
        .createNamespace(
            identifier.namespace(),
            ImmutableMap.of(
                IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY, location));

    Assertions.assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    // CAN create a view with a custom metadata location `baseLocation/customLocation`,
    // as long as the location is within the parent namespace's `write.metadata.path=baseLocation`
    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withDefaultCatalog(catalog().name())
            .withQuery("spark", "select * from ns.tbl")
            .withProperty(
                IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY, customLocation)
            .withLocation(location)
            .create();

    Assertions.assertThat(view).isNotNull();
    Assertions.assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();
    Assertions.assertThat(view.properties()).containsEntry("write.metadata.path", customLocation);
    Assertions.assertThat(((BaseView) view).operations().current().metadataFileLocation())
        .isNotNull()
        .startsWith(customLocation);

    // CANNOT update the view with a new metadata location `baseLocation/customLocation2`,
    // even though the new location is still under the parent namespace's
    // `write.metadata.path=baseLocation`.
    Assertions.assertThatThrownBy(
            () ->
                catalog()
                    .loadView(identifier)
                    .updateProperties()
                    .set(
                        IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY,
                        customLocation2)
                    .commit())
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Forbidden: Invalid locations");

    // CANNOT update the view with a child metadata location `baseLocation/customLocation/child`,
    // even though it is a subpath of the original view's
    // `write.metadata.path=baseLocation/customLocation`.
    Assertions.assertThatThrownBy(
            () ->
                catalog()
                    .loadView(identifier)
                    .updateProperties()
                    .set(
                        IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY,
                        customLocationChild)
                    .commit())
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Forbidden: Invalid locations");
  }
}
