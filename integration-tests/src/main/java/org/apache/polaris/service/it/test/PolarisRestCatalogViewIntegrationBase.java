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

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.FeatureConfiguration;
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

  private static String adminToken;
  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ManagementApi managementApi;

  private RESTCatalog restCatalog;
  private StorageConfigInfo storageConfig;
  private String defaultBaseLocation;

  @BeforeAll
  static void setup(PolarisApiEndpoints apiEndpoints, ClientCredentials credentials) {
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    adminToken = client.obtainToken(credentials);
    managementApi = client.managementApi(adminToken);
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

    storageConfig = getStorageConfigInfo();
    defaultBaseLocation =
        storageConfig.getAllowedLocations().getFirst()
            + "/"
            + System.getenv("USER")
            + "/path/to/data";

    CatalogProperties props =
        CatalogProperties.builder(defaultBaseLocation)
            .addProperty(
                FeatureConfiguration.ALLOW_EXTERNAL_METADATA_FILE_LOCATION.catalogConfig(), "true")
            .addProperty(
                FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "true")
            .addProperty(FeatureConfiguration.DROP_WITH_PURGE_ENABLED.catalogConfig(), "true")
            .addProperty(FeatureConfiguration.PURGE_VIEW_METADATA_ON_DROP.catalogConfig(), "false")
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(props)
            .setStorageConfigInfo(storageConfig)
            .build();

    createPolarisCatalog(catalog);
    managementApi.makeAdmin(principalRoleName, catalog);

    restCatalog =
        IcebergHelper.restCatalog(
            endpoints,
            catalogName,
            DEFAULT_REST_CATALOG_CONFIG,
            client.obtainToken(principalCredentials));
  }

  @AfterEach
  public void cleanUp() {
    client.cleanUp(adminToken);
  }

  /** Overridable methods to allow subclasses to execute additional logic on catalog creation. */
  protected void createPolarisCatalog(Catalog catalog) {
    managementApi.createCatalog(catalog);
  }

  /**
   * @return The catalog's storage config.
   */
  protected abstract StorageConfigInfo getStorageConfigInfo();

  /**
   * Builds the {@code allowedLocations} list for a catalog rooted at {@code baseLocation}.
   *
   * <p>Namespace creation validates the namespace's default location against the catalog's allowed
   * locations with the namespace name appended. {@link #defaultBaseLocation} always resolves to
   * {@code baseLocation/$USER/path/to/data}, so that variant must be present alongside the raw base
   * location or top-level namespace creation is rejected.
   */
  protected static List<String> allowedLocations(String baseLocation) {
    return List.of(baseLocation, baseLocation + "/" + System.getenv("USER") + "/path/to/data");
  }

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
   * Roots view locations at the catalog's default base location instead of the {@code @TempDir}
   * used by {@link ViewCatalogTests}.
   *
   * <p>The inherited implementation derives locations from a local temporary directory, which
   * Polaris rejects because it falls outside the catalog's allowed locations, and which cloud
   * storage backends cannot address at all. Anchoring on the base location keeps the requested
   * locations valid for every storage type this class is subclassed for.
   *
   * <p>The file-based test base builds its base location via {@link java.nio.file.Path#toUri},
   * which produces a {@code file:///} URI. Polaris normalises stored metadata paths to the
   * single-slash form {@code file:/} used by {@link java.io.File#toURI}. Stripping the redundant
   * authority component keeps the return value consistent with the stored form so that the {@code
   * startsWith} assertions in {@link ViewCatalogTests} remain valid for file-backed catalogs. Cloud
   * URI schemes (s3://, gs://, abfss://) are not affected by this normalisation.
   */
  @Override
  protected String viewLocation(String... paths) {
    StringBuilder location =
        new StringBuilder(LocationUtil.stripTrailingSlash(defaultBaseLocation));
    for (String path : paths) {
      location.append("/").append(path);
    }

    return normalizeToStoredForm(location.toString());
  }

  /**
   * Normalises a {@code file:///} URI to the single-slash {@code file:/} form that Polaris stores
   * (see {@link #viewLocation} for the rationale). Cloud URI schemes (s3://, gs://, abfss://) are
   * returned unchanged.
   */
  private static String normalizeToStoredForm(String location) {
    if (location.startsWith("file:///")) {
      return "file:/" + location.substring(8);
    }
    return location;
  }

  @Test
  public void createViewWithCustomMetadataLocationUsingPolaris() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    String location = defaultBaseLocation + "/custom-view-location";
    String customLocation =
        normalizeToStoredForm(
                LocationUtil.stripTrailingSlash(storageConfig.getAllowedLocations().getFirst()))
            + "/custom-location1";

    catalog().createNamespace(identifier.namespace());

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
  }
}
