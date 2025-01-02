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
package org.apache.polaris.service.quarkus.catalog;

import static org.apache.polaris.service.context.DefaultRealmContextResolver.REALM_PROPERTY_KEY;

import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.service.quarkus.test.PolarisIntegrationTestFixture;
import org.apache.polaris.service.quarkus.test.PolarisIntegrationTestHelper;
import org.apache.polaris.service.quarkus.test.TestEnvironment;
import org.apache.polaris.service.quarkus.test.TestEnvironmentExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Import the full core Iceberg catalog tests by hitting the REST service via the RESTCatalog
 * client.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestEnvironmentExtension.class)
public abstract class PolarisRestCatalogViewIntegrationTest extends ViewCatalogTests<RESTCatalog> {

  @Inject PolarisIntegrationTestHelper helper;

  private TestEnvironment testEnv;
  private PolarisIntegrationTestFixture fixture;
  private RESTCatalog restCatalog;

  @BeforeAll
  public void createFixture(TestEnvironment testEnv, TestInfo testInfo) {
    Assumptions.assumeFalse(shouldSkip());
    this.testEnv = testEnv;
    fixture = helper.createFixture(testEnv, testInfo);
  }

  @BeforeEach
  public void setUpTempDir(@TempDir Path tempDir) throws Exception {
    // see https://github.com/quarkusio/quarkus/issues/13261
    Field field = ViewCatalogTests.class.getDeclaredField("tempDir");
    field.setAccessible(true);
    field.set(this, tempDir);
  }

  @BeforeEach
  void before(TestInfo testInfo) {
    testInfo
        .getTestMethod()
        .ifPresent(
            method -> {
              String catalogName = method.getName();
              try (Response response =
                  fixture
                      .client
                      .target(
                          String.format(
                              "%s/api/management/v1/catalogs/%s", testEnv.baseUri(), catalogName))
                      .request("application/json")
                      .header("Authorization", "Bearer " + fixture.adminToken)
                      .header(REALM_PROPERTY_KEY, fixture.realm)
                      .get()) {
                if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                  // Already exists! Must be in a parameterized test.
                  // Quick hack to get a unique catalogName.
                  // TODO: Have a while-loop instead with consecutive incrementing suffixes.
                  catalogName = catalogName + System.currentTimeMillis();
                }
              }

              StorageConfigInfo storageConfig = getStorageConfigInfo();
              String defaultBaseLocation =
                  storageConfig.getAllowedLocations().getFirst()
                      + "/"
                      + System.getenv("USER")
                      + "/path/to/data";

              org.apache.polaris.core.admin.model.CatalogProperties props =
                  org.apache.polaris.core.admin.model.CatalogProperties.builder(defaultBaseLocation)
                      .addProperty(
                          CatalogEntity.REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY,
                          "file:")
                      .addProperty(
                          PolarisConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(),
                          "true")
                      .addProperty(
                          PolarisConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
                          "true")
                      .build();
              Catalog catalog =
                  PolarisCatalog.builder()
                      .setType(Catalog.TypeEnum.INTERNAL)
                      .setName(catalogName)
                      .setProperties(props)
                      .setStorageConfigInfo(storageConfig)
                      .build();
              restCatalog =
                  TestUtil.createSnowmanManagedCatalog(testEnv, fixture, catalog, Map.of());
            });
  }

  @AfterAll
  public void destroyFixture() {
    if (fixture != null) {
      fixture.destroy();
    }
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
}
