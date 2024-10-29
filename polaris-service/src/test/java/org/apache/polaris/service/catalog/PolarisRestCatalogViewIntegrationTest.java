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

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.service.PolarisApplication;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.PolarisConnectionExtension.PolarisToken;
import org.apache.polaris.service.test.PolarisRealm;
import org.apache.polaris.service.test.SnowmanCredentialsExtension;
import org.apache.polaris.service.test.SnowmanCredentialsExtension.SnowmanCredentials;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Import the full core Iceberg catalog tests by hitting the REST service via the RESTCatalog
 * client.
 */
@ExtendWith({
  DropwizardExtensionsSupport.class,
  PolarisConnectionExtension.class,
  SnowmanCredentialsExtension.class
})
public class PolarisRestCatalogViewIntegrationTest extends ViewCatalogTests<RESTCatalog> {
  public static final String IDENTITY =
      Optional.ofNullable(System.getenv("INTEGRATION_TEST_ROLE_ARN")) // Backward compatability
              .orElse(Optional.ofNullable(System.getenv("INTEGRATION_TEST_IDENTITY"))
                      .orElse("arn:aws:iam::123456789012:role/my-role"));
  public static final String BASE_LOCATION =
      Optional.ofNullable(System.getenv("INTEGRATION_TEST_S3_PATH")) // Backward compatability
              .orElse(Optional.ofNullable(System.getenv("INTEGRATION_TEST_BASE_LOCATION"))
          .orElse("file:///tmp/buckets/my-bucket"));
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          ConfigOverride.config(
              "server.applicationConnectors[0].port",
              "0"), // Bind to random port to support parallelism
          ConfigOverride.config(
              "server.adminConnectors[0].port", "0")); // Bind to random port to support parallelism

  private RESTCatalog restCatalog;

  @BeforeAll
  public static void setup(@PolarisRealm String realm) throws IOException {
    // Set up test location
    PolarisConnectionExtension.createTestDir(realm);
  }

  @BeforeEach
  public void before(
      TestInfo testInfo,
      PolarisToken adminToken,
      SnowmanCredentials snowmanCredentials,
      @PolarisRealm String realm) {
    String userToken = adminToken.token();
    testInfo
        .getTestMethod()
        .ifPresent(
            method -> {
              String catalogName = method.getName();
              try (Response response =
                  EXT.client()
                      .target(
                          String.format(
                              "http://localhost:%d/api/management/v1/catalogs/%s",
                              EXT.getLocalPort(), catalogName))
                      .request("application/json")
                      .header("Authorization", "Bearer " + userToken)
                      .header(REALM_PROPERTY_KEY, realm)
                      .get()) {
                if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                  // Already exists! Must be in a parameterized test.
                  // Quick hack to get a unique catalogName.
                  // TODO: Have a while-loop instead with consecutive incrementing suffixes.
                  catalogName = catalogName + System.currentTimeMillis();
                }
              }

              String defaultBaseLocation = BASE_LOCATION + "/" + System.getenv("USER") + "/path/to/data";
              StorageConfigInfo storageConfig = TestUtil.buildStorageInfo(defaultBaseLocation, IDENTITY);
              org.apache.polaris.core.admin.model.CatalogProperties props =
                  org.apache.polaris.core.admin.model.CatalogProperties.builder(
                          defaultBaseLocation)
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
                      .setStorageConfigInfo(
                          storageConfig)
                      .build();
              restCatalog =
                  TestUtil.createSnowmanManagedCatalog(
                      EXT, adminToken, snowmanCredentials, realm, catalog, Map.of());
            });
  }

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
