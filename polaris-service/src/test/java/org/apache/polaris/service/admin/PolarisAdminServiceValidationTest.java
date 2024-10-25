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

import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.PolarisApplication;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.PolarisRealm;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/** Tests for the validations performed in {@link PolarisAdminService}. */
@ExtendWith({DropwizardExtensionsSupport.class, PolarisConnectionExtension.class})
public class PolarisAdminServiceValidationTest {
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT_BLOCK_WASB =
      initExt(ConfigOverride.config("featureConfiguration.SUPPORT_WASB_CATALOG", "false"));
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT_SUPPORT_WASB =
      initExt(ConfigOverride.config("featureConfiguration.SUPPORT_WASB_CATALOG", "true"));

  private static String userToken;
  private static String realm;

  private static DropwizardAppExtension<PolarisApplicationConfig> initExt(
      ConfigOverride... overrides) {
    return new DropwizardAppExtension<>(
        PolarisApplication.class,
        ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
        Stream.concat(
                Stream.of(
                    // Bind to random port to support parallelism
                    ConfigOverride.config("server.applicationConnectors[0].port", "0"),
                    ConfigOverride.config("server.adminConnectors[0].port", "0")),
                Stream.of(overrides))
            .toArray(ConfigOverride[]::new));
  }

  @BeforeAll
  public static void setup(
      PolarisConnectionExtension.PolarisToken adminToken, @PolarisRealm String polarisRealm)
      throws IOException {
    userToken = adminToken.token();
    realm = polarisRealm;

    // Set up the database location
    PolarisConnectionExtension.createTestDir(realm);
  }

  private static Invocation.Builder request(DropwizardAppExtension<PolarisApplicationConfig> ext) {
    return ext.client()
        .target(String.format("http://localhost:%d/api/management/v1/catalogs", ext.getLocalPort()))
        .request("application/json")
        .header("Authorization", "Bearer " + userToken)
        .header(REALM_PROPERTY_KEY, realm);
  }

  private Response createAzureCatalog(
      DropwizardAppExtension<PolarisApplicationConfig> ext,
      String defaultBaseLocation,
      boolean isExternal,
      List<String> allowedLocations) {
    String uuid = UUID.randomUUID().toString();
    StorageConfigInfo config =
        AzureStorageConfigInfo.builder()
            .setTenantId("tenantId")
            .setConsentUrl("https://consentUrl")
            .setMultiTenantAppName("multiTenantAppName")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setAllowedLocations(allowedLocations)
            .build();
    Catalog catalog =
        new Catalog(
            isExternal ? Catalog.TypeEnum.EXTERNAL : Catalog.TypeEnum.INTERNAL,
            String.format("overlap_catalog_%s", uuid),
            new CatalogProperties(defaultBaseLocation),
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            1,
            config);
    try (Response response = request(ext).post(Entity.json(new CreateCatalogRequest(catalog)))) {
      return response;
    }
  }

  @ParameterizedTest
  @ArgumentsSource(AzurePathArgs.class)
  public void testWasbCatalogCreationBlocked(String defaultBaseLocation, String allowedLocation) {
    int expectedStatusCode =
        (defaultBaseLocation.startsWith("wasbs") || allowedLocation.startsWith("wasbs"))
            ? Response.Status.BAD_REQUEST.getStatusCode()
            : Response.Status.CREATED.getStatusCode();

    assertThat(
            createAzureCatalog(
                EXT_BLOCK_WASB,
                defaultBaseLocation,
                false,
                Collections.singletonList(allowedLocation)))
        .returns(expectedStatusCode, Response::getStatus);
    assertThat(
            createAzureCatalog(
                EXT_BLOCK_WASB,
                defaultBaseLocation,
                true,
                Collections.singletonList(allowedLocation)))
        .returns(expectedStatusCode, Response::getStatus);
  }

  @ParameterizedTest
  @ArgumentsSource(AzurePathArgs.class)
  public void testWasbCatalogCreationAllowed(String defaultBaseLocation, String allowedLocation) {
    assertThat(
            createAzureCatalog(
                EXT_SUPPORT_WASB,
                defaultBaseLocation,
                false,
                Collections.singletonList(allowedLocation)))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    assertThat(
            createAzureCatalog(
                EXT_SUPPORT_WASB,
                defaultBaseLocation,
                true,
                Collections.singletonList(allowedLocation)))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
  }

  private static class AzurePathArgs implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
      return Stream.of(createArguments(), createArguments(), createArguments(), createArguments());
    }

    private Arguments createArguments() {
      String prefix = UUID.randomUUID().toString();
      String wasbPath = String.format("wasbs://container@acct.blob.windows.net/%s/wasb", prefix);
      String abfsPath = String.format("abfss://container@acct.blob.windows.net/%s/abfs", prefix);
      return Arguments.of(wasbPath, abfsPath);
    }
  }
}
