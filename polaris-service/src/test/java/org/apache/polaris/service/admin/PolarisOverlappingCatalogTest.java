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

import static org.assertj.core.api.Assertions.assertThat;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.PolarisApplication;
import org.apache.polaris.service.catalog.PolarisTestClient;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.PolarisRealm;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith({DropwizardExtensionsSupport.class, PolarisConnectionExtension.class})
public class PolarisOverlappingCatalogTest {
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          // Bind to random port to support parallelism
          ConfigOverride.config("server.applicationConnectors[0].port", "0"),
          ConfigOverride.config("server.adminConnectors[0].port", "0"),
          // Block overlapping catalog paths:
          ConfigOverride.config("featureConfiguration.ALLOW_OVERLAPPING_CATALOG_URLS", "false"));
  private static PolarisTestClient userClient;

  @BeforeAll
  public static void setup(
      PolarisConnectionExtension.PolarisToken adminToken, @PolarisRealm String polarisRealm)
      throws IOException {
    String userToken = adminToken.token();
    userClient = new PolarisTestClient(EXT.client(), EXT.getLocalPort(), userToken, polarisRealm);

    // Set up the database location
    PolarisConnectionExtension.createTestDir(polarisRealm);
  }

  private Response createCatalog(String prefix, String defaultBaseLocation, boolean isExternal) {
    return createCatalog(prefix, defaultBaseLocation, isExternal, new ArrayList<String>());
  }

  private Response createCatalog(
      String prefix,
      String defaultBaseLocation,
      boolean isExternal,
      List<String> allowedLocations) {
    String uuid = UUID.randomUUID().toString();
    StorageConfigInfo config =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(
                allowedLocations.stream()
                    .map(
                        l -> {
                          return String.format("s3://bucket/%s/%s", prefix, l);
                        })
                    .toList())
            .build();
    Catalog catalog =
        new Catalog(
            isExternal ? Catalog.TypeEnum.EXTERNAL : Catalog.TypeEnum.INTERNAL,
            String.format("overlap_catalog_%s", uuid),
            new CatalogProperties(String.format("s3://bucket/%s/%s", prefix, defaultBaseLocation)),
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            1,
            config);
    try (Response response = userClient.createCatalog(catalog)) {
      return response;
    }
  }

  @ParameterizedTest
  @CsvSource({"true, true", "true, false", "false, true", "false, false"})
  public void testBasicOverlappingCatalogs(boolean initiallyExternal, boolean laterExternal) {
    String prefix = UUID.randomUUID().toString();

    assertThat(createCatalog(prefix, "root", initiallyExternal))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

    // OK, non-overlapping
    assertThat(createCatalog(prefix, "boot", laterExternal))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

    // OK, non-overlapping due to no `/`
    assertThat(createCatalog(prefix, "roo", laterExternal))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

    // Also OK due to no `/`
    assertThat(createCatalog(prefix, "root.child", laterExternal))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

    // inside `root`
    assertThat(createCatalog(prefix, "root/child", laterExternal))
        .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);

    // `root` is inside this
    assertThat(createCatalog(prefix, "", laterExternal))
        .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
  }

  @ParameterizedTest
  @CsvSource({"true, true", "true, false", "false, true", "false, false"})
  public void testAllowedLocationOverlappingCatalogs(
      boolean initiallyExternal, boolean laterExternal) {
    String prefix = UUID.randomUUID().toString();

    assertThat(createCatalog(prefix, "animals", initiallyExternal, Arrays.asList("dogs", "cats")))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

    // OK, non-overlapping
    assertThat(createCatalog(prefix, "danimals", laterExternal, Arrays.asList("dan", "daniel")))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

    // This DBL overlaps with initial AL
    assertThat(createCatalog(prefix, "dogs", initiallyExternal, Arrays.asList("huskies", "labs")))
        .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);

    // This AL overlaps with initial DBL
    assertThat(
            createCatalog(
                prefix, "kingdoms", initiallyExternal, Arrays.asList("plants", "animals")))
        .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);

    // This AL overlaps with an initial AL
    assertThat(createCatalog(prefix, "plays", initiallyExternal, Arrays.asList("rent", "cats")))
        .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
  }
}
