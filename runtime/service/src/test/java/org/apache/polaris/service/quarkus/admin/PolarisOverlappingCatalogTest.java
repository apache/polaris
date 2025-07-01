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
package org.apache.polaris.service.quarkus.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class PolarisOverlappingCatalogTest {

  static TestServices services =
      TestServices.builder().config(Map.of("ALLOW_OVERLAPPING_CATALOG_URLS", "false")).build();

  private Response createCatalog(String prefix, String defaultBaseLocation, boolean isExternal) {
    return createCatalog("s3", prefix, defaultBaseLocation, isExternal, new ArrayList<String>());
  }

  private Response createCatalog(
      String s3Scheme, String prefix, String defaultBaseLocation, boolean isExternal) {
    return createCatalog(
        s3Scheme, prefix, defaultBaseLocation, isExternal, new ArrayList<String>());
  }

  private Response createCatalog(
      String prefix,
      String defaultBaseLocation,
      boolean isExternal,
      List<String> allowedLocations) {
    return createCatalog("s3", prefix, defaultBaseLocation, isExternal, allowedLocations);
  }

  private Response createCatalog(
      String s3Scheme,
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
                          return String.format(s3Scheme + "://bucket/%s/%s", prefix, l);
                        })
                    .toList())
            .build();
    Catalog catalog =
        new Catalog(
            isExternal ? Catalog.TypeEnum.EXTERNAL : Catalog.TypeEnum.INTERNAL,
            String.format("overlap_catalog_%s", uuid),
            new CatalogProperties(
                String.format(s3Scheme + "://bucket/%s/%s", prefix, defaultBaseLocation)),
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            1,
            config);
    return services
        .catalogsApi()
        .createCatalog(
            new CreateCatalogRequest(catalog), services.realmContext(), services.securityContext());
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
    assertThatThrownBy(() -> createCatalog(prefix, "root/child", laterExternal))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("One or more of its locations overlaps with an existing catalog");

    // `root` is inside this
    assertThatThrownBy(() -> createCatalog(prefix, "", laterExternal))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("One or more of its locations overlaps with an existing catalog");
  }

  @ParameterizedTest
  @CsvSource({"s3,s3a", "s3a,s3"})
  public void testBasicOverlappingCatalogWSchemeChange(String rootScheme, String overlapScheme) {
    String prefix = UUID.randomUUID().toString();

    assertThat(createCatalog(rootScheme, prefix, "root", false))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

    // - inside `root` but using different scheme
    assertThatThrownBy(() -> createCatalog(overlapScheme, prefix, "root/child", false))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("One or more of its locations overlaps with an existing catalog");
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
    assertThatThrownBy(
            () ->
                createCatalog(prefix, "dogs", initiallyExternal, Arrays.asList("huskies", "labs")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("One or more of its locations overlaps with an existing catalog");

    // This AL overlaps with initial DBL
    assertThatThrownBy(
            () ->
                createCatalog(
                    prefix, "kingdoms", initiallyExternal, Arrays.asList("plants", "animals")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("One or more of its locations overlaps with an existing catalog");

    // This AL overlaps with an initial AL
    assertThatThrownBy(
            () -> createCatalog(prefix, "plays", initiallyExternal, Arrays.asList("rent", "cats")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("One or more of its locations overlaps with an existing catalog");
  }

  @ParameterizedTest
  @CsvSource({"s3,s3a", "s3a,s3"})
  public void testAllowedLocationOverlappingCatalogsWSchemeChange(
      String rootScheme, String overlapScheme) {
    String prefix = UUID.randomUUID().toString();

    assertThat(createCatalog(rootScheme, prefix, "animals", false, Arrays.asList("dogs", "cats")))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

    // This DBL overlaps with initial AL
    assertThatThrownBy(
            () ->
                createCatalog(
                    overlapScheme, prefix, "dogs", false, Arrays.asList("huskies", "labs")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("One or more of its locations overlaps with an existing catalog");

    // This AL overlaps with initial DBL
    assertThatThrownBy(
            () ->
                createCatalog(
                    overlapScheme, prefix, "kingdoms", false, Arrays.asList("plants", "animals")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("One or more of its locations overlaps with an existing catalog");

    // This AL overlaps with an initial AL
    assertThatThrownBy(
            () ->
                createCatalog(overlapScheme, prefix, "plays", false, Arrays.asList("rent", "cats")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("One or more of its locations overlaps with an existing catalog");
  }
}
