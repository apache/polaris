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

import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION;
import static org.apache.polaris.core.config.FeatureConfiguration.DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED;
import static org.apache.polaris.core.config.FeatureConfiguration.OPTIMIZED_SIBLING_CHECK;
import static org.apache.polaris.service.quarkus.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.common.LocationUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PolarisOverlappingTableTest {

  private static final String namespace = "ns";
  private static final String catalog = "test-catalog";

  private String getTableName() {
    return "table_" + UUID.randomUUID();
  }

  /** Attempt to create a table at a given location, and return the response code */
  private int createTable(TestServices services, String location) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder()
            .withName(getTableName())
            .withLocation(location)
            .withSchema(SCHEMA)
            .build();
    try (Response response =
        services
            .restApi()
            .createTable(
                catalog,
                namespace,
                createTableRequest,
                null,
                services.realmContext(),
                services.securityContext())) {
      return response.getStatus();
    } catch (ForbiddenException e) {
      return Response.Status.FORBIDDEN.getStatusCode();
    }
  }

  /**
   * Attempt to create a table without a location, and return the location it gets created at If the
   * creation fails, this should return null
   */
  private String createTableWithName(TestServices services, String name) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder().withName(name).withSchema(SCHEMA).build();
    try (Response response =
        services
            .restApi()
            .createTable(
                catalog,
                namespace,
                createTableRequest,
                null,
                services.realmContext(),
                services.securityContext())) {
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        return null;
      } else {
        return response.readEntity(LoadTableResponse.class).tableMetadata().location();
      }
    } catch (ForbiddenException e) {
      return null;
    }
  }

  private void createCatalogAndNamespace(
      TestServices services, Map<String, String> catalogConfig, String catalogLocation) {
    CatalogProperties.Builder propertiesBuilder =
        CatalogProperties.builder()
            .setDefaultBaseLocation(String.format("%s/%s", catalogLocation, catalog))
            .putAll(catalogConfig);

    StorageConfigInfo config =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .build();
    Catalog catalogObject =
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            catalog,
            propertiesBuilder.build(),
            1725487592064L,
            1725487592064L,
            1,
            config);
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalogObject),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }

    CreateNamespaceRequest createNamespaceRequest =
        CreateNamespaceRequest.builder().withNamespace(Namespace.of(namespace)).build();
    try (Response response =
        services
            .restApi()
            .createNamespace(
                catalog,
                createNamespaceRequest,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  static Stream<Arguments> testTableLocationRestrictions() {
    Map<String, Object> laxServices =
        Map.of(
            "ALLOW_UNSTRUCTURED_TABLE_LOCATION",
            "true",
            "ALLOW_TABLE_LOCATION_OVERLAP",
            "true",
            "ALLOW_INSECURE_STORAGE_TYPES",
            "true",
            "SUPPORTED_CATALOG_STORAGE_TYPES",
            List.of("FILE", "S3"));
    Map<String, Object> strictServices =
        Map.of(
            "ALLOW_UNSTRUCTURED_TABLE_LOCATION",
            "false",
            "ALLOW_TABLE_LOCATION_OVERLAP",
            "false",
            "ALLOW_INSECURE_STORAGE_TYPES",
            "true",
            "SUPPORTED_CATALOG_STORAGE_TYPES",
            List.of("FILE", "S3"));
    Map<String, Object> laxCatalog =
        Map.of(
            ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
            "true",
            ALLOW_TABLE_LOCATION_OVERLAP.catalogConfig(),
            "true");
    Map<String, Object> strictCatalog =
        Map.of(
            ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
            "false",
            ALLOW_TABLE_LOCATION_OVERLAP.catalogConfig(),
            "false");
    return Stream.of(
        Arguments.of(strictServices, Map.of(), Response.Status.FORBIDDEN.getStatusCode()),
        Arguments.of(strictServices, strictCatalog, Response.Status.FORBIDDEN.getStatusCode()),
        Arguments.of(strictServices, laxCatalog, Response.Status.OK.getStatusCode()),
        Arguments.of(laxServices, Map.of(), Response.Status.OK.getStatusCode()),
        Arguments.of(laxServices, strictCatalog, Response.Status.FORBIDDEN.getStatusCode()),
        Arguments.of(laxServices, laxCatalog, Response.Status.OK.getStatusCode()));
  }

  @ParameterizedTest
  @MethodSource()
  @DisplayName("Test restrictions on table locations")
  void testTableLocationRestrictions(
      Map<String, Object> serverConfig,
      Map<String, String> catalogConfig,
      int expectedStatusForOverlaps,
      @TempDir Path tempDir) {
    TestServices services = TestServices.builder().config(serverConfig).build();

    String baseLocation = tempDir.toAbsolutePath().toUri().toString();
    if (baseLocation.endsWith("/")) {
      baseLocation = baseLocation.substring(0, baseLocation.length() - 1);
    }
    createCatalogAndNamespace(services, catalogConfig, baseLocation);

    // Original table
    assertThat(
            createTable(
                services, String.format("%s/%s/%s/table_1", baseLocation, catalog, namespace)))
        .isEqualTo(Response.Status.OK.getStatusCode());

    // Unrelated path
    assertThat(
            createTable(
                services, String.format("%s/%s/%s/table_2", baseLocation, catalog, namespace)))
        .isEqualTo(Response.Status.OK.getStatusCode());

    // Trailing slash makes this not overlap with table_1
    assertThat(
            createTable(
                services, String.format("%s/%s/%s/table_100", baseLocation, catalog, namespace)))
        .isEqualTo(Response.Status.OK.getStatusCode());

    // Repeat location
    assertThat(
            createTable(
                services, String.format("%s/%s/%s/table_100", baseLocation, catalog, namespace)))
        .isEqualTo(expectedStatusForOverlaps);

    // Parent of existing location
    assertThat(createTable(services, String.format("%s/%s/%s", baseLocation, catalog, namespace)))
        .isEqualTo(expectedStatusForOverlaps);

    // Child of existing location
    assertThat(
            createTable(
                services,
                String.format("%s/%s/%s/table_100/child", baseLocation, catalog, namespace)))
        .isEqualTo(expectedStatusForOverlaps);

    // Outside the namespace
    assertThat(createTable(services, String.format("%s/%s", baseLocation, catalog)))
        .isEqualTo(expectedStatusForOverlaps);

    // Outside the catalog
    assertThat(createTable(services, String.format("%s", baseLocation)))
        .isEqualTo(Response.Status.FORBIDDEN.getStatusCode());
  }

  static Stream<Arguments> testStandardTableLocations() {
    Map<String, Object> noPrefixCatalog =
        Map.of(
            ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
            "true",
            ALLOW_TABLE_LOCATION_OVERLAP.catalogConfig(),
            "false",
            DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED.catalogConfig(),
            "false");
    return Stream.of(Arguments.of(Map.of()), Arguments.of(noPrefixCatalog));
  }

  @ParameterizedTest
  @MethodSource()
  @DisplayName("Test tables getting created at standard locations")
  void testStandardTableLocations(Map<String, String> catalogConfig, @TempDir Path tempDir) {
    Map<String, Object> strictServices =
        Map.of(
            "ALLOW_UNSTRUCTURED_TABLE_LOCATION",
            "false",
            "ALLOW_TABLE_LOCATION_OVERLAP",
            "false",
            "ALLOW_INSECURE_STORAGE_TYPES",
            "true",
            "SUPPORTED_CATALOG_STORAGE_TYPES",
            List.of("FILE", "S3"));

    TestServices services = TestServices.builder().config(strictServices).build();

    String baseLocation = tempDir.toAbsolutePath().toUri().toString();
    if (baseLocation.endsWith("/")) {
      baseLocation = baseLocation.substring(0, baseLocation.length() - 1);
    }
    createCatalogAndNamespace(services, catalogConfig, baseLocation);

    String tableName;

    tableName = getTableName();
    Assertions.assertEquals(
        String.format("%s/%s/%s/%s", baseLocation, catalog, namespace, tableName),
        createTableWithName(services, tableName));

    // Overlap fails:
    assertThat(
            createTable(
                services,
                String.format("%s/%s/%s/%s", baseLocation, catalog, namespace, tableName)))
        .isEqualTo(Response.Status.FORBIDDEN.getStatusCode());
  }

  static Stream<Arguments> testInvalidSetupsForObjectStorageLocation() {
    Map<String, Object> prefixAndNoOverlapCatalog =
        Map.of(
            DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED.catalogConfig(),
            "true",
            ALLOW_TABLE_LOCATION_OVERLAP.catalogConfig(),
            "false");
    Map<String, Object> prefixAndOverlapButNoOptimizedCatalog =
        Map.of(
            DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED.catalogConfig(),
            "true",
            ALLOW_TABLE_LOCATION_OVERLAP.catalogConfig(),
            "true");
    return Stream.of(
        Arguments.of(prefixAndNoOverlapCatalog),
        Arguments.of(prefixAndOverlapButNoOptimizedCatalog));
  }

  @ParameterizedTest
  @MethodSource()
  @DisplayName("Test invalid configurations for enabling prefixed locations")
  void testInvalidSetupsForObjectStorageLocation(
      Map<String, String> catalogConfig, @TempDir Path tempDir) {
    Map<String, Object> strictServicesNoOptimizedOverlapCheck =
        Map.of(
            "ALLOW_UNSTRUCTURED_TABLE_LOCATION",
            "false",
            "ALLOW_TABLE_LOCATION_OVERLAP",
            "false",
            "ALLOW_INSECURE_STORAGE_TYPES",
            "true",
            "SUPPORTED_CATALOG_STORAGE_TYPES",
            List.of("FILE", "S3"),
            OPTIMIZED_SIBLING_CHECK.key(),
            "false");

    TestServices services =
        TestServices.builder().config(strictServicesNoOptimizedOverlapCheck).build();

    String baseLocation = tempDir.toAbsolutePath().toUri().toString();
    if (baseLocation.endsWith("/")) {
      baseLocation = baseLocation.substring(0, baseLocation.length() - 1);
    }
    createCatalogAndNamespace(services, catalogConfig, baseLocation);

    Assertions.assertThrows(
        IllegalStateException.class, () -> createTableWithName(services, getTableName()));
  }

  @Test
  @DisplayName("Test tables getting created at locations with a hash prefix")
  public void testHashedTableLocations(@TempDir Path tempDir) {
    Map<String, Object> strictServicesWithOptimizedOverlapCheck =
        Map.of(
            "ALLOW_UNSTRUCTURED_TABLE_LOCATION",
            "false",
            "ALLOW_TABLE_LOCATION_OVERLAP",
            "false",
            "ALLOW_INSECURE_STORAGE_TYPES",
            "true",
            "SUPPORTED_CATALOG_STORAGE_TYPES",
            List.of("FILE", "S3"),
            OPTIMIZED_SIBLING_CHECK.key(),
            "true");
    Map<String, String> hashedAndOverlapButNoOptimizedCatalog =
        Map.of(
            DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED.catalogConfig(),
            "true",
            ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
            "true");

    TestServices services =
        TestServices.builder().config(strictServicesWithOptimizedOverlapCheck).build();

    String baseLocation = tempDir.toAbsolutePath().toUri().toString();
    if (baseLocation.endsWith("/")) {
      baseLocation = baseLocation.substring(0, baseLocation.length() - 1);
    }
    createCatalogAndNamespace(services, hashedAndOverlapButNoOptimizedCatalog, baseLocation);

    String tableName;
    String tableLocation;

    // Location check works:
    tableName = getTableName();
    Assertions.assertNotNull(createTableWithName(services, tableName));

    // Non-default pattern:
    tableName = getTableName();
    Assertions.assertNotEquals(
        String.format("%s/%s/%s/%s", baseLocation, catalog, namespace, tableName),
        createTableWithName(services, tableName));

    // Verify components:
    tableName = getTableName();
    tableLocation = createTableWithName(services, tableName);
    Assertions.assertEquals(
        String.format("%s/%s/", baseLocation, catalog),
        tableLocation.substring(0, String.format("%s/%s/", baseLocation, catalog).length()));
    Assertions.assertEquals(
        String.format("%s/%s", namespace, tableName),
        tableLocation.substring(
            String.format("%s/%s/", baseLocation, catalog).length()
                + (LocationUtils.HASH_BINARY_STRING_BITS + LocationUtils.ENTROPY_DIR_LENGTH)));

    // Overlap fails:
    assertThat(createTable(services, tableLocation))
        .isEqualTo(Response.Status.FORBIDDEN.getStatusCode());

    // The hashed prefix does not actually have to be stable, so this test
    // is okay to change in the future.
    assertThat(createTableWithName(services, "determinism_check").substring(baseLocation.length()))
        .isEqualTo("/test-catalog/1110/1010/0001/01111010/ns/determinism_check");
  }
}
