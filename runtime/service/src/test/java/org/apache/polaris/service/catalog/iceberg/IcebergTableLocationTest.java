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
package org.apache.polaris.service.catalog.iceberg;

import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_CLIENT_SPECIFIED_TABLE_LOCATION;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION;
import static org.apache.polaris.core.config.FeatureConfiguration.DEFAULT_UNIQUE_TABLE_LOCATION_ENABLED;
import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.util.LocationUtil;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for the unique default table location feature ({@link
 * org.apache.polaris.core.config.FeatureConfiguration#DEFAULT_UNIQUE_TABLE_LOCATION_ENABLED}) and
 * the opt-in for caller-specified locations ({@link
 * org.apache.polaris.core.config.FeatureConfiguration#ALLOW_CLIENT_SPECIFIED_TABLE_LOCATION}).
 */
public class IcebergTableLocationTest {

  private static final String NAMESPACE = "ns";
  private static final String CATALOG = "test-catalog";
  // UUID v7
  private static final UUID IDEMPOTENCY_KEY = new UUID(116617318654508422L, -7820829973016961092L);

  private static final Map<String, Object> FILE_STORAGE_CONFIG =
      Map.of(
          "ALLOW_INSECURE_STORAGE_TYPES",
          "true",
          "SUPPORTED_CATALOG_STORAGE_TYPES",
          List.of("FILE", "S3"));

  private String getTableName() {
    return "table_" + UUID.randomUUID();
  }

  /**
   * Builds a server config of the shared file-storage settings plus the given feature overrides.
   */
  private static Map<String, Object> serverConfig(String... featureOverrides) {
    ImmutableMap.Builder<String, Object> builder =
        ImmutableMap.<String, Object>builder().putAll(FILE_STORAGE_CONFIG);
    for (int i = 0; i < featureOverrides.length; i += 2) {
      builder.put(featureOverrides[i], featureOverrides[i + 1]);
    }
    return builder.build();
  }

  private void createCatalogAndNamespace(
      TestServices services, Map<String, String> catalogConfig, String catalogLocation) {
    CatalogProperties.Builder propertiesBuilder =
        CatalogProperties.builder()
            .setDefaultBaseLocation(String.format("%s/%s", catalogLocation, CATALOG))
            .putAll(catalogConfig);
    StorageConfigInfo config =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .build();
    Catalog catalogObject =
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            CATALOG,
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
        CreateNamespaceRequest.builder().withNamespace(Namespace.of(NAMESPACE)).build();
    try (Response response =
        services
            .restApi()
            .createNamespace(
                CATALOG,
                createNamespaceRequest,
                IDEMPOTENCY_KEY,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  /** Creates a table without a caller-specified location and returns the location it was given. */
  private String createTableWithName(TestServices services, String name) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder().withName(name).withSchema(SCHEMA).build();
    try (Response response =
        services
            .restApi()
            .createTable(
                CATALOG,
                NAMESPACE,
                createTableRequest,
                null,
                IDEMPOTENCY_KEY,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return response.readEntity(LoadTableResponse.class).tableMetadata().location();
    }
  }

  private Response.StatusType createTableWithLocation(TestServices services, String location) {
    return submitCreateTable(
        services,
        CreateTableRequest.builder()
            .withName(getTableName())
            .withLocation(location)
            .withSchema(SCHEMA)
            .build());
  }

  /** Creates a table with no top-level location but the given property set. */
  private Response.StatusType createTableWithProperty(
      TestServices services, String propertyKey, String propertyValue) {
    return submitCreateTable(
        services,
        CreateTableRequest.builder()
            .withName(getTableName())
            .withSchema(SCHEMA)
            .setProperty(propertyKey, propertyValue)
            .build());
  }

  private Response.StatusType submitCreateTable(TestServices services, CreateTableRequest request) {
    try (Response response =
        services
            .restApi()
            .createTable(
                CATALOG,
                NAMESPACE,
                request,
                null,
                IDEMPOTENCY_KEY,
                services.realmContext(),
                services.securityContext())) {
      return response.getStatusInfo();
    }
  }

  @Test
  @DisplayName("Default table location has a unique suffix when the feature is enabled")
  void testDefaultLocationHasUniqueSuffix(@TempDir Path tempDir) {
    TestServices services =
        TestServices.builder()
            .config(serverConfig(DEFAULT_UNIQUE_TABLE_LOCATION_ENABLED.key(), "true"))
            .build();
    String baseLocation =
        LocationUtil.stripTrailingSlash(tempDir.toAbsolutePath().toUri().toString());
    createCatalogAndNamespace(services, Map.of(), baseLocation);

    String tableName = getTableName();
    String location = createTableWithName(services, tableName);

    String expectedPrefix =
        String.format("%s/%s/%s/%s-", baseLocation, CATALOG, NAMESPACE, tableName);
    assertThat(location).startsWith(expectedPrefix);
    String suffix = location.substring(expectedPrefix.length());
    // The suffix is a unique, unpredictable generated identifier.
    assertThat(UUID.fromString(suffix)).isNotNull();
  }

  @Test
  @DisplayName("Sibling tables with adjacent names get non-overlapping unique locations")
  void testSiblingTablesDoNotOverlap(@TempDir Path tempDir) {
    TestServices services =
        TestServices.builder()
            .config(serverConfig(DEFAULT_UNIQUE_TABLE_LOCATION_ENABLED.key(), "true"))
            .build();
    String baseLocation =
        LocationUtil.stripTrailingSlash(tempDir.toAbsolutePath().toUri().toString());
    createCatalogAndNamespace(services, Map.of(), baseLocation);

    // "t1" and "t1a" are adjacent by name and would collide under prefix matching without a unique
    // suffix.
    String loc1 = LocationUtil.stripTrailingSlash(createTableWithName(services, "t1"));
    String loc2 = LocationUtil.stripTrailingSlash(createTableWithName(services, "t1a"));

    assertThat(loc1).isNotEqualTo(loc2);
    assertThat(loc1 + "/").doesNotStartWith(loc2 + "/");
    assertThat(loc2 + "/").doesNotStartWith(loc1 + "/");
  }

  @Test
  @DisplayName("Caller-specified location is rejected when the opt-in is disabled")
  void testClientSpecifiedLocationRejectedWhenDisabled(@TempDir Path tempDir) {
    TestServices services =
        TestServices.builder()
            .config(serverConfig(ALLOW_CLIENT_SPECIFIED_TABLE_LOCATION.key(), "false"))
            .build();
    String baseLocation =
        LocationUtil.stripTrailingSlash(tempDir.toAbsolutePath().toUri().toString());
    createCatalogAndNamespace(services, Map.of(), baseLocation);

    String someLocation =
        String.format("%s/%s/%s/caller-location", baseLocation, CATALOG, NAMESPACE);
    assertThatThrownBy(() -> createTableWithLocation(services, someLocation))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining(ALLOW_CLIENT_SPECIFIED_TABLE_LOCATION.catalogConfig());
  }

  @Test
  @DisplayName("Caller-specified write.data.path / write.metadata.path are rejected when disabled")
  void testClientSpecifiedWritePathsRejectedWhenDisabled(@TempDir Path tempDir) {
    TestServices services =
        TestServices.builder()
            .config(serverConfig(ALLOW_CLIENT_SPECIFIED_TABLE_LOCATION.key(), "false"))
            .build();
    String baseLocation =
        LocationUtil.stripTrailingSlash(tempDir.toAbsolutePath().toUri().toString());
    createCatalogAndNamespace(services, Map.of(), baseLocation);

    String dataPath = String.format("%s/%s/%s/custom-data", baseLocation, CATALOG, NAMESPACE);
    assertThatThrownBy(
            () ->
                createTableWithProperty(
                    services,
                    IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
                    dataPath))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY);

    String metadataPath = String.format("%s/%s/%s/custom-meta", baseLocation, CATALOG, NAMESPACE);
    assertThatThrownBy(
            () ->
                createTableWithProperty(
                    services,
                    IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY,
                    metadataPath))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY);
  }

  @Test
  @DisplayName("Caller-specified location and write paths are rejected on table update by default")
  void testClientSpecifiedLocationRejectedOnUpdate(@TempDir Path tempDir) {
    TestServices services =
        TestServices.builder()
            .config(serverConfig(ALLOW_CLIENT_SPECIFIED_TABLE_LOCATION.key(), "false"))
            .build();
    String baseLocation =
        LocationUtil.stripTrailingSlash(tempDir.toAbsolutePath().toUri().toString());
    createCatalogAndNamespace(services, Map.of(), baseLocation);

    // Create a table with no caller-specified location (allowed even when the flag is off).
    String tableName = getTableName();
    createTableWithName(services, tableName);
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), tableName);
    String nsPrefix = String.format("%s/%s/%s", baseLocation, CATALOG, NAMESPACE);

    // Setting write.metadata.path on an existing table is rejected.
    UpdateTableRequest setWriteMetadata =
        UpdateTableRequest.create(
            tableId,
            List.of(),
            List.of(
                new MetadataUpdate.SetProperties(
                    Map.of(
                        IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY,
                        nsPrefix + "/elsewhere"))));
    assertThatThrownBy(
            () ->
                services
                    .catalogAdapter()
                    .newHandler(services.securityContext(), CATALOG)
                    .updateTable(tableId, setWriteMetadata))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY);

    // Changing the base location via SetLocation is rejected.
    UpdateTableRequest setLocation =
        UpdateTableRequest.create(
            tableId, List.of(), List.of(new MetadataUpdate.SetLocation(nsPrefix + "/moved")));
    assertThatThrownBy(
            () ->
                services
                    .catalogAdapter()
                    .newHandler(services.securityContext(), CATALOG)
                    .updateTable(tableId, setLocation))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining(ALLOW_CLIENT_SPECIFIED_TABLE_LOCATION.catalogConfig());
  }

  @Test
  @DisplayName("Caller-specified write paths are rejected in a multi-table commitTransaction")
  void testClientSpecifiedLocationRejectedInCommitTransaction(@TempDir Path tempDir) {
    TestServices services =
        TestServices.builder()
            .config(serverConfig(ALLOW_CLIENT_SPECIFIED_TABLE_LOCATION.key(), "false"))
            .build();
    String baseLocation =
        LocationUtil.stripTrailingSlash(tempDir.toAbsolutePath().toUri().toString());
    createCatalogAndNamespace(services, Map.of(), baseLocation);

    String tableName = getTableName();
    createTableWithName(services, tableName);
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), tableName);

    // A multi-table transaction must not bypass the gate that updateTable enforces.
    CommitTransactionRequest commit =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(
                    tableId,
                    List.of(),
                    List.of(
                        new MetadataUpdate.SetProperties(
                            Map.of(
                                IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY,
                                String.format(
                                    "%s/%s/%s/elsewhere", baseLocation, CATALOG, NAMESPACE)))))));
    assertThatThrownBy(
            () ->
                services
                    .restApi()
                    .commitTransaction(
                        CATALOG,
                        commit,
                        IDEMPOTENCY_KEY,
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY);
  }

  @Test
  @DisplayName("Caller-specified location is honored when the opt-in is enabled")
  void testClientSpecifiedLocationHonoredWhenEnabled(@TempDir Path tempDir) {
    TestServices services =
        TestServices.builder()
            .config(
                serverConfig(
                    ALLOW_CLIENT_SPECIFIED_TABLE_LOCATION.key(), "true",
                    ALLOW_UNSTRUCTURED_TABLE_LOCATION.key(), "true"))
            .build();
    String baseLocation =
        LocationUtil.stripTrailingSlash(tempDir.toAbsolutePath().toUri().toString());
    createCatalogAndNamespace(services, Map.of(), baseLocation);

    String someLocation =
        String.format("%s/%s/%s/caller-location", baseLocation, CATALOG, NAMESPACE);
    assertThat(createTableWithLocation(services, someLocation).getStatusCode())
        .isEqualTo(Response.Status.OK.getStatusCode());
  }

  @Test
  @DisplayName("Default table location is the legacy form when the feature is disabled")
  void testLegacyLocationWhenDisabled(@TempDir Path tempDir) {
    TestServices services =
        TestServices.builder()
            .config(serverConfig(DEFAULT_UNIQUE_TABLE_LOCATION_ENABLED.key(), "false"))
            .build();
    String baseLocation =
        LocationUtil.stripTrailingSlash(tempDir.toAbsolutePath().toUri().toString());
    createCatalogAndNamespace(services, Map.of(), baseLocation);

    String tableName = getTableName();
    assertThat(createTableWithName(services, tableName))
        .isEqualTo(String.format("%s/%s/%s/%s", baseLocation, CATALOG, NAMESPACE, tableName));
  }
}
