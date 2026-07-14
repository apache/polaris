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
import static org.apache.polaris.core.config.FeatureConfiguration.DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED;
import static org.apache.polaris.core.config.FeatureConfiguration.DEFAULT_UNIQUE_TABLE_LOCATION_ENABLED;
import static org.apache.polaris.core.config.FeatureConfiguration.OPTIMIZED_SIBLING_CHECK;
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
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.common.LocationUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for the unique default table location feature ({@link
 * org.apache.polaris.core.config.FeatureConfiguration#DEFAULT_UNIQUE_TABLE_LOCATION_ENABLED}) and
 * the gate for caller-specified locations ({@link
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

  /** Stages a table without a caller-specified location and returns its generated location. */
  private String stageTableWithName(TestServices services, String name) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder().withName(name).withSchema(SCHEMA).stageCreate().build();
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

  /** Creates a view without a caller-specified location and returns its generated location. */
  private String createViewWithName(TestServices services, String name) {
    CreateViewRequest createViewRequest = createViewRequest(name, null);
    try (Response response =
        services
            .restApi()
            .createView(
                CATALOG,
                NAMESPACE,
                createViewRequest,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return response.readEntity(LoadViewResponse.class).metadata().location();
    }
  }

  private CreateViewRequest createViewRequest(String name, String location) {
    ImmutableCreateViewRequest.Builder builder =
        ImmutableCreateViewRequest.builder()
            .name(name)
            .schema(SCHEMA)
            .viewVersion(
                ImmutableViewVersion.builder()
                    .versionId(1)
                    .timestampMillis(System.currentTimeMillis())
                    .schemaId(SCHEMA.schemaId())
                    .defaultNamespace(Namespace.of(NAMESPACE))
                    .addRepresentations(
                        ImmutableSQLViewRepresentation.builder()
                            .sql("SELECT 1")
                            .dialect("spark")
                            .build())
                    .build());
    if (location != null) {
      builder.location(location);
    }
    return builder.build();
  }

  private static void assertObjectStorageLocationFormat(
      String location, String baseLocation, String name) {
    String normalizedLocation = LocationUtil.stripTrailingSlash(location);
    TableIdentifier identifier = TableIdentifier.of(Namespace.of(NAMESPACE), name);
    String expectedPrefix =
        String.format(
            "%s/%s/%s/%s/%s-",
            baseLocation,
            CATALOG,
            LocationUtils.computeHash(identifier.toString()),
            NAMESPACE,
            name);
    assertThat(normalizedLocation).startsWith(expectedPrefix);
    assertThat(normalizedLocation.substring(expectedPrefix.length())).matches("[0-9a-f]{32}");
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
  @DisplayName("Recreated tables get different random location suffixes")
  void testDefaultLocationHasUniqueSuffix(@TempDir Path tempDir) {
    TestServices services =
        TestServices.builder()
            .config(serverConfig(DEFAULT_UNIQUE_TABLE_LOCATION_ENABLED.key(), "true"))
            .build();
    String baseLocation =
        LocationUtil.stripTrailingSlash(tempDir.toAbsolutePath().toUri().toString());
    createCatalogAndNamespace(services, Map.of(), baseLocation);

    String tableName = getTableName();
    String expectedPrefix =
        String.format("%s/%s/%s/%s-", baseLocation, CATALOG, NAMESPACE, tableName);
    String firstLocation = createTableWithName(services, tableName);
    assertThat(firstLocation).startsWith(expectedPrefix);
    String firstSuffix = firstLocation.substring(expectedPrefix.length());
    assertThat(firstSuffix).matches("[0-9a-f]{32}");

    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), tableName);
    services
        .catalogAdapter()
        .newHandler(services.securityContext(), CATALOG)
        .dropTableWithoutPurge(tableIdentifier);

    String secondLocation = createTableWithName(services, tableName);
    assertThat(secondLocation).startsWith(expectedPrefix);
    String secondSuffix = secondLocation.substring(expectedPrefix.length());
    assertThat(secondSuffix).matches("[0-9a-f]{32}").isNotEqualTo(firstSuffix);
  }

  @Test
  @DisplayName("Unique and object-storage-prefix default locations compose")
  void testUniqueAndObjectStoragePrefixLocationsCompose(@TempDir Path tempDir) {
    TestServices services =
        TestServices.builder()
            .config(
                serverConfig(
                    DEFAULT_UNIQUE_TABLE_LOCATION_ENABLED.key(), "true",
                    OPTIMIZED_SIBLING_CHECK.key(), "true"))
            .build();
    String baseLocation =
        LocationUtil.stripTrailingSlash(tempDir.toAbsolutePath().toUri().toString());
    createCatalogAndNamespace(
        services,
        Map.of(
            DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED.catalogConfig(), "true",
            ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "true"),
        baseLocation);

    String directTableName = getTableName();
    String firstLocation = createTableWithName(services, directTableName);
    assertObjectStorageLocationFormat(firstLocation, baseLocation, directTableName);

    TableIdentifier directTableIdentifier =
        TableIdentifier.of(Namespace.of(NAMESPACE), directTableName);
    services
        .catalogAdapter()
        .newHandler(services.securityContext(), CATALOG)
        .dropTableWithoutPurge(directTableIdentifier);
    String secondLocation = createTableWithName(services, directTableName);
    assertObjectStorageLocationFormat(secondLocation, baseLocation, directTableName);
    assertThat(secondLocation).isNotEqualTo(firstLocation);

    String stagedTableName = getTableName();
    assertObjectStorageLocationFormat(
        stageTableWithName(services, stagedTableName), baseLocation, stagedTableName);

    String viewName = "view_" + UUID.randomUUID();
    assertObjectStorageLocationFormat(
        createViewWithName(services, viewName), baseLocation, viewName);
  }

  @Test
  @DisplayName("Caller-specified location is rejected when the location gate is disabled")
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
        .hasMessageContaining("the location field");
  }

  @Test
  @DisplayName("Caller-specified location is rejected for staged creates when disabled")
  void testClientSpecifiedLocationRejectedForStagedCreate(@TempDir Path tempDir) {
    TestServices services =
        TestServices.builder()
            .config(serverConfig(ALLOW_CLIENT_SPECIFIED_TABLE_LOCATION.key(), "false"))
            .build();
    String baseLocation =
        LocationUtil.stripTrailingSlash(tempDir.toAbsolutePath().toUri().toString());
    createCatalogAndNamespace(services, Map.of(), baseLocation);

    String someLocation =
        String.format("%s/%s/%s/staged-caller-location", baseLocation, CATALOG, NAMESPACE);
    CreateTableRequest request =
        CreateTableRequest.builder()
            .withName(getTableName())
            .withLocation(someLocation)
            .withSchema(SCHEMA)
            .stageCreate()
            .build();
    assertThatThrownBy(() -> submitCreateTable(services, request))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("the location field");
  }

  @Test
  @DisplayName("Caller-specified locations are rejected for view create and replace when disabled")
  void testClientSpecifiedLocationRejectedForViews(@TempDir Path tempDir) {
    TestServices services =
        TestServices.builder()
            .config(serverConfig(ALLOW_CLIENT_SPECIFIED_TABLE_LOCATION.key(), "false"))
            .build();
    String baseLocation =
        LocationUtil.stripTrailingSlash(tempDir.toAbsolutePath().toUri().toString());
    createCatalogAndNamespace(services, Map.of(), baseLocation);

    String someLocation =
        String.format("%s/%s/%s/view-caller-location", baseLocation, CATALOG, NAMESPACE);
    assertThatThrownBy(
            () ->
                services
                    .restApi()
                    .createView(
                        CATALOG,
                        NAMESPACE,
                        createViewRequest("view_" + UUID.randomUUID(), someLocation),
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("the location field");

    String viewName = "view_" + UUID.randomUUID();
    createViewWithName(services, viewName);
    TableIdentifier viewIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), viewName);
    UpdateTableRequest setLocation =
        UpdateTableRequest.create(
            viewIdentifier,
            List.of(),
            List.of(new MetadataUpdate.SetLocation(someLocation + "-replacement")));
    assertThatThrownBy(
            () ->
                services
                    .catalogAdapter()
                    .newHandler(services.securityContext(), CATALOG)
                    .replaceView(viewIdentifier, setLocation))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("the location field");
  }

  @Test
  @DisplayName("Catalog-scoped location gate overrides the realm configuration")
  void testClientSpecifiedLocationCatalogOverride(@TempDir Path tempDir) {
    TestServices services =
        TestServices.builder()
            .config(serverConfig(ALLOW_CLIENT_SPECIFIED_TABLE_LOCATION.key(), "true"))
            .build();
    String baseLocation =
        LocationUtil.stripTrailingSlash(tempDir.toAbsolutePath().toUri().toString());
    createCatalogAndNamespace(
        services,
        Map.of(ALLOW_CLIENT_SPECIFIED_TABLE_LOCATION.catalogConfig(), "false"),
        baseLocation);

    String someLocation =
        String.format("%s/%s/%s/catalog-override", baseLocation, CATALOG, NAMESPACE);
    assertThatThrownBy(() -> createTableWithLocation(services, someLocation))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("the location field");
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
        .hasMessageContaining("the location field");
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
  @DisplayName("Caller-specified location is honored when the location gate is enabled")
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
  @DisplayName("Default table location uses the legacy form")
  void testLegacyLocationByDefault(@TempDir Path tempDir) {
    TestServices services = TestServices.builder().config(serverConfig()).build();
    String baseLocation =
        LocationUtil.stripTrailingSlash(tempDir.toAbsolutePath().toUri().toString());
    createCatalogAndNamespace(services, Map.of(), baseLocation);

    String tableName = getTableName();
    assertThat(createTableWithName(services, tableName))
        .isEqualTo(String.format("%s/%s/%s/%s", baseLocation, CATALOG, NAMESPACE, tableName));
  }
}
