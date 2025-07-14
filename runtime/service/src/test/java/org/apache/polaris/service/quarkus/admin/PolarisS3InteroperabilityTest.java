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

import static org.apache.polaris.core.entity.CatalogEntity.DEFAULT_BASE_LOCATION_KEY;
import static org.apache.polaris.core.entity.PolarisEntityConstants.ENTITY_BASE_LOCATION;
import static org.apache.polaris.service.quarkus.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class PolarisS3InteroperabilityTest {

  private static final String BASE_LOCATION = "://bucket1/base";
  private static final Map<String, Object> SERVER_CONFIG =
      Map.of(
          "ALLOW_UNSTRUCTURED_TABLE_LOCATION",
          "false",
          "ALLOW_TABLE_LOCATION_OVERLAP",
          "false",
          "ALLOW_INSECURE_STORAGE_TYPES",
          "true",
          "SUPPORTED_CATALOG_STORAGE_TYPES",
          List.of("FILE", "S3"));
  private static final FileIO fileIO = new InMemoryFileIO();

  private final TestServices services;

  private static String makeNamespaceLocation(String catalogName, String namespace, String scheme) {
    return "%s%s/%s/%s".formatted(scheme, BASE_LOCATION, catalogName, namespace);
  }

  private static String makeTableLocation(
      String catalogName, String namespace, String tableName, String scheme) {
    return "%s%s/%s/%s/%s".formatted(scheme, BASE_LOCATION, catalogName, namespace, tableName);
  }

  public PolarisS3InteroperabilityTest() {
    TestServices.FileIOFactorySupplier fileIOFactorySupplier =
        (entityManagerFactory, metaStoreManagerFactory) ->
            (FileIOFactory)
                (callContext,
                    ioImplClassName,
                    properties,
                    identifier,
                    tableLocations,
                    storageActions,
                    resolvedEntityPath) -> new InMemoryFileIO();
    services =
        TestServices.builder()
            .config(SERVER_CONFIG)
            .fileIOFactorySupplier(fileIOFactorySupplier)
            .build();
  }

  private PolarisCatalog createCatalog(String catalogName, String scheme) {
    CatalogProperties.Builder propertiesBuilder =
        CatalogProperties.builder()
            .setDefaultBaseLocation(String.format("%s%s/%s", scheme, BASE_LOCATION, catalogName))
            .putAll(Map.of());

    StorageConfigInfo config =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setRoleArn("arn:aws:iam::123456789012:role/catalog_role")
            .build();
    Catalog catalogObject =
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            catalogName,
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
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(catalogName, services.realmContext(), services.securityContext())) {
      return response.readEntity(PolarisCatalog.class);
    }
  }

  private GetNamespaceResponse createNamespace(
      String catalogName, String namespace, String scheme) {
    Map<String, String> properties = new HashMap<>();
    properties.put(ENTITY_BASE_LOCATION, makeNamespaceLocation(catalogName, namespace, scheme));
    CreateNamespaceRequest createNamespaceRequest =
        CreateNamespaceRequest.builder()
            .withNamespace(Namespace.of(namespace))
            .setProperties(properties)
            .build();
    try (Response response =
        services
            .restApi()
            .createNamespace(
                catalogName,
                createNamespaceRequest,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
    try (Response response =
        services
            .restApi()
            .loadNamespaceMetadata(
                catalogName, namespace, services.realmContext(), services.securityContext())) {
      return response.readEntity(GetNamespaceResponse.class);
    }
  }

  private LoadTableResponse createTable(
      String catalogName, String namespace, String tableName, String scheme, String location) {
    String tableLocation =
        location == null ? makeTableLocation(catalogName, namespace, tableName, scheme) : location;
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder()
            .withName(tableName)
            .withLocation(tableLocation)
            .withSchema(SCHEMA)
            .build();
    try (Response response =
        services
            .restApi()
            .createTable(
                catalogName,
                namespace,
                createTableRequest,
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
    try (Response response =
        services
            .restApi()
            .loadTable(
                catalogName,
                namespace,
                tableName,
                null,
                null,
                "ALL",
                services.realmContext(),
                services.securityContext())) {
      return response.readEntity(LoadTableResponse.class);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3", "s3a"})
  void testCatalog(String scheme) {
    PolarisCatalog catalogEntity = createCatalog("cat_" + scheme, scheme);
    assertThat(catalogEntity).isNotNull();
    assertThat(catalogEntity.getName()).isEqualTo("cat_" + scheme);
    assertThat(catalogEntity.getProperties())
        .extractingByKey(DEFAULT_BASE_LOCATION_KEY)
        .asString()
        .startsWith(scheme);
  }

  @ParameterizedTest
  @CsvSource({"s3,s3a", "s3a,s3"})
  public void testNamespace(String catalogScheme, String namespaceScheme) {
    String catalogName = "cat_n_" + catalogScheme;
    createCatalog(catalogName, catalogScheme);
    GetNamespaceResponse namespaceEntity = createNamespace(catalogName, "ns1", namespaceScheme);
    assertThat(namespaceEntity).isNotNull();
    assertThat(namespaceEntity.properties())
        .extractingByKey("location")
        .asString()
        .startsWith(namespaceScheme);
  }

  @ParameterizedTest
  @CsvSource({"s3,s3a", "s3a,s3"})
  public void testTable(String catalogScheme, String tableScheme) {
    String catalogName = "cat_nt_" + catalogScheme;
    createCatalog(catalogName, catalogScheme);
    createNamespace(catalogName, "ns1", catalogScheme);
    LoadTableResponse tableEntity1 = createTable(catalogName, "ns1", "tbl1", tableScheme, null);
    assertThat(tableEntity1).isNotNull();
    assertThat(tableEntity1.metadataLocation()).startsWith(tableScheme);
    assertThat(tableEntity1.tableMetadata().location()).startsWith(tableScheme);

    LoadTableResponse tableEntity2 = createTable(catalogName, "ns1", "tbl2", catalogScheme, null);
    assertThat(tableEntity2).isNotNull();
    assertThat(tableEntity2.metadataLocation()).startsWith(catalogScheme);
    assertThat(tableEntity2.tableMetadata().location()).startsWith(catalogScheme);
  }

  @ParameterizedTest
  @CsvSource({"s3,s3a", "s3a,s3"})
  public void testTableOverlap(String table1Scheme, String table2Scheme) {
    String catalogName = "cat_nt";
    createCatalog(catalogName, "s3");
    createNamespace(catalogName, "ns1", "s3");
    createTable(
        catalogName,
        "ns1",
        "tbl1",
        table1Scheme,
        makeTableLocation(catalogName, "ns1", "tbl1", table1Scheme));
    ForbiddenException ex =
        assertThrows(
            ForbiddenException.class,
            () ->
                createTable(
                    catalogName,
                    "ns1",
                    "tbl2",
                    table2Scheme,
                    makeTableLocation(catalogName, "ns1", "tbl1", table2Scheme)));
    assertThat(ex.getMessage()).contains("Unable to create table at location");
    assertThat(ex.getMessage()).contains(table1Scheme);
    assertThat(ex.getMessage()).contains(table2Scheme);
  }
}
