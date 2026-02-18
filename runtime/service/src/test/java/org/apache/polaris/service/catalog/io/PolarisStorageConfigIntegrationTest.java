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
package org.apache.polaris.service.catalog.io;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.NamespaceStorageConfigResponse;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.TableStorageConfigResponse;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for storage configuration management endpoints.
 *
 * <p>These tests verify the Management API endpoints for:
 *
 * <ul>
 *   <li>GET/PUT/DELETE namespace storage config
 *   <li>GET/PUT/DELETE table storage config
 * </ul>
 */
public class PolarisStorageConfigIntegrationTest {

  private static final String TEST_CATALOG = "test_storage_config_catalog";
  private static final String TEST_NAMESPACE = "test_namespace";
  private static final String TEST_TABLE = "test_table";
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private TestServices services;

  @BeforeEach
  public void setup() {
    services =
        TestServices.builder()
            .config(
                Map.of(
                    "SUPPORTED_CATALOG_STORAGE_TYPES",
                    List.of("S3", "GCS", "AZURE", "FILE"),
                    "ALLOW_INSECURE_STORAGE_TYPES",
                    true))
            .build();

    // Create test catalog
    FileStorageConfigInfo catalogStorageConfig =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file:///tmp/test/"))
            .setStorageName("catalog-storage")
            .build();

    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(TEST_CATALOG)
            .setProperties(new CatalogProperties("file:///tmp/test/"))
            .setStorageConfigInfo(catalogStorageConfig)
            .build();

    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }

    // Create test namespace
    CreateNamespaceRequest createNamespaceRequest =
        CreateNamespaceRequest.builder().withNamespace(Namespace.of(TEST_NAMESPACE)).build();

    try (Response response =
        services
            .restApi()
            .createNamespace(
                TEST_CATALOG,
                createNamespaceRequest,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // Create test table
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder().withName(TEST_TABLE).withSchema(SCHEMA).build();

    try (Response response =
        services
            .restApi()
            .createTable(
                TEST_CATALOG,
                TEST_NAMESPACE,
                createTableRequest,
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  @AfterEach
  public void tearDown() {
    // Cleanup if needed
  }

  /** Test GET namespace storage config endpoint. */
  @Test
  public void testGetNamespaceStorageConfig() {
    // Test GET namespace storage config - should return catalog's config since namespace has none
    Response response =
        services
            .catalogsApi()
            .getNamespaceStorageConfig(
                TEST_CATALOG, TEST_NAMESPACE, services.realmContext(), services.securityContext());

    // Expect 200 OK with catalog storage config
    assertThat(response.getStatus())
        .as("GET namespace storage config should return 200")
        .isEqualTo(Response.Status.OK.getStatusCode());

    StorageConfigInfo config = response.readEntity(StorageConfigInfo.class);
    assertThat(config).isNotNull();
    assertThat(config.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.FILE);
    assertThat(config.getStorageName()).isEqualTo("catalog-storage");

    response.close();
  }

  /** Test PUT namespace storage config endpoint. */
  @Test
  public void testSetNamespaceStorageConfig() {
    // Create namespace-specific Azure storage config
    AzureStorageConfigInfo namespaceStorageConfig =
        AzureStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setAllowedLocations(List.of("abfss://container@storage.dfs.core.windows.net/"))
            .setStorageName("namespace-storage")
            .setTenantId("tenant-123")
            .build();

    // Test PUT namespace storage config
    Response response =
        services
            .catalogsApi()
            .setNamespaceStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                namespaceStorageConfig,
                services.realmContext(),
                services.securityContext());

    // Expect 200 OK
    assertThat(response.getStatus())
        .as("PUT namespace storage config should return 200")
        .isEqualTo(Response.Status.OK.getStatusCode());

    NamespaceStorageConfigResponse nsResponse =
        response.readEntity(NamespaceStorageConfigResponse.class);
    assertThat(nsResponse).isNotNull();
    assertThat(nsResponse.getStorageConfigInfo()).isNotNull();
    assertThat(nsResponse.getStorageConfigInfo().getStorageType())
        .isEqualTo(StorageConfigInfo.StorageTypeEnum.AZURE);

    response.close();

    // Verify we can GET it back
    try (Response getResponse =
        services
            .catalogsApi()
            .getNamespaceStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                services.realmContext(),
                services.securityContext())) {
      assertThat(getResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo retrieved = getResponse.readEntity(StorageConfigInfo.class);
      assertThat(retrieved).isInstanceOf(AzureStorageConfigInfo.class);
      AzureStorageConfigInfo azureRetrieved = (AzureStorageConfigInfo) retrieved;
      assertThat(azureRetrieved.getTenantId()).isEqualTo("tenant-123");
      assertThat(azureRetrieved.getStorageName()).isEqualTo("namespace-storage");
    }
  }

  /** Test DELETE namespace storage config endpoint. */
  @Test
  public void testDeleteNamespaceStorageConfig() {
    // First set a config
    AzureStorageConfigInfo namespaceStorageConfig =
        AzureStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setAllowedLocations(List.of("abfss://container@storage.dfs.core.windows.net/"))
            .setTenantId("tenant-123")
            .build();

    try (Response setResponse =
        services
            .catalogsApi()
            .setNamespaceStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                namespaceStorageConfig,
                services.realmContext(),
                services.securityContext())) {
      assertThat(setResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // Now delete it
    Response response =
        services
            .catalogsApi()
            .deleteNamespaceStorageConfig(
                TEST_CATALOG, TEST_NAMESPACE, services.realmContext(), services.securityContext());

    // Expect 204 No Content
    assertThat(response.getStatus())
        .as("DELETE namespace storage config should return 204")
        .isEqualTo(Response.Status.NO_CONTENT.getStatusCode());

    response.close();

    // Verify it's gone - should now return catalog config
    try (Response getResponse =
        services
            .catalogsApi()
            .getNamespaceStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                services.realmContext(),
                services.securityContext())) {
      assertThat(getResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo retrieved = getResponse.readEntity(StorageConfigInfo.class);
      assertThat(retrieved.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.FILE);
    }
  }

  /** Test GET table storage config endpoint. */
  @Test
  public void testGetTableStorageConfig() {
    // Test GET table storage config - should return catalog's config since table has none
    Response response =
        services
            .catalogsApi()
            .getTableStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                TEST_TABLE,
                services.realmContext(),
                services.securityContext());

    // Expect 200 OK
    assertThat(response.getStatus())
        .as("GET table storage config should return 200")
        .isEqualTo(Response.Status.OK.getStatusCode());

    StorageConfigInfo config = response.readEntity(StorageConfigInfo.class);
    assertThat(config).isNotNull();
    assertThat(config.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.FILE);

    response.close();
  }

  /** Test PUT table storage config endpoint. */
  @Test
  public void testSetTableStorageConfig() {
    // Create table-specific S3 storage config
    AwsStorageConfigInfo tableStorageConfig =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://table-bucket/"))
            .setStorageName("table-storage")
            .setRoleArn("arn:aws:iam::123456789012:role/table-role")
            .build();

    // Test PUT table storage config
    Response response =
        services
            .catalogsApi()
            .setTableStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                TEST_TABLE,
                tableStorageConfig,
                services.realmContext(),
                services.securityContext());

    // Expect 200 OK
    assertThat(response.getStatus())
        .as("PUT table storage config should return 200")
        .isEqualTo(Response.Status.OK.getStatusCode());

    TableStorageConfigResponse tableResponse =
        response.readEntity(TableStorageConfigResponse.class);
    assertThat(tableResponse).isNotNull();
    assertThat(tableResponse.getStorageConfigInfo()).isNotNull();
    assertThat(tableResponse.getStorageConfigInfo().getStorageType())
        .isEqualTo(StorageConfigInfo.StorageTypeEnum.S3);

    response.close();

    // Verify we can GET it back
    try (Response getResponse =
        services
            .catalogsApi()
            .getTableStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                TEST_TABLE,
                services.realmContext(),
                services.securityContext())) {
      assertThat(getResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo retrieved = getResponse.readEntity(StorageConfigInfo.class);
      assertThat(retrieved).isInstanceOf(AwsStorageConfigInfo.class);
      AwsStorageConfigInfo awsRetrieved = (AwsStorageConfigInfo) retrieved;
      assertThat(awsRetrieved.getRoleArn()).isEqualTo("arn:aws:iam::123456789012:role/table-role");
      assertThat(awsRetrieved.getStorageName()).isEqualTo("table-storage");
    }
  }

  @Test
  public void testTableEffectiveFallbackSemantics() {
    AzureStorageConfigInfo namespaceStorageConfig =
        AzureStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setAllowedLocations(List.of("abfss://container@storage.dfs.core.windows.net/"))
            .setStorageName("namespace-effective")
            .setTenantId("tenant-effective")
            .build();

    AwsStorageConfigInfo tableStorageConfig =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://table-effective/"))
            .setStorageName("table-effective")
            .setRoleArn("arn:aws:iam::123456789012:role/table-effective")
            .build();

    try (Response setNamespaceResponse =
        services
            .catalogsApi()
            .setNamespaceStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                namespaceStorageConfig,
                services.realmContext(),
                services.securityContext())) {
      assertThat(setNamespaceResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response setTableResponse =
        services
            .catalogsApi()
            .setTableStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                TEST_TABLE,
                tableStorageConfig,
                services.realmContext(),
                services.securityContext())) {
      assertThat(setTableResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response getTableResponse =
        services
            .catalogsApi()
            .getTableStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                TEST_TABLE,
                services.realmContext(),
                services.securityContext())) {
      assertThat(getTableResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AwsStorageConfigInfo tableConfig = getTableResponse.readEntity(AwsStorageConfigInfo.class);
      assertThat(tableConfig.getStorageName()).isEqualTo("table-effective");
    }

    try (Response deleteTableResponse =
        services
            .catalogsApi()
            .deleteTableStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                TEST_TABLE,
                services.realmContext(),
                services.securityContext())) {
      assertThat(deleteTableResponse.getStatus())
          .isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    try (Response getNamespaceFallback =
        services
            .catalogsApi()
            .getTableStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                TEST_TABLE,
                services.realmContext(),
                services.securityContext())) {
      assertThat(getNamespaceFallback.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AzureStorageConfigInfo namespaceConfig =
          getNamespaceFallback.readEntity(AzureStorageConfigInfo.class);
      assertThat(namespaceConfig.getStorageName()).isEqualTo("namespace-effective");
    }

    try (Response deleteNamespaceResponse =
        services
            .catalogsApi()
            .deleteNamespaceStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                services.realmContext(),
                services.securityContext())) {
      assertThat(deleteNamespaceResponse.getStatus())
          .isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    try (Response getCatalogFallback =
        services
            .catalogsApi()
            .getTableStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                TEST_TABLE,
                services.realmContext(),
                services.securityContext())) {
      assertThat(getCatalogFallback.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      FileStorageConfigInfo catalogConfig =
          getCatalogFallback.readEntity(FileStorageConfigInfo.class);
      assertThat(catalogConfig.getStorageName()).isEqualTo("catalog-storage");
    }
  }

  @Test
  public void testTableEffectiveFallbackUsesAncestorNamespaceInNestedPath() {
    String namespaceL1 = "ns1";
    String namespaceL3 = "ns1\u001Fns2\u001Fns3";
    String nestedTable = "nested_table";

    try (Response createNs1 =
        services
            .restApi()
            .createNamespace(
                TEST_CATALOG,
                CreateNamespaceRequest.builder().withNamespace(Namespace.of("ns1")).build(),
                services.realmContext(),
                services.securityContext())) {
      assertThat(createNs1.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response createNs2 =
        services
            .restApi()
            .createNamespace(
                TEST_CATALOG,
                CreateNamespaceRequest.builder().withNamespace(Namespace.of("ns1", "ns2")).build(),
                services.realmContext(),
                services.securityContext())) {
      assertThat(createNs2.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response createNs3 =
        services
            .restApi()
            .createNamespace(
                TEST_CATALOG,
                CreateNamespaceRequest.builder()
                    .withNamespace(Namespace.of("ns1", "ns2", "ns3"))
                    .build(),
                services.realmContext(),
                services.securityContext())) {
      assertThat(createNs3.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response createNestedTable =
        services
            .restApi()
            .createTable(
                TEST_CATALOG,
                namespaceL3,
                CreateTableRequest.builder().withName(nestedTable).withSchema(SCHEMA).build(),
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(createNestedTable.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    AzureStorageConfigInfo ancestorStorageConfig =
        AzureStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setAllowedLocations(List.of("abfss://ancestor@storage.dfs.core.windows.net/"))
            .setStorageName("ancestor-storage")
            .setTenantId("ancestor-tenant")
            .build();

    try (Response setAncestorConfig =
        services
            .catalogsApi()
            .setNamespaceStorageConfig(
                TEST_CATALOG,
                namespaceL1,
                ancestorStorageConfig,
                services.realmContext(),
                services.securityContext())) {
      assertThat(setAncestorConfig.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response getNestedTableConfig =
        services
            .catalogsApi()
            .getTableStorageConfig(
                TEST_CATALOG,
                namespaceL3,
                nestedTable,
                services.realmContext(),
                services.securityContext())) {
      assertThat(getNestedTableConfig.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AzureStorageConfigInfo resolved =
          getNestedTableConfig.readEntity(AzureStorageConfigInfo.class);
      assertThat(resolved.getStorageName()).isEqualTo("ancestor-storage");
      assertThat(resolved.getTenantId()).isEqualTo("ancestor-tenant");
    }
  }

  /** Test DELETE table storage config endpoint. */
  @Test
  public void testDeleteTableStorageConfig() {
    // First set a config
    AwsStorageConfigInfo tableStorageConfig =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://table-bucket/"))
            .setRoleArn("arn:aws:iam::123456789012:role/table-role")
            .build();

    try (Response setResponse =
        services
            .catalogsApi()
            .setTableStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                TEST_TABLE,
                tableStorageConfig,
                services.realmContext(),
                services.securityContext())) {
      assertThat(setResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // Now delete it
    Response response =
        services
            .catalogsApi()
            .deleteTableStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                TEST_TABLE,
                services.realmContext(),
                services.securityContext());

    // Expect 204 No Content
    assertThat(response.getStatus())
        .as("DELETE table storage config should return 204")
        .isEqualTo(Response.Status.NO_CONTENT.getStatusCode());

    response.close();

    // Verify it's gone - should now return catalog config
    try (Response getResponse =
        services
            .catalogsApi()
            .getTableStorageConfig(
                TEST_CATALOG,
                TEST_NAMESPACE,
                TEST_TABLE,
                services.realmContext(),
                services.securityContext())) {
      assertThat(getResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo retrieved = getResponse.readEntity(StorageConfigInfo.class);
      assertThat(retrieved.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.FILE);
    }
  }

  /** Test storage config operations with multipart namespace paths. */
  @Test
  public void testMultipartNamespaceStorageConfig() {
    // Create a multipart namespace - need to create parents first
    String multipartNamespace = "level1\u001Flevel2\u001Flevel3"; // Unit separator

    // Create parent namespaces first
    CreateNamespaceRequest createLevel1Request =
        CreateNamespaceRequest.builder().withNamespace(Namespace.of("level1")).build();

    try (Response createResponse =
        services
            .restApi()
            .createNamespace(
                TEST_CATALOG,
                createLevel1Request,
                services.realmContext(),
                services.securityContext())) {
      assertThat(createResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    CreateNamespaceRequest createLevel2Request =
        CreateNamespaceRequest.builder().withNamespace(Namespace.of("level1", "level2")).build();

    try (Response createResponse =
        services
            .restApi()
            .createNamespace(
                TEST_CATALOG,
                createLevel2Request,
                services.realmContext(),
                services.securityContext())) {
      assertThat(createResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    CreateNamespaceRequest createNamespaceRequest =
        CreateNamespaceRequest.builder()
            .withNamespace(Namespace.of("level1", "level2", "level3"))
            .build();

    try (Response createResponse =
        services
            .restApi()
            .createNamespace(
                TEST_CATALOG,
                createNamespaceRequest,
                services.realmContext(),
                services.securityContext())) {
      assertThat(createResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // Create a GCP storage config for multipart namespace
    GcpStorageConfigInfo storageConfig =
        GcpStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
            .setAllowedLocations(List.of("gs://gcs-bucket/path/"))
            .setGcsServiceAccount("test@test.iam.gserviceaccount.com")
            .build();

    // Test with multipart namespace
    Response response =
        services
            .catalogsApi()
            .setNamespaceStorageConfig(
                TEST_CATALOG,
                multipartNamespace,
                storageConfig,
                services.realmContext(),
                services.securityContext());

    // Expect 200 OK
    assertThat(response.getStatus())
        .as("Multipart namespace storage config should return 200")
        .isEqualTo(Response.Status.OK.getStatusCode());

    response.close();

    // Verify we can GET it back
    try (Response getResponse =
        services
            .catalogsApi()
            .getNamespaceStorageConfig(
                TEST_CATALOG,
                multipartNamespace,
                services.realmContext(),
                services.securityContext())) {
      assertThat(getResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo retrieved = getResponse.readEntity(StorageConfigInfo.class);
      assertThat(retrieved).isInstanceOf(GcpStorageConfigInfo.class);
      GcpStorageConfigInfo gcpRetrieved = (GcpStorageConfigInfo) retrieved;
      assertThat(gcpRetrieved.getGcsServiceAccount())
          .isEqualTo("test@test.iam.gserviceaccount.com");
    }
  }
}
