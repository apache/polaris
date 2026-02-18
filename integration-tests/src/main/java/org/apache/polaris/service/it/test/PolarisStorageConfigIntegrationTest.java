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
package org.apache.polaris.service.it.test;

import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.CatalogApi;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for namespace and table storage configuration management endpoints.
 *
 * <p>These tests verify the complete implementation of:
 *
 * <ul>
 *   <li>GET/PUT/DELETE namespace storage config
 *   <li>GET/PUT/DELETE table storage config
 *   <li>Storage config hierarchy resolution (namespace → catalog)
 *   <li>Nested namespace ancestor fallback for table storage config
 *   <li>Multipart namespace handling with unit separator encoding
 * </ul>
 */
@ExtendWith(PolarisIntegrationTestExtension.class)
public class PolarisStorageConfigIntegrationTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisStorageConfigIntegrationTest.class);
  private static final String S3_BASE_LOCATION_PROPERTY = "polaris.it.storage.s3.base-location";
  private static final String S3_ROLE_ARN_PROPERTY = "polaris.it.storage.s3.role-arn";
  private static final String S3_ENDPOINT_PROPERTY = "polaris.it.storage.s3.endpoint";
  private static final String S3_PATH_STYLE_ACCESS_PROPERTY =
      "polaris.it.storage.s3.path-style-access";
  private static final String S3_DATA_PLANE_ENABLED_PROPERTY = "polaris.it.storage.s3.enabled";
  private static final String DEFAULT_S3_BASE_LOCATION = "s3://test-bucket/";

  private static PolarisClient client;
  private static ManagementApi managementApi;
  private static CatalogApi catalogApi;
  private static ClientCredentials rootCredentials;
  private static String authToken;

  @BeforeAll
  public static void setup(PolarisApiEndpoints endpoints, ClientCredentials credentials) {
    client = polarisClient(endpoints);
    authToken = client.obtainToken(credentials);
    managementApi = client.managementApi(authToken);
    catalogApi = client.catalogApi(authToken);
    rootCredentials = credentials;
  }

  @AfterAll
  public static void close() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  @AfterEach
  public void tearDown() {
    client.cleanUp(authToken);
  }

  /**
   * Test GET namespace storage config endpoint. Verifies that storage config is resolved from the
   * hierarchy (namespace override or catalog default).
   */
  @Test
  public void testGetNamespaceStorageConfig() {
    String catalogName = "test_catalog";
    String namespace = "test_namespace";

    managementApi.createCatalog(createS3Catalog(catalogName, "catalog-storage", "test-role"));

    // Create the namespace via Iceberg REST API
    catalogApi.createNamespace(catalogName, namespace);

    // Test GET namespace storage config - should return catalog config since no namespace override
    try (Response response = managementApi.getNamespaceStorageConfig(catalogName, namespace)) {
      assertThat(response.getStatus())
          .as("GET namespace storage config should return 200 OK")
          .isEqualTo(Response.Status.OK.getStatusCode());

      StorageConfigInfo retrieved = response.readEntity(StorageConfigInfo.class);
      assertThat(retrieved).isNotNull();
      assertThat(retrieved.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.S3);
      assertThat(retrieved).isInstanceOf(AwsStorageConfigInfo.class);
      AwsStorageConfigInfo awsConfig = (AwsStorageConfigInfo) retrieved;
      assertThat(awsConfig.getRoleArn()).isEqualTo("arn:aws:iam::123456789012:role/test-role");

      LOGGER.info("GET namespace storage config successfully returned catalog default");
    }
  }

  /**
   * Test PUT namespace storage config endpoint. Verifies that namespace-specific storage config can
   * be set and retrieved.
   */
  @Test
  public void testSetNamespaceStorageConfig() {
    String catalogName = "test_catalog_set_ns";
    String namespace = "test_namespace";

    // Create catalog with S3 storage
    AwsStorageConfigInfo catalogStorageConfig =
        createS3StorageConfig("catalog-storage", "catalog-role");

    PolarisCatalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties(s3BaseLocation()))
            .setStorageConfigInfo(catalogStorageConfig)
            .build();

    managementApi.createCatalog(catalog);

    // Create the namespace
    catalogApi.createNamespace(catalogName, namespace);

    // Create namespace-specific Azure storage config
    AzureStorageConfigInfo namespaceStorageConfig =
        AzureStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setAllowedLocations(List.of("abfss://container@storage.dfs.core.windows.net/"))
            .setStorageName("namespace-storage")
            .setTenantId("tenant-123")
            .build();

    // Test PUT namespace storage config
    try (Response response =
        managementApi.setNamespaceStorageConfig(catalogName, namespace, namespaceStorageConfig)) {
      assertThat(response.getStatus())
          .as("PUT namespace storage config should return 200 OK")
          .isEqualTo(Response.Status.OK.getStatusCode());

      LOGGER.info("PUT namespace storage config successful");
    }

    // Verify we can GET it back and it returns the Azure config, not the catalog S3 config
    try (Response response = managementApi.getNamespaceStorageConfig(catalogName, namespace)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo retrieved = response.readEntity(StorageConfigInfo.class);
      assertThat(retrieved.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.AZURE);
      assertThat(retrieved).isInstanceOf(AzureStorageConfigInfo.class);
      AzureStorageConfigInfo azureConfig = (AzureStorageConfigInfo) retrieved;
      assertThat(azureConfig.getTenantId()).isEqualTo("tenant-123");
      assertThat(azureConfig.getStorageName()).isEqualTo("namespace-storage");

      LOGGER.info("Namespace storage config override verified");
    }
  }

  @Test
  public void testTableStorageConfigCrudAndFallback() {
    requireS3DataPlane();

    String catalogName = "test_catalog_table_crud";
    String namespace = "test_namespace";
    String table = "test_table";

    AwsStorageConfigInfo catalogStorageConfig =
        createS3StorageConfig("catalog-storage", "catalog-role");

    PolarisCatalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties(s3BaseLocation()))
            .setStorageConfigInfo(catalogStorageConfig)
            .build();

    managementApi.createCatalog(catalog);
    catalogApi.createNamespace(catalogName, namespace);
    catalogApi.createTable(catalogName, namespace, table);

    AwsStorageConfigInfo tableStorageConfig = createS3StorageConfig("table-storage", "table-role");

    try (Response response =
        managementApi.setTableStorageConfig(catalogName, namespace, table, tableStorageConfig)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, namespace, table)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AwsStorageConfigInfo retrieved = response.readEntity(AwsStorageConfigInfo.class);
      assertThat(retrieved.getRoleArn()).isEqualTo("arn:aws:iam::123456789012:role/table-role");
      assertThat(retrieved.getStorageName()).isEqualTo("table-storage");
    }

    try (Response response =
        managementApi.deleteTableStorageConfig(catalogName, namespace, table)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, namespace, table)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo retrieved = response.readEntity(StorageConfigInfo.class);
      assertThat(retrieved.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.S3);
      assertThat(retrieved.getStorageName()).isEqualTo("catalog-storage");
    }
  }

  @Test
  public void testNestedAncestorNamespaceFallbackForTableStorageConfig() {
    requireS3DataPlane();

    String catalogName = "test_catalog_nested_table";
    String ns1 = "ns1";
    String ns2 = "ns1\u001Fns2";
    String ns3 = "ns1\u001Fns2\u001Fns3";
    String table = "nested_table";

    AwsStorageConfigInfo catalogStorageConfig =
        createS3StorageConfig("catalog-storage", "catalog-role");

    PolarisCatalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties(s3BaseLocation()))
            .setStorageConfigInfo(catalogStorageConfig)
            .build();

    managementApi.createCatalog(catalog);
    catalogApi.createNamespace(catalogName, ns1);
    catalogApi.createNamespace(catalogName, ns2);
    catalogApi.createNamespace(catalogName, ns3);
    catalogApi.createTable(catalogName, ns3, table);

    AzureStorageConfigInfo ns1StorageConfig =
        AzureStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setAllowedLocations(List.of("abfss://ancestor@storage.dfs.core.windows.net/"))
            .setStorageName("ancestor-storage")
            .setTenantId("ancestor-tenant")
            .build();

    try (Response response =
        managementApi.setNamespaceStorageConfig(catalogName, ns1, ns1StorageConfig)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, ns3, table)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AzureStorageConfigInfo retrieved = response.readEntity(AzureStorageConfigInfo.class);
      assertThat(retrieved.getStorageName()).isEqualTo("ancestor-storage");
      assertThat(retrieved.getTenantId()).isEqualTo("ancestor-tenant");
    }
  }

  @Test
  public void testStorageConfigEndpointsReturnForbiddenForUnauthorizedPrincipal() {
    requireS3DataPlane();

    String catalogName = "test_catalog_forbidden";
    String namespace = "test_namespace";
    String table = "test_table";

    AwsStorageConfigInfo catalogStorageConfig = createS3StorageConfig(null, "catalog-role");

    PolarisCatalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties(s3BaseLocation()))
            .setStorageConfigInfo(catalogStorageConfig)
            .build();

    managementApi.createCatalog(catalog);
    catalogApi.createNamespace(catalogName, namespace);
    catalogApi.createTable(catalogName, namespace, table);

    PrincipalWithCredentials unauthorizedPrincipal =
        managementApi.createPrincipal(client.newEntityName("storage_cfg_user"));
    String unauthorizedToken = client.obtainToken(unauthorizedPrincipal);
    ManagementApi unauthorizedManagementApi = client.managementApi(unauthorizedToken);

    AzureStorageConfigInfo namespaceStorageConfig =
        AzureStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setAllowedLocations(List.of("abfss://container@storage.dfs.core.windows.net/"))
            .setTenantId("tenant-123")
            .build();

    AwsStorageConfigInfo tableStorageConfig = createS3StorageConfig(null, "table-role");

    try (Response response =
        unauthorizedManagementApi.getNamespaceStorageConfig(catalogName, namespace)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.FORBIDDEN.getStatusCode());
    }

    try (Response response =
        unauthorizedManagementApi.setNamespaceStorageConfig(
            catalogName, namespace, namespaceStorageConfig)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.FORBIDDEN.getStatusCode());
    }

    try (Response response =
        unauthorizedManagementApi.deleteNamespaceStorageConfig(catalogName, namespace)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.FORBIDDEN.getStatusCode());
    }

    try (Response response =
        unauthorizedManagementApi.getTableStorageConfig(catalogName, namespace, table)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.FORBIDDEN.getStatusCode());
    }

    try (Response response =
        unauthorizedManagementApi.setTableStorageConfig(
            catalogName, namespace, table, tableStorageConfig)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.FORBIDDEN.getStatusCode());
    }

    try (Response response =
        unauthorizedManagementApi.deleteTableStorageConfig(catalogName, namespace, table)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.FORBIDDEN.getStatusCode());
    }
  }

  @Test
  public void testCredentialVendingAccessDelegationAcrossFallbackHierarchy() {
    requireS3DataPlane();

    String catalogName = "test_catalog_vended_creds";
    String namespace = "test_namespace";
    String table = "test_table";

    AwsStorageConfigInfo catalogStorageConfig =
        createS3StorageConfig("catalog-storage", "catalog-role");

    PolarisCatalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties(s3BaseLocation()))
            .setStorageConfigInfo(catalogStorageConfig)
            .build();

    managementApi.createCatalog(catalog);
    catalogApi.createNamespace(catalogName, namespace);
    catalogApi.createTable(catalogName, namespace, table);

    String principalName = client.newEntityName("storage_cfg_delegate_user");
    String principalRoleName = client.newEntityName("storage_cfg_delegate_role");
    PrincipalWithCredentials delegatedPrincipal =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);
    managementApi.makeAdmin(principalRoleName, catalog);
    String delegatedToken = client.obtainToken(delegatedPrincipal);
    CatalogApi delegatedCatalogApi = client.catalogApi(delegatedToken);

    AwsStorageConfigInfo namespaceStorageConfig =
        createS3StorageConfig("namespace-storage", "namespace-role");

    AwsStorageConfigInfo tableStorageConfig = createS3StorageConfig("table-storage", "table-role");

    try (Response response =
        managementApi.setNamespaceStorageConfig(catalogName, namespace, namespaceStorageConfig)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response response =
        managementApi.setTableStorageConfig(catalogName, namespace, table, tableStorageConfig)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    TableIdentifier tableId = TableIdentifier.of(Namespace.of(namespace), table);

    try (Response response = managementApi.getTableStorageConfig(catalogName, namespace, table)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AwsStorageConfigInfo effective = response.readEntity(AwsStorageConfigInfo.class);
      assertThat(effective.getStorageName()).isEqualTo("table-storage");
    }

    LoadTableResponse tableLoad =
        delegatedCatalogApi.loadTableWithAccessDelegation(catalogName, tableId, "ALL");
    assertThat(tableLoad).isNotNull();
    assertThat(tableLoad.metadataLocation()).isNotNull();
    assertThat(tableLoad.credentials()).isNotNull();

    try (Response response =
        managementApi.deleteTableStorageConfig(catalogName, namespace, table)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, namespace, table)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AwsStorageConfigInfo effective = response.readEntity(AwsStorageConfigInfo.class);
      assertThat(effective.getStorageName()).isEqualTo("namespace-storage");
    }

    LoadTableResponse namespaceLoad =
        delegatedCatalogApi.loadTableWithAccessDelegation(catalogName, tableId, "ALL");
    assertThat(namespaceLoad).isNotNull();
    assertThat(namespaceLoad.metadataLocation()).isNotNull();
    assertThat(namespaceLoad.credentials()).isNotNull();

    try (Response response = managementApi.deleteNamespaceStorageConfig(catalogName, namespace)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, namespace, table)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AwsStorageConfigInfo effective = response.readEntity(AwsStorageConfigInfo.class);
      assertThat(effective.getStorageName()).isEqualTo("catalog-storage");
    }

    LoadTableResponse catalogLoad =
        delegatedCatalogApi.loadTableWithAccessDelegation(catalogName, tableId, "ALL");
    assertThat(catalogLoad).isNotNull();
    assertThat(catalogLoad.metadataLocation()).isNotNull();
    assertThat(catalogLoad.credentials()).isNotNull();
  }

  /**
   * Test DELETE namespace storage config endpoint. Verifies that namespace override can be removed
   * and resolution falls back to catalog config.
   */
  @Test
  public void testDeleteNamespaceStorageConfig() {
    String catalogName = "test_catalog_delete_ns";
    String namespace = "test_namespace";

    // Create a test catalog with S3 storage
    AwsStorageConfigInfo catalogStorageConfig =
        createS3StorageConfig("catalog-storage", "test-role");

    PolarisCatalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties(s3BaseLocation()))
            .setStorageConfigInfo(catalogStorageConfig)
            .build();

    managementApi.createCatalog(catalog);

    // Create the namespace
    catalogApi.createNamespace(catalogName, namespace);

    // Set a namespace-specific GCP config
    GcpStorageConfigInfo namespaceStorageConfig =
        GcpStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
            .setAllowedLocations(List.of("gs://namespace-bucket/"))
            .setGcsServiceAccount("ns-sa@project.iam.gserviceaccount.com")
            .build();

    managementApi.setNamespaceStorageConfig(catalogName, namespace, namespaceStorageConfig);

    // Verify the GCP config is returned
    try (Response response = managementApi.getNamespaceStorageConfig(catalogName, namespace)) {
      StorageConfigInfo retrieved = response.readEntity(StorageConfigInfo.class);
      assertThat(retrieved.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.GCS);
    }

    // Test DELETE namespace storage config
    try (Response response = managementApi.deleteNamespaceStorageConfig(catalogName, namespace)) {
      assertThat(response.getStatus())
          .as("DELETE namespace storage config should return 204 No Content")
          .isEqualTo(Response.Status.NO_CONTENT.getStatusCode());

      LOGGER.info("DELETE namespace storage config successful");
    }

    // Verify it now falls back to catalog S3 config
    try (Response response = managementApi.getNamespaceStorageConfig(catalogName, namespace)) {
      StorageConfigInfo retrieved = response.readEntity(StorageConfigInfo.class);
      assertThat(retrieved.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.S3);
      assertThat(retrieved).isInstanceOf(AwsStorageConfigInfo.class);

      LOGGER.info("Namespace storage config successfully reverted to catalog default");
    }
  }

  /**
   * Test namespace storage config with multipart namespace. Verifies that unit separator encoding
   * works correctly for hierarchical namespaces.
   */
  @Test
  public void testMultipartNamespaceStorageConfig() {
    String catalogName = "test_catalog_multipart";
    // Multipart namespace: "accounting.tax"
    // Encoded with unit separator (0x1F): "accounting\u001Ftax"
    String namespace = "accounting\u001Ftax";

    // Create a test catalog
    AwsStorageConfigInfo catalogStorageConfig =
        createS3StorageConfig("catalog-storage", "test-role");

    PolarisCatalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties(s3BaseLocation()))
            .setStorageConfigInfo(catalogStorageConfig)
            .build();

    managementApi.createCatalog(catalog);

    // Create parent namespace first
    catalogApi.createNamespace(catalogName, "accounting");
    // Create child namespace
    catalogApi.createNamespace(catalogName, namespace);

    // Set a GCS config on the multipart namespace
    GcpStorageConfigInfo namespaceStorageConfig =
        GcpStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
            .setAllowedLocations(List.of("gs://tax-bucket/"))
            .setGcsServiceAccount("tax-sa@project.iam.gserviceaccount.com")
            .build();

    managementApi.setNamespaceStorageConfig(catalogName, namespace, namespaceStorageConfig);

    // Test GET with multipart namespace
    try (Response response = managementApi.getNamespaceStorageConfig(catalogName, namespace)) {
      assertThat(response.getStatus())
          .as("GET multipart namespace storage config should return 200 OK")
          .isEqualTo(Response.Status.OK.getStatusCode());

      StorageConfigInfo retrieved = response.readEntity(StorageConfigInfo.class);
      assertThat(retrieved.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.GCS);
      assertThat(retrieved).isInstanceOf(GcpStorageConfigInfo.class);
      GcpStorageConfigInfo gcpConfig = (GcpStorageConfigInfo) retrieved;
      assertThat(gcpConfig.getGcsServiceAccount())
          .isEqualTo("tax-sa@project.iam.gserviceaccount.com");

      LOGGER.info("Multipart namespace storage config handling works correctly");
    }
  }

  @Test
  public void testDeepHierarchyClosestWinsAndDeleteTransitions() {
    requireS3DataPlane();

    String catalogName = "test_catalog_deep_hierarchy";
    String nsL1 = "dept";
    String nsL2 = "dept\u001Fanalytics";
    String nsL3 = "dept\u001Fanalytics\u001Freports";
    String tableL1 = "table_l1";
    String tableL2 = "table_l2";
    String tableL3 = "table_l3";

    PolarisCatalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties(s3BaseLocation()))
            .setStorageConfigInfo(createS3StorageConfig("catalog-storage", "catalog-role"))
            .build();

    managementApi.createCatalog(catalog);
    catalogApi.createNamespace(catalogName, nsL1);
    catalogApi.createNamespace(catalogName, nsL2);
    catalogApi.createNamespace(catalogName, nsL3);
    catalogApi.createTable(catalogName, nsL1, tableL1);
    catalogApi.createTable(catalogName, nsL2, tableL2);
    catalogApi.createTable(catalogName, nsL3, tableL3);

    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName, nsL1, createAzureStorageConfig("l1-storage", "tenant-l1", "l1"))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName,
            nsL2,
            createGcsStorageConfig(
                "l2-storage", "dept-analytics-bucket", "l2-sa@project.iam.gserviceaccount.com"))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response response =
        managementApi.setTableStorageConfig(
            catalogName, nsL3, tableL3, createS3StorageConfig("l3-table-storage", "table-role"))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, nsL1, tableL1)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AzureStorageConfigInfo effective = response.readEntity(AzureStorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.AZURE);
      assertThat(effective.getStorageName()).isEqualTo("l1-storage");
      assertThat(effective.getTenantId()).isEqualTo("tenant-l1");
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, nsL2, tableL2)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      GcpStorageConfigInfo effective = response.readEntity(GcpStorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.GCS);
      assertThat(effective.getStorageName()).isEqualTo("l2-storage");
      assertThat(effective.getGcsServiceAccount())
          .isEqualTo("l2-sa@project.iam.gserviceaccount.com");
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, nsL3, tableL3)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AwsStorageConfigInfo effective = response.readEntity(AwsStorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.S3);
      assertThat(effective.getStorageName()).isEqualTo("l3-table-storage");
    }

    try (Response response = managementApi.deleteTableStorageConfig(catalogName, nsL3, tableL3)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, nsL3, tableL3)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      GcpStorageConfigInfo effective = response.readEntity(GcpStorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.GCS);
      assertThat(effective.getStorageName()).isEqualTo("l2-storage");
    }

    try (Response response = managementApi.deleteNamespaceStorageConfig(catalogName, nsL2)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, nsL2, tableL2)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AzureStorageConfigInfo effective = response.readEntity(AzureStorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.AZURE);
      assertThat(effective.getStorageName()).isEqualTo("l1-storage");
    }

    try (Response response = managementApi.deleteNamespaceStorageConfig(catalogName, nsL1)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, nsL1, tableL1)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AwsStorageConfigInfo effective = response.readEntity(AwsStorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.S3);
      assertThat(effective.getStorageName()).isEqualTo("catalog-storage");
    }
  }

  @Test
  public void testCatalogOnlyFallbackAndDelegatedCredentialVendingAcrossDepths() {
    requireS3DataPlane();

    String catalogName = "test_catalog_only_fallback";
    String nsL1 = "team";
    String nsL2 = "team\u001Fcore";
    String nsL3 = "team\u001Fcore\u001Fwarehouse";
    String tableL1 = "orders";
    String tableL2 = "line_items";
    String tableL3 = "daily_rollup";

    PolarisCatalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties(s3BaseLocation()))
            .setStorageConfigInfo(createS3StorageConfig("catalog-only-storage", "catalog-role"))
            .build();

    managementApi.createCatalog(catalog);
    catalogApi.createNamespace(catalogName, nsL1);
    catalogApi.createNamespace(catalogName, nsL2);
    catalogApi.createNamespace(catalogName, nsL3);
    catalogApi.createTable(catalogName, nsL1, tableL1);
    catalogApi.createTable(catalogName, nsL2, tableL2);
    catalogApi.createTable(catalogName, nsL3, tableL3);

    String principalName = client.newEntityName("catalog_only_delegate_user");
    String principalRoleName = client.newEntityName("catalog_only_delegate_role");
    PrincipalWithCredentials delegatedPrincipal =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);
    managementApi.makeAdmin(principalRoleName, catalog);
    String delegatedToken = client.obtainToken(delegatedPrincipal);
    CatalogApi delegatedCatalogApi = client.catalogApi(delegatedToken);

    try (Response response = managementApi.getTableStorageConfig(catalogName, nsL1, tableL1)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AwsStorageConfigInfo effective = response.readEntity(AwsStorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.S3);
      assertThat(effective.getStorageName()).isEqualTo("catalog-only-storage");
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, nsL2, tableL2)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AwsStorageConfigInfo effective = response.readEntity(AwsStorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.S3);
      assertThat(effective.getStorageName()).isEqualTo("catalog-only-storage");
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, nsL3, tableL3)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AwsStorageConfigInfo effective = response.readEntity(AwsStorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.S3);
      assertThat(effective.getStorageName()).isEqualTo("catalog-only-storage");
    }

    LoadTableResponse tableL1Load =
        delegatedCatalogApi.loadTableWithAccessDelegation(
            catalogName, TableIdentifier.of(Namespace.of(nsL1), tableL1), "ALL");
    assertThat(tableL1Load).isNotNull();
    assertThat(tableL1Load.metadataLocation()).isNotNull();
    assertThat(tableL1Load.credentials()).isNotNull();

    LoadTableResponse tableL2Load =
        delegatedCatalogApi.loadTableWithAccessDelegation(
            catalogName, TableIdentifier.of(Namespace.of("team", "core"), tableL2), "ALL");
    assertThat(tableL2Load).isNotNull();
    assertThat(tableL2Load.metadataLocation()).isNotNull();
    assertThat(tableL2Load.credentials()).isNotNull();

    LoadTableResponse tableL3Load =
        delegatedCatalogApi.loadTableWithAccessDelegation(
            catalogName,
            TableIdentifier.of(Namespace.of("team", "core", "warehouse"), tableL3),
            "ALL");
    assertThat(tableL3Load).isNotNull();
    assertThat(tableL3Load.metadataLocation()).isNotNull();
    assertThat(tableL3Load.credentials()).isNotNull();
  }

  @Test
  public void testSiblingIsolationAndSequentialTypeUpdates() {
    requireS3DataPlane();

    String catalogName = "test_catalog_sibling_isolation";
    String parentNs = "finance";
    String taxNs = "finance\u001Ftax";
    String auditNs = "finance\u001Faudit";
    String taxTable = "returns";
    String auditTable = "checkpoints";

    PolarisCatalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties(s3BaseLocation()))
            .setStorageConfigInfo(createS3StorageConfig("catalog-storage", "catalog-role"))
            .build();

    managementApi.createCatalog(catalog);
    catalogApi.createNamespace(catalogName, parentNs);
    catalogApi.createNamespace(catalogName, taxNs);
    catalogApi.createNamespace(catalogName, auditNs);
    catalogApi.createTable(catalogName, taxNs, taxTable);
    catalogApi.createTable(catalogName, auditNs, auditTable);

    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName, taxNs, createAzureStorageConfig("tax-azure", "tax-tenant", "tax"))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, taxNs, taxTable)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AzureStorageConfigInfo effective = response.readEntity(AzureStorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.AZURE);
      assertThat(effective.getStorageName()).isEqualTo("tax-azure");
    }

    try (Response response =
        managementApi.getTableStorageConfig(catalogName, auditNs, auditTable)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AwsStorageConfigInfo effective = response.readEntity(AwsStorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.S3);
      assertThat(effective.getStorageName()).isEqualTo("catalog-storage");
    }

    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName,
            taxNs,
            createGcsStorageConfig(
                "tax-gcs", "tax-gcs-bucket", "tax-sa@project.iam.gserviceaccount.com"))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, taxNs, taxTable)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      GcpStorageConfigInfo effective = response.readEntity(GcpStorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.GCS);
      assertThat(effective.getStorageName()).isEqualTo("tax-gcs");
      assertThat(effective.getGcsServiceAccount())
          .isEqualTo("tax-sa@project.iam.gserviceaccount.com");
    }

    try (Response response =
        managementApi.setTableStorageConfig(
            catalogName,
            auditNs,
            auditTable,
            createFileStorageConfig("audit-file", "file:///tmp/polaris/audit/"))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response response =
        managementApi.getTableStorageConfig(catalogName, auditNs, auditTable)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      FileStorageConfigInfo effective = response.readEntity(FileStorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.FILE);
      assertThat(effective.getStorageName()).isEqualTo("audit-file");
    }

    try (Response response = managementApi.getTableStorageConfig(catalogName, taxNs, taxTable)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      GcpStorageConfigInfo effective = response.readEntity(GcpStorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.GCS);
      assertThat(effective.getStorageName()).isEqualTo("tax-gcs");
    }
  }

  /**
   * Test that namespace {@code storageName} is correctly reflected in the effective GET response
   * before and after PUT, and reverts to the catalog's {@code storageName} after DELETE.
   *
   * <p>Task 3 of the storageName × hierarchy test-gap closure (DESIGN.md Q3).
   */
  @Test
  public void testNamespaceStorageNameRoundtripInEffectiveGet() {
    String catalogName = "test_catalog_ns_sn_roundtrip";
    String namespace = "test_namespace";

    managementApi.createCatalog(createS3Catalog(catalogName, "catalog-storage", "catalog-role"));
    catalogApi.createNamespace(catalogName, namespace);

    // Before namespace override: effective config should fall back to catalog storageName
    try (Response response = managementApi.getNamespaceStorageConfig(catalogName, namespace)) {
      assertThat(response.getStatus())
          .as("GET before namespace override should return 200")
          .isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo config = response.readEntity(StorageConfigInfo.class);
      assertThat(config.getStorageName())
          .as("Effective storageName before namespace PUT must equal catalog storageName")
          .isEqualTo("catalog-storage");
    }

    // PUT namespace storage config with storageName="ns-storage"
    AwsStorageConfigInfo nsConfig = createS3StorageConfig("ns-storage", "ns-role");
    try (Response response =
        managementApi.setNamespaceStorageConfig(catalogName, namespace, nsConfig)) {
      assertThat(response.getStatus())
          .as("PUT namespace storage config should return 200")
          .isEqualTo(Response.Status.OK.getStatusCode());
    }

    // After PUT: effective config must now carry namespace storageName
    try (Response response = managementApi.getNamespaceStorageConfig(catalogName, namespace)) {
      assertThat(response.getStatus())
          .as("GET after namespace PUT should return 200")
          .isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo config = response.readEntity(StorageConfigInfo.class);
      assertThat(config.getStorageName())
          .as("Effective storageName after namespace PUT must be 'ns-storage'")
          .isEqualTo("ns-storage");
    }

    // DELETE the namespace storage config
    try (Response response = managementApi.deleteNamespaceStorageConfig(catalogName, namespace)) {
      assertThat(response.getStatus())
          .as("DELETE namespace storage config should return 204")
          .isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    // After DELETE: effective config must revert to catalog storageName
    try (Response response = managementApi.getNamespaceStorageConfig(catalogName, namespace)) {
      assertThat(response.getStatus())
          .as("GET after DELETE should return 200 (falls back to catalog)")
          .isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo config = response.readEntity(StorageConfigInfo.class);
      assertThat(config.getStorageName())
          .as(
              "Effective storageName after DELETE must revert to catalog storageName "
                  + "'catalog-storage'")
          .isEqualTo("catalog-storage");
    }
  }

  /**
   * Test that a table-level {@code storageName} overrides the namespace-level {@code storageName}
   * in the effective GET response for that table.
   *
   * <p>Task 4 of the storageName × hierarchy test-gap closure (DESIGN.md Q3).
   */
  @Test
  public void testTableStorageNameOverridesNamespaceStorageName() {
    String catalogName = "test_catalog_tbl_sn_override";
    String namespace = "test_namespace";
    String table = "test_table";

    managementApi.createCatalog(createS3Catalog(catalogName, "catalog-storage", "catalog-role"));
    catalogApi.createNamespace(catalogName, namespace);
    catalogApi.createTable(catalogName, namespace, table);

    // PUT namespace storage config
    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName, namespace, createS3StorageConfig("ns-storage", "ns-role"))) {
      assertThat(response.getStatus())
          .as("PUT namespace storage config should return 200")
          .isEqualTo(Response.Status.OK.getStatusCode());
    }

    // Before table override: effective config for the table must carry namespace storageName
    try (Response response = managementApi.getTableStorageConfig(catalogName, namespace, table)) {
      assertThat(response.getStatus())
          .as("GET table config before table PUT should return 200")
          .isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo config = response.readEntity(StorageConfigInfo.class);
      assertThat(config.getStorageName())
          .as("Effective table storageName before table PUT must equal namespace storageName")
          .isEqualTo("ns-storage");
    }

    // PUT table storage config with storageName="table-storage"
    try (Response response =
        managementApi.setTableStorageConfig(
            catalogName, namespace, table, createS3StorageConfig("table-storage", "table-role"))) {
      assertThat(response.getStatus())
          .as("PUT table storage config should return 200")
          .isEqualTo(Response.Status.OK.getStatusCode());
    }

    // After table PUT: effective config must carry table-level storageName
    try (Response response = managementApi.getTableStorageConfig(catalogName, namespace, table)) {
      assertThat(response.getStatus())
          .as("GET table config after table PUT should return 200")
          .isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo config = response.readEntity(StorageConfigInfo.class);
      assertThat(config.getStorageName())
          .as(
              "Effective table storageName after table PUT must be 'table-storage', "
                  + "overriding the namespace-level 'ns-storage'")
          .isEqualTo("table-storage");
      assertThat(config.getStorageName())
          .as("Namespace storageName 'ns-storage' must not leak through when table has its own")
          .isNotEqualTo("ns-storage");
    }
  }

  /**
   * Test that DELETE-ing a table storage config reverts the effective config back to the
   * namespace-level {@code storageName}.
   *
   * <p>Task 5 of the storageName × hierarchy test-gap closure (DESIGN.md Q3).
   */
  @Test
  public void testDeleteTableStorageConfigRevertsToNamespaceStorageName() {
    String catalogName = "test_catalog_tbl_sn_delete_revert";
    String namespace = "test_namespace";
    String table = "test_table";

    managementApi.createCatalog(createS3Catalog(catalogName, "catalog-storage", "catalog-role"));
    catalogApi.createNamespace(catalogName, namespace);
    catalogApi.createTable(catalogName, namespace, table);

    // PUT namespace storage config with storageName="ns-storage"
    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName, namespace, createS3StorageConfig("ns-storage", "ns-role"))) {
      assertThat(response.getStatus())
          .as("PUT namespace storage config should return 200")
          .isEqualTo(Response.Status.OK.getStatusCode());
    }

    // PUT table storage config with storageName="table-storage"
    try (Response response =
        managementApi.setTableStorageConfig(
            catalogName, namespace, table, createS3StorageConfig("table-storage", "table-role"))) {
      assertThat(response.getStatus())
          .as("PUT table storage config should return 200")
          .isEqualTo(Response.Status.OK.getStatusCode());
    }

    // DELETE the table storage config
    try (Response response =
        managementApi.deleteTableStorageConfig(catalogName, namespace, table)) {
      assertThat(response.getStatus())
          .as("DELETE table storage config should return 204")
          .isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    // After DELETE: table must revert to the namespace-level storageName="ns-storage"
    try (Response response = managementApi.getTableStorageConfig(catalogName, namespace, table)) {
      assertThat(response.getStatus())
          .as("GET table config after DELETE should return 200")
          .isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo config = response.readEntity(StorageConfigInfo.class);
      assertThat(config.getStorageName())
          .as(
              "After DELETE of table storage config, effective storageName must revert to "
                  + "the namespace-level 'ns-storage', not the catalog-level 'catalog-storage'")
          .isEqualTo("ns-storage");
      assertThat(config.getStorageName())
          .as("Deleted table storageName 'table-storage' must no longer be returned")
          .isNotEqualTo("table-storage");
    }
  }

  private static AwsStorageConfigInfo createS3StorageConfig(String storageName, String roleName) {
    String roleArn =
        System.getProperty(S3_ROLE_ARN_PROPERTY, "arn:aws:iam::123456789012:role/" + roleName);

    AwsStorageConfigInfo.Builder builder =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(s3BaseLocation()))
            .setRoleArn(roleArn);

    if (storageName != null) {
      builder.setStorageName(storageName);
    }

    String endpoint = System.getProperty(S3_ENDPOINT_PROPERTY);
    if (endpoint != null && !endpoint.isBlank()) {
      builder.setEndpoint(endpoint);
      builder.setPathStyleAccess(
          Boolean.parseBoolean(System.getProperty(S3_PATH_STYLE_ACCESS_PROPERTY, "false")));
    }

    return builder.build();
  }

  private static AzureStorageConfigInfo createAzureStorageConfig(
      String storageName, String tenantId, String containerName) {
    return AzureStorageConfigInfo.builder()
        .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
        .setAllowedLocations(List.of("abfss://" + containerName + "@storage.dfs.core.windows.net/"))
        .setStorageName(storageName)
        .setTenantId(tenantId)
        .build();
  }

  private static GcpStorageConfigInfo createGcsStorageConfig(
      String storageName, String bucketName, String serviceAccount) {
    return GcpStorageConfigInfo.builder()
        .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
        .setAllowedLocations(List.of("gs://" + bucketName + "/"))
        .setStorageName(storageName)
        .setGcsServiceAccount(serviceAccount)
        .build();
  }

  private static FileStorageConfigInfo createFileStorageConfig(
      String storageName, String location) {
    return FileStorageConfigInfo.builder()
        .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
        .setAllowedLocations(List.of(location))
        .setStorageName(storageName)
        .build();
  }

  private static PolarisCatalog createS3Catalog(
      String catalogName, String storageName, String roleName) {
    return PolarisCatalog.builder()
        .setType(Catalog.TypeEnum.INTERNAL)
        .setName(catalogName)
        .setProperties(new CatalogProperties(s3BaseLocation()))
        .setStorageConfigInfo(createS3StorageConfig(storageName, roleName))
        .build();
  }

  private static String s3BaseLocation() {
    return System.getProperty(S3_BASE_LOCATION_PROPERTY, DEFAULT_S3_BASE_LOCATION);
  }

  private static void requireS3DataPlane() {
    Assumptions.assumeTrue(
        Boolean.parseBoolean(System.getProperty(S3_DATA_PLANE_ENABLED_PROPERTY, "false")),
        "S3 data-plane tests require MinIO/AWS configuration");
  }
}
