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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.ext.PolarisSparkIntegrationTestBase;
import org.apache.polaris.service.it.ext.SparkSessionBuilder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolarisSparkStorageConfigCredentialVendingIntegrationTest
    extends PolarisSparkIntegrationTestBase {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisSparkStorageConfigCredentialVendingIntegrationTest.class);

  private String delegatedCatalogName;

  @Override
  @AfterEach
  public void after() throws Exception {
    try {
      cleanupCatalogWithNestedNamespaces(catalogName);
    } catch (Exception e) {
      LOGGER.warn("Failed to cleanup catalog {}", catalogName, e);
    }

    try {
      cleanupCatalogWithNestedNamespaces(externalCatalogName);
    } catch (Exception e) {
      LOGGER.warn("Failed to cleanup catalog {}", externalCatalogName, e);
    }

    try {
      SparkSession.clearDefaultSession();
      SparkSession.clearActiveSession();
      spark.close();
    } catch (Exception e) {
      LOGGER.error("Unable to close spark session", e);
    }

    client.close();
  }

  @Test
  public void testSparkQueryWithAccessDelegationAcrossStorageHierarchyFallbackTransitions() {
    onSpark("CREATE NAMESPACE ns1");
    onSpark("CREATE NAMESPACE ns1.ns2");
    onSpark("USE ns1.ns2");
    onSpark("CREATE TABLE txns (id int, data string)");
    onSpark("INSERT INTO txns VALUES (1, 'a'), (2, 'b'), (3, 'c')");

    recreateSparkSessionWithAccessDelegationHeaders();
    onSpark("USE " + delegatedCatalogName + ".ns1.ns2");

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".ns1.ns2.txns").count())
        .isEqualTo(3L);

    String baseLocation =
        managementApi
            .getCatalog(catalogName)
            .getProperties()
            .toMap()
            .getOrDefault("default-base-location", "s3://my-bucket/path/to/data");

    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName, "ns1", createS3StorageConfig("ns1-storage", "ns1-role", baseLocation))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName,
            "ns1\u001Fns2",
            createS3StorageConfig("ns2-storage", "ns2-role", baseLocation))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response response =
        managementApi.setTableStorageConfig(
            catalogName,
            "ns1\u001Fns2",
            "txns",
            createS3StorageConfig("table-storage", "table-role", baseLocation))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("ns1\u001Fns2", "txns", "table-storage");
    assertThat(
            onSpark("SELECT * FROM " + delegatedCatalogName + ".ns1.ns2.txns WHERE id >= 1")
                .count())
        .isEqualTo(3L);

    try (Response response =
        managementApi.deleteTableStorageConfig(catalogName, "ns1\u001Fns2", "txns")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    assertEffectiveStorageName("ns1\u001Fns2", "txns", "ns2-storage");
    assertThat(
            onSpark("SELECT * FROM " + delegatedCatalogName + ".ns1.ns2.txns WHERE id >= 1")
                .count())
        .isEqualTo(3L);

    try (Response response =
        managementApi.deleteNamespaceStorageConfig(catalogName, "ns1\u001Fns2")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    assertEffectiveStorageName("ns1\u001Fns2", "txns", "ns1-storage");
    assertThat(
            onSpark("SELECT * FROM " + delegatedCatalogName + ".ns1.ns2.txns WHERE id >= 1")
                .count())
        .isEqualTo(3L);
  }

  @Test
  public void testSparkQuerySiblingBranchIsolationWithAccessDelegation() {
    onSpark("CREATE NAMESPACE finance");
    onSpark("CREATE NAMESPACE finance.tax");
    onSpark("CREATE NAMESPACE finance.audit");
    onSpark("CREATE TABLE finance.tax.returns (id int, data string)");
    onSpark("CREATE TABLE finance.audit.logs (id int, data string)");
    onSpark("INSERT INTO finance.tax.returns VALUES (1, 'tax')");
    onSpark("INSERT INTO finance.audit.logs VALUES (1, 'audit')");

    recreateSparkSessionWithAccessDelegationHeaders();
    onSpark("USE " + delegatedCatalogName);

    String baseLocation =
        managementApi
            .getCatalog(catalogName)
            .getProperties()
            .toMap()
            .getOrDefault("default-base-location", "s3://my-bucket/path/to/data");

    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName,
            "finance\u001Ftax",
            createAzureStorageConfig("tax-azure", "tax-tenant", "finance-tax"))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageType(
        "finance\u001Ftax", "returns", StorageConfigInfo.StorageTypeEnum.AZURE);

    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName,
            "finance\u001Ftax",
            createGcsStorageConfig(
                "tax-gcs", "finance-tax", "tax-sa@project.iam.gserviceaccount.com"))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageType(
        "finance\u001Ftax", "returns", StorageConfigInfo.StorageTypeEnum.GCS);

    try (Response response =
        managementApi.getTableStorageConfig(catalogName, "finance\u001Faudit", "logs")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo effective = response.readEntity(StorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.S3);
    }

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".finance.tax.returns").count())
        .isEqualTo(1L);
    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".finance.audit.logs").count())
        .isEqualTo(1L);
  }

  @Test
  public void testSparkQueryCatalogOnlyFallbackAcrossDepths() {
    onSpark("CREATE NAMESPACE team");
    onSpark("CREATE NAMESPACE team.core");
    onSpark("CREATE NAMESPACE team.core.warehouse");
    onSpark("CREATE TABLE team.orders (id int, data string)");
    onSpark("CREATE TABLE team.core.line_items (id int, data string)");
    onSpark("CREATE TABLE team.core.warehouse.daily_rollup (id int, data string)");
    onSpark("INSERT INTO team.orders VALUES (1, 'o1')");
    onSpark("INSERT INTO team.core.line_items VALUES (1, 'l1')");
    onSpark("INSERT INTO team.core.warehouse.daily_rollup VALUES (1, 'd1')");

    recreateSparkSessionWithAccessDelegationHeaders();
    onSpark("USE " + delegatedCatalogName);

    assertEffectiveStorageType("team", "orders", StorageConfigInfo.StorageTypeEnum.S3);
    assertEffectiveStorageType(
        "team\u001Fcore", "line_items", StorageConfigInfo.StorageTypeEnum.S3);
    assertEffectiveStorageType(
        "team\u001Fcore\u001Fwarehouse", "daily_rollup", StorageConfigInfo.StorageTypeEnum.S3);

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".team.orders").count())
        .isEqualTo(1L);
    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".team.core.line_items").count())
        .isEqualTo(1L);
    assertThat(
            onSpark("SELECT * FROM " + delegatedCatalogName + ".team.core.warehouse.daily_rollup")
                .count())
        .isEqualTo(1L);
  }

  @Test
  public void testSparkQueryDeepHierarchyClosestWinsAndDeleteTransitions() {
    onSpark("CREATE NAMESPACE dept");
    onSpark("CREATE NAMESPACE dept.analytics");
    onSpark("CREATE NAMESPACE dept.analytics.reports");
    onSpark("CREATE TABLE dept.analytics.reports.table_l3 (id int, data string)");
    onSpark("INSERT INTO dept.analytics.reports.table_l3 VALUES (1, 'r1')");

    String baseLocation =
        managementApi
            .getCatalog(catalogName)
            .getProperties()
            .toMap()
            .getOrDefault("default-base-location", "s3://my-bucket/path/to/data");

    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName, "dept", createAzureStorageConfig("l1-storage", "tenant-l1", "l1"))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName,
            "dept\u001Fanalytics",
            createGcsStorageConfig(
                "l2-storage", "dept-analytics", "l2-sa@project.iam.gserviceaccount.com"))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response response =
        managementApi.setTableStorageConfig(
            catalogName,
            "dept\u001Fanalytics\u001Freports",
            "table_l3",
            createS3StorageConfig("l3-table-storage", "table-role", baseLocation))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    recreateSparkSessionWithAccessDelegationHeaders();

    assertEffectiveStorageName("dept\u001Fanalytics\u001Freports", "table_l3", "l3-table-storage");
    assertThat(
            onSpark("SELECT * FROM " + delegatedCatalogName + ".dept.analytics.reports.table_l3")
                .count())
        .isEqualTo(1L);

    try (Response response =
        managementApi.deleteTableStorageConfig(
            catalogName, "dept\u001Fanalytics\u001Freports", "table_l3")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    assertEffectiveStorageType(
        "dept\u001Fanalytics\u001Freports", "table_l3", StorageConfigInfo.StorageTypeEnum.GCS);

    try (Response response =
        managementApi.deleteNamespaceStorageConfig(catalogName, "dept\u001Fanalytics")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    assertEffectiveStorageType(
        "dept\u001Fanalytics\u001Freports", "table_l3", StorageConfigInfo.StorageTypeEnum.AZURE);

    try (Response response = managementApi.deleteNamespaceStorageConfig(catalogName, "dept")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    assertEffectiveStorageType(
        "dept\u001Fanalytics\u001Freports", "table_l3", StorageConfigInfo.StorageTypeEnum.S3);

    assertThat(
            onSpark("SELECT * FROM " + delegatedCatalogName + ".dept.analytics.reports.table_l3")
                .count())
        .isEqualTo(1L);
  }

  @Test
  public void testSparkDelegatedReadForbiddenForUnauthorizedPrincipal() {
    onSpark("CREATE NAMESPACE blocked");
    onSpark("CREATE TABLE blocked.t1 (id int, data string)");
    onSpark("INSERT INTO blocked.t1 VALUES (1, 'x')");

    PrincipalWithCredentials unauthorizedPrincipal =
        managementApi.createPrincipal(client.newEntityName("spark_unauth_user"));
    String unauthorizedToken = client.obtainToken(unauthorizedPrincipal);
    String unauthorizedCatalogName = client.newEntityName("spark_unauth_catalog");

    SparkSession unauthorizedSpark =
        SparkSessionBuilder.buildWithTestDefaults()
            .withWarehouse(warehouseDir)
            .withConfig(
                "spark.sql.catalog."
                    + unauthorizedCatalogName
                    + ".header.X-Iceberg-Access-Delegation",
                "vended-credentials")
            .withConfig("spark.sql.catalog." + unauthorizedCatalogName + ".cache-enabled", "false")
            .withConfig("spark.sql.catalog." + unauthorizedCatalogName + ".warehouse", catalogName)
            .addCatalog(
                unauthorizedCatalogName,
                "org.apache.iceberg.spark.SparkCatalog",
                endpoints,
                unauthorizedToken)
            .getOrCreate();

    try {
      assertThatThrownBy(
              () ->
                  unauthorizedSpark
                      .sql("SELECT * FROM " + unauthorizedCatalogName + ".blocked.t1")
                      .count())
          .hasMessageContaining("not authorized");
    } finally {
      try {
        unauthorizedSpark.close();
      } catch (Exception e) {
        LOGGER.warn("Unable to close unauthorized spark session", e);
      }
    }
  }

  private void assertEffectiveStorageName(
      String namespace, String table, String expectedStorageName) {
    try (Response response = managementApi.getTableStorageConfig(catalogName, namespace, table)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AwsStorageConfigInfo effective = response.readEntity(AwsStorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.S3);
      assertThat(effective.getStorageName()).isEqualTo(expectedStorageName);
    }
  }

  private void assertEffectiveStorageType(
      String namespace, String table, StorageConfigInfo.StorageTypeEnum expectedStorageType) {
    try (Response response = managementApi.getTableStorageConfig(catalogName, namespace, table)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo effective = response.readEntity(StorageConfigInfo.class);
      assertThat(effective.getStorageType()).isEqualTo(expectedStorageType);
    }
  }

  private AwsStorageConfigInfo createS3StorageConfig(
      String storageName, String roleName, String baseLocation) {
    AwsStorageConfigInfo.Builder builder =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(baseLocation))
            .setRoleArn("arn:aws:iam::123456789012:role/" + roleName)
            .setStorageName(storageName);

    Map<String, String> endpointProps = s3Container.getS3ConfigProperties();
    String endpoint = endpointProps.get("s3.endpoint");
    if (endpoint != null) {
      builder
          .setEndpoint(endpoint)
          .setPathStyleAccess(
              Boolean.parseBoolean(endpointProps.getOrDefault("s3.path-style-access", "false")));
    }

    return builder.build();
  }

  private AzureStorageConfigInfo createAzureStorageConfig(
      String storageName, String tenantId, String containerName) {
    return AzureStorageConfigInfo.builder()
        .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
        .setAllowedLocations(List.of("abfss://" + containerName + "@storage.dfs.core.windows.net/"))
        .setStorageName(storageName)
        .setTenantId(tenantId)
        .build();
  }

  private GcpStorageConfigInfo createGcsStorageConfig(
      String storageName, String bucketName, String serviceAccount) {
    return GcpStorageConfigInfo.builder()
        .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
        .setAllowedLocations(List.of("gs://" + bucketName + "/"))
        .setStorageName(storageName)
        .setGcsServiceAccount(serviceAccount)
        .build();
  }

  private void recreateSparkSessionWithAccessDelegationHeaders() {
    String principalName = client.newEntityName("spark_delegate_user");
    String principalRoleName = client.newEntityName("spark_delegate_role");
    PrincipalWithCredentials delegatedPrincipal =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);
    managementApi.makeAdmin(principalRoleName, managementApi.getCatalog(catalogName));
    sparkToken = client.obtainToken(delegatedPrincipal);
    delegatedCatalogName = client.newEntityName("spark_delegate_catalog");

    SparkSession.clearDefaultSession();
    SparkSession.clearActiveSession();
    spark.close();

    spark =
        SparkSessionBuilder.buildWithTestDefaults()
            .withWarehouse(warehouseDir)
            .withConfig(
                "spark.sql.catalog." + catalogName + ".header.X-Iceberg-Access-Delegation",
                "vended-credentials")
            .withConfig(
                "spark.sql.catalog." + delegatedCatalogName + ".header.X-Iceberg-Access-Delegation",
                "vended-credentials")
            .withConfig("spark.sql.catalog." + catalogName + ".cache-enabled", "false")
            .withConfig("spark.sql.catalog." + delegatedCatalogName + ".cache-enabled", "false")
            .withConfig("spark.sql.catalog." + delegatedCatalogName + ".warehouse", catalogName)
            .addCatalog(catalogName, "org.apache.iceberg.spark.SparkCatalog", endpoints, sparkToken)
            .addCatalog(
                delegatedCatalogName,
                "org.apache.iceberg.spark.SparkCatalog",
                endpoints,
                sparkToken)
            .getOrCreate();
  }

  private void cleanupCatalogWithNestedNamespaces(String targetCatalogName) {
    catalogApi.purge(targetCatalogName);
    managementApi.dropCatalog(targetCatalogName);
  }
}
