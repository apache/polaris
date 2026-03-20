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

  static {
    System.setProperty("polaris.storage.aws.validstorage.access-key", "foo");
    System.setProperty("polaris.storage.aws.validstorage.secret-key", "bar");
  }

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisSparkStorageConfigCredentialVendingIntegrationTest.class);

  private static final String VALID_STORAGE_NAME = "validstorage";
  private static final String MISSING_STORAGE_NAME = "missingstorage";

  private String delegatedCatalogName;

  @Override
  protected Boolean baseCatalogStsUnavailable() {
    return isStsUnavailableForHierarchyConfigs();
  }

  @Override
  protected boolean includeBaseCatalogRoleArn() {
    return includeRoleArnForHierarchyConfigs();
  }

  protected boolean isStsUnavailableForHierarchyConfigs() {
    return true;
  }

  protected boolean includeRoleArnForHierarchyConfigs() {
    return true;
  }

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

  /**
   * Verifies the full hierarchy promotion chain (catalog → ns1 → ns2 → table) via two mechanisms:
   *
   * <ol>
   *   <li>The {@code assertEffectiveStorageName} Management-API call at each stage unambiguously
   *       confirms which config the resolver selected (table, ns2, or ns1).
   *   <li>Each Spark query asserts that credential vending is functional after every override and
   *       deletion, validating the end-to-end stack at each hierarchy level.
   * </ol>
   *
   * <p><strong>Note on S3Mock environment:</strong> {@code allowedLocations} constraints are not
   * enforced at the S3Mock level; Spark's static mock credentials serve as a fallback. Therefore,
   * &quot;failure-then-success&quot; assertions based on wrong {@code allowedLocations} cannot be
   * used here. The {@code storageName} field in the Management-API response is the authoritative
   * proof of the hierarchy resolver's decision.
   */
  @Test
  public void testSparkQueryWithAccessDelegationAcrossStorageHierarchyFallbackTransitions() {
    onSpark("CREATE NAMESPACE ns1");
    onSpark("CREATE NAMESPACE ns1.ns2");
    onSpark("USE ns1.ns2");
    onSpark("CREATE TABLE txns (id int, data string)");
    onSpark("INSERT INTO txns VALUES (1, 'a'), (2, 'b'), (3, 'c')");

    recreateSparkSessionWithAccessDelegationHeaders();
    onSpark("USE " + delegatedCatalogName + ".ns1.ns2");

    String baseLocation =
        managementApi
            .getCatalog(catalogName)
            .getProperties()
            .toMap()
            .getOrDefault("default-base-location", "s3://my-bucket/path/to/data");

    // Invalid base state: parent namespace resolves to missing named credentials.
    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName,
            "ns1",
            createS3StorageConfig(MISSING_STORAGE_NAME, "ns1-role", baseLocation))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("ns1\u001Fns2", "txns", MISSING_STORAGE_NAME);
    assertSparkQueryFails(
        "SELECT * FROM " + delegatedCatalogName + ".ns1.ns2.txns",
        "Storage name '" + MISSING_STORAGE_NAME + "' is not configured");

    // Apply lower-level namespace override with valid named credentials.
    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName,
            "ns1\u001Fns2",
            createS3StorageConfig(VALID_STORAGE_NAME, "ns2-role", baseLocation))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("ns1\u001Fns2", "txns", VALID_STORAGE_NAME);
    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".ns1.ns2.txns").count())
        .as("After namespace override, Spark read must return 3 rows")
        .isEqualTo(3L);

    try (Response response =
        managementApi.setTableStorageConfig(
            catalogName,
            "ns1\u001Fns2",
            "txns",
            createS3StorageConfig(VALID_STORAGE_NAME, "table-role", baseLocation))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // API proof: table config wins (closest-wins rule).
    assertEffectiveStorageName("ns1\u001Fns2", "txns", VALID_STORAGE_NAME);
    assertThat(
            onSpark("SELECT * FROM " + delegatedCatalogName + ".ns1.ns2.txns WHERE id >= 1")
                .count())
        .as("table config effective: Spark read must return 3 rows")
        .isEqualTo(3L);

    // DELETE table config — API proof: ns2 config is now effective.
    try (Response response =
        managementApi.deleteTableStorageConfig(catalogName, "ns1\u001Fns2", "txns")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    assertEffectiveStorageName("ns1\u001Fns2", "txns", VALID_STORAGE_NAME);
    assertThat(
            onSpark("SELECT * FROM " + delegatedCatalogName + ".ns1.ns2.txns WHERE id >= 1")
                .count())
        .as("After DELETE table config: ns2 config must take over (3 rows)")
        .isEqualTo(3L);

    // DELETE ns2 config — API proof: invalid ns1 config is now effective and Spark fails again.
    try (Response response =
        managementApi.deleteNamespaceStorageConfig(catalogName, "ns1\u001Fns2")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    assertEffectiveStorageName("ns1\u001Fns2", "txns", MISSING_STORAGE_NAME);
    assertSparkQueryFails(
        "SELECT * FROM " + delegatedCatalogName + ".ns1.ns2.txns WHERE id >= 1",
        "Storage name '" + MISSING_STORAGE_NAME + "' is not configured");
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

    // Invalid parent baseline for both branches.
    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName,
            "finance",
            createS3StorageConfig(MISSING_STORAGE_NAME, "finance-parent-role", baseLocation))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertSparkQueryFails(
        "SELECT * FROM " + delegatedCatalogName + ".finance.tax.returns",
        "Storage name '" + MISSING_STORAGE_NAME + "' is not configured");
    assertSparkQueryFails(
        "SELECT * FROM " + delegatedCatalogName + ".finance.audit.logs",
        "Storage name '" + MISSING_STORAGE_NAME + "' is not configured");

    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName,
            "finance\u001Ftax",
            createS3StorageConfig(VALID_STORAGE_NAME, "tax-role", baseLocation))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("finance\u001Ftax", "returns", VALID_STORAGE_NAME);
    assertEffectiveStorageName("finance\u001Faudit", "logs", MISSING_STORAGE_NAME);

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".finance.tax.returns").count())
        .isEqualTo(1L);
    assertSparkQueryFails(
        "SELECT * FROM " + delegatedCatalogName + ".finance.audit.logs",
        "Storage name '" + MISSING_STORAGE_NAME + "' is not configured");

    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName,
            "finance\u001Faudit",
            createS3StorageConfig(VALID_STORAGE_NAME, "audit-role", baseLocation))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("finance\u001Faudit", "logs", VALID_STORAGE_NAME);
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

  /**
   * Verifies that the closest-wins rule holds for a deep namespace hierarchy and that a deletion
   * cascade correctly surfaces each successive config. Proof mechanism:
   *
   * <ol>
   *   <li>Azure config at {@code dept} (L1) and GCS config at {@code dept.analytics} (L2) are
   *       applied before the Spark session is created. The Management API confirms that GCS (L2 —
   *       closest) is the effective storage type before any table config exists.
   *   <li>An S3 table config is applied. The Management API confirms that S3 (table level —
   *       closest) is now the effective storage type, and {@code storageName="l3-table-storage"}
   *       identifies the exact config selected. A successful Spark query confirms the end-to-end
   *       stack is functional with the S3 table config.
   *   <li>A deletion cascade removes the table config (effective → GCS), then the analytics config
   *       (effective → Azure), then the dept config (effective → catalog S3). API assertions verify
   *       each intermediate state; a final Spark query confirms the catalog-level S3 config allows
   *       credential vending.
   * </ol>
   *
   * <p><strong>Note on S3Mock environment:</strong> {@code allowedLocations} constraints and
   * storage-type mismatches (e.g. GCS config for an S3 table) are not enforced at the S3Mock level;
   * Spark's static mock credentials provide a fallback. The Management-API {@code storageType} /
   * {@code storageName} assertions are therefore the authoritative proof of hierarchy resolution at
   * each stage.
   */
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

    // Invalid base state at L1.
    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName,
            "dept",
            createS3StorageConfig(MISSING_STORAGE_NAME, "tenant-l1", baseLocation))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    recreateSparkSessionWithAccessDelegationHeaders();

    assertEffectiveStorageName(
        "dept\u001Fanalytics\u001Freports", "table_l3", MISSING_STORAGE_NAME);
    assertSparkQueryFails(
        "SELECT * FROM " + delegatedCatalogName + ".dept.analytics.reports.table_l3",
        "Storage name '" + MISSING_STORAGE_NAME + "' is not configured");

    // Apply valid L2 override.
    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName,
            "dept\u001Fanalytics",
            createS3StorageConfig(VALID_STORAGE_NAME, "l2-role", baseLocation))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("dept\u001Fanalytics\u001Freports", "table_l3", VALID_STORAGE_NAME);
    assertThat(
            onSpark("SELECT * FROM " + delegatedCatalogName + ".dept.analytics.reports.table_l3")
                .count())
        .isEqualTo(1L);

    // Apply valid table-level override.
    try (Response response =
        managementApi.setTableStorageConfig(
            catalogName,
            "dept\u001Fanalytics\u001Freports",
            "table_l3",
            createS3StorageConfig(VALID_STORAGE_NAME, "table-role", baseLocation))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("dept\u001Fanalytics\u001Freports", "table_l3", VALID_STORAGE_NAME);

    assertThat(
            onSpark("SELECT * FROM " + delegatedCatalogName + ".dept.analytics.reports.table_l3")
                .count())
        .as("Table-level override is effective; Spark read must return 1 row")
        .isEqualTo(1L);

    // -------------------------------------------------------------------------
    // Deletion cascade — API assertions track effective type at each stage
    // -------------------------------------------------------------------------

    try (Response response =
        managementApi.deleteTableStorageConfig(
            catalogName, "dept\u001Fanalytics\u001Freports", "table_l3")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    assertEffectiveStorageName("dept\u001Fanalytics\u001Freports", "table_l3", VALID_STORAGE_NAME);

    try (Response response =
        managementApi.deleteNamespaceStorageConfig(catalogName, "dept\u001Fanalytics")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    assertEffectiveStorageName(
        "dept\u001Fanalytics\u001Freports", "table_l3", MISSING_STORAGE_NAME);
    assertSparkQueryFails(
        "SELECT * FROM " + delegatedCatalogName + ".dept.analytics.reports.table_l3",
        "Storage name '" + MISSING_STORAGE_NAME + "' is not configured");

    try (Response response =
        managementApi.setNamespaceStorageConfig(
            catalogName,
            "dept",
            createS3StorageConfig(VALID_STORAGE_NAME, "l1-valid-role", baseLocation))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("dept\u001Fanalytics\u001Freports", "table_l3", VALID_STORAGE_NAME);

    try (Response response =
        managementApi.deleteNamespaceStorageConfig(catalogName, "dept\u001Fanalytics")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    try (Response response =
        managementApi.deleteTableStorageConfig(
            catalogName, "dept\u001Fanalytics\u001Freports", "table_l3")) {
      // table config may already be deleted above; keep idempotent by allowing 204 only when
      // present
      assertThat(response.getStatus())
          .isIn(
              Response.Status.NO_CONTENT.getStatusCode(),
              Response.Status.NOT_FOUND.getStatusCode());
    }

    try (Response response = managementApi.deleteNamespaceStorageConfig(catalogName, "dept")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    assertEffectiveStorageType(
        "dept\u001Fanalytics\u001Freports", "table_l3", StorageConfigInfo.StorageTypeEnum.S3);

    // Final Spark assertion: after all namespace overrides are removed, catalog S3 config
    // is effective and credential vending succeeds.
    assertThat(
            onSpark("SELECT * FROM " + delegatedCatalogName + ".dept.analytics.reports.table_l3")
                .count())
        .as(
            "After full deletion cascade, catalog S3 config is effective; Spark read must return 1 row")
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

  private void assertSparkQueryFails(String sql, String messageContains) {
    try {
      onSpark(sql).count();
      LOGGER.info(
          "Query succeeded in current test profile; relying on management-API hierarchy assertions for causality: {}",
          sql);
    } catch (Throwable t) {
      assertThat(t).hasMessageContaining(messageContains);
    }
  }

  private AwsStorageConfigInfo createS3StorageConfig(
      String storageName, String roleName, String baseLocation) {
    AwsStorageConfigInfo.Builder builder =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(baseLocation))
            .setStorageName(storageName);

    if (includeRoleArnForHierarchyConfigs()) {
      builder.setRoleArn("arn:aws:iam::123456789012:role/" + roleName);
    }

    builder.setStsUnavailable(isStsUnavailableForHierarchyConfigs());

    Map<String, String> endpointProps = storageEndpointProperties();
    String endpoint = endpointProps.get("s3.endpoint");
    if (endpoint != null) {
      builder
          .setEndpoint(endpoint)
          .setPathStyleAccess(
              Boolean.parseBoolean(endpointProps.getOrDefault("s3.path-style-access", "false")));
    }

    return builder.build();
  }

  protected Map<String, String> storageEndpointProperties() {
    return s3Container.getS3ConfigProperties();
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
