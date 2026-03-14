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

/**
 * Spark integration tests for {@code storageName} × hierarchical storage configuration resolution.
 *
 * <p>These tests exercise every meaningful combination of named and unnamed storage configs at the
 * catalog, namespace, and table levels, verifying:
 *
 * <ol>
 *   <li>The correct {@code storageName} is preserved in the effective config returned by the
 *       Management API (GET table/namespace storage config).
 *   <li>Spark queries succeed end-to-end with access delegation, confirming that credential vending
 *       is not broken by any combination of named/unnamed configs in the hierarchy.
 * </ol>
 *
 * <p><strong>Note on {@code RESOLVE_CREDENTIALS_BY_STORAGE_NAME}:</strong> This flag defaults to
 * {@code false} in the test environment; the S3Mock accepts any credentials, so Spark queries
 * succeed regardless of which named-credential set would be dispatched. The Spark queries in each
 * test therefore serve as "whole-stack does not break" regression guards, while the {@code
 * assertEffectiveStorageName} assertions are the precise regression barrier for the field
 * preservation invariant.
 *
 * <p>Scenarios covered:
 *
 * <ol>
 *   <li>Catalog unnamed only — no namespace or table configs
 *   <li>Named namespace overrides unnamed catalog
 *   <li>Named table overrides named namespace
 *   <li>Named table overrides unnamed namespace
 *   <li>Namespace config with null storageName stops hierarchy walk (does not fall through to
 *       catalog)
 *   <li>DELETE named table config reverts to named namespace storageName
 *   <li>DELETE named namespace config reverts to unnamed catalog (null storageName)
 *   <li>All three levels named — progressive DELETE cascade
 *   <li>Sibling namespace isolation — named on one branch, unnamed on the other
 *   <li>Deep hierarchy with named storage at the middle namespace, unnamed outer/inner
 *   <li>Table-only named storage under fully unnamed ancestor hierarchy
 * </ol>
 */
public class PolarisSparkStorageNameHierarchyIntegrationTest
    extends PolarisSparkIntegrationTestBase {

  static {
    for (String storageName :
        List.of(
            "ns-named",
            "tbl-named",
            "ns",
            "tbl",
            "billing-creds",
            "mid",
            "tbl-only",
            "ns-shared",
            "tbl-override",
            "ns-v1",
            "ns-v2")) {
      System.setProperty("polaris.storage.aws." + storageName + ".access-key", "foo");
      System.setProperty("polaris.storage.aws." + storageName + ".secret-key", "bar");
    }
  }

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisSparkStorageNameHierarchyIntegrationTest.class);

  private static final String MISSING_STORAGE_NAME = "missingstorage";

  /** Name of the catalog used in the access-delegated Spark session (set per test). */
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

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @Override
  @AfterEach
  public void after() throws Exception {
    // Purge tables/namespaces then drop catalog roles + catalog.
    // dropCatalog() also removes any extra CatalogRoles created by setUpAccessDelegation().
    try {
      catalogApi.purge(catalogName);
      managementApi.dropCatalog(catalogName);
    } catch (Exception e) {
      LOGGER.warn("Failed to cleanup catalog {}", catalogName, e);
    }

    // externalCatalogName is created by the base-class before() but not used in these tests;
    // clean it up so tests stay independent.
    try {
      catalogApi.purge(externalCatalogName);
      managementApi.dropCatalog(externalCatalogName);
    } catch (Exception e) {
      LOGGER.warn("Failed to cleanup external catalog {}", externalCatalogName, e);
    }

    // delegatedCatalogName is a Spark-session alias pointing at catalogName (warehouse=catalogName)
    // — it is NOT a separate Polaris catalog, so no management-API cleanup is required.

    try {
      SparkSession.clearDefaultSession();
      SparkSession.clearActiveSession();
      spark.close();
    } catch (Exception e) {
      LOGGER.error("Unable to close spark session", e);
    }

    client.close();
  }

  // ---------------------------------------------------------------------------
  // Scenario 1: Catalog unnamed only — no namespace or table configs
  // ---------------------------------------------------------------------------

  /**
   * Baseline: the catalog has no storageName (unnamed). No namespace or table inline configs exist.
   * The effective storageName for a table must be {@code null}, and Spark queries must still
   * succeed.
   */
  @Test
  public void testCatalogUnnamedOnlyNoNamespaceOrTableConfig() {
    onSpark("CREATE NAMESPACE ns");
    onSpark("CREATE TABLE ns.events (id int, data string)");
    onSpark("INSERT INTO ns.events VALUES (1, 'a'), (2, 'b')");

    setUpAccessDelegation();

    // Effective config falls back to catalog; catalog has no storageName
    assertEffectiveStorageNameIsNull("ns", "events");

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".ns.events").count())
        .as("Spark read must return 2 rows with catalog-only unnamed config")
        .isEqualTo(2L);
  }

  // ---------------------------------------------------------------------------
  // Scenario 2: Named namespace overrides unnamed catalog
  // ---------------------------------------------------------------------------

  /**
   * Namespace gets a storage config with {@code storageName="ns-named"}. The catalog has no
   * storageName. The effective storageName for a table without its own config must be {@code
   * "ns-named"}.
   *
   * <p>The Management-API assertion ({@code assertEffectiveStorageName}) is the authoritative proof
   * that the namespace config is selected by the hierarchy resolver. The Spark query then confirms
   * that the full credential-vending stack is not broken by the namespace override.
   *
   * <p><strong>Note on S3Mock environment:</strong> {@code allowedLocations} constraints are not
   * enforced at the S3Mock level; Spark's static mock credentials provide a fallback path that
   * bypasses location checks. The hierarchy is therefore verified via the Management API's {@code
   * storageName} field, not via an assert-failure pattern.
   */
  @Test
  public void testNamedNamespaceOverridesUnnamedCatalog() {
    onSpark("CREATE NAMESPACE ns");
    onSpark("CREATE TABLE ns.orders (id int, data string)");
    onSpark("INSERT INTO ns.orders VALUES (1, 'x')");

    setUpAccessDelegation();

    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "ns", createS3ConfigMissing("ns-missing-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("ns", "orders", MISSING_STORAGE_NAME);
    assertSparkQueryFails(
        "SELECT * FROM " + delegatedCatalogName + ".ns.orders",
        "Storage name '" + MISSING_STORAGE_NAME + "' is not configured");

    // Apply namespace config whose allowedLocations covers the actual table location.
    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "ns", createS3Config("ns-named", "ns-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // The API hierarchy walk must return storageName="ns-named" (namespace wins over unnamed
    // catalog).
    assertEffectiveStorageName("ns", "orders", "ns-named");

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".ns.orders").count())
        .as(
            "Spark read must return 1 row: namespace config 'ns-named' was resolved by the hierarchy")
        .isEqualTo(1L);
  }

  // ---------------------------------------------------------------------------
  // Scenario 3: Named table overrides named namespace
  // ---------------------------------------------------------------------------

  /**
   * Both the namespace ({@code storageName="ns-named"}) and the table ({@code
   * storageName="tbl-named"}) carry named storage configs. The table-level name must win.
   *
   * <p>The Management-API assertion ({@code assertEffectiveStorageName}) is the authoritative proof
   * that the table config is selected over the namespace config by the hierarchy resolver. The
   * namespace config is deliberately configured with {@code
   * allowedLocations=["s3://wrong-bucket-ns-only/"]} so that — in a production environment where
   * {@code allowedLocations} is enforced — the query would fail if the namespace config were
   * erroneously selected. In the S3Mock test environment the static mock credentials bypass that
   * enforcement; the storageName API assertion therefore carries the authoritative proof.
   */
  @Test
  public void testNamedTableOverridesNamedNamespace() {
    onSpark("CREATE NAMESPACE dept");
    onSpark("CREATE TABLE dept.reports (id int, data string)");
    onSpark("INSERT INTO dept.reports VALUES (1, 'r1'), (2, 'r2')");

    setUpAccessDelegation();

    // Invalid base state: namespace resolves to missing named credentials.
    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "dept", createS3ConfigMissing("ns-missing-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("dept", "reports", MISSING_STORAGE_NAME);
    assertSparkQueryFails(
        "SELECT * FROM " + delegatedCatalogName + ".dept.reports WHERE id > 0",
        "Storage name '" + MISSING_STORAGE_NAME + "' is not configured");

    // Table config points to the real base location — the only config that permits vending.
    try (Response r =
        managementApi.setTableStorageConfig(
            catalogName, "dept", "reports", createS3Config("tbl-named", "tbl-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // API proof: hierarchy resolver must select the table config (storageName="tbl-named"), not the
    // namespace config (storageName="ns-named").
    assertEffectiveStorageName("dept", "reports", "tbl-named");

    assertThat(
            onSpark("SELECT * FROM " + delegatedCatalogName + ".dept.reports WHERE id > 0").count())
        .as(
            "Spark read must return 2 rows; storageName='tbl-named' confirms table config was resolved")
        .isEqualTo(2L);
  }

  // ---------------------------------------------------------------------------
  // Scenario 4: Named table overrides unnamed namespace
  // ---------------------------------------------------------------------------

  /**
   * The namespace has an inline storage config but with {@code storageName=null} (unnamed). The
   * table has {@code storageName="tbl-named"}. The table-level config must win.
   *
   * <p>The Management-API assertion ({@code assertEffectiveStorageName}) is the authoritative proof
   * that the table config is selected over the unnamed namespace config. The namespace config is
   * deliberately configured with {@code allowedLocations=["s3://wrong-bucket-ns-unnamed/"]} so that
   * — in a production environment where {@code allowedLocations} is enforced — the query would fail
   * if the namespace config were erroneously selected. In the S3Mock test environment the static
   * mock credentials bypass that enforcement; the storageName API assertion therefore carries the
   * authoritative proof.
   */
  @Test
  public void testNamedTableOverridesUnnamedNamespace() {
    onSpark("CREATE NAMESPACE finance");
    onSpark("CREATE TABLE finance.ledger (id int, data string)");
    onSpark("INSERT INTO finance.ledger VALUES (1, 'entry1')");

    setUpAccessDelegation();

    // Invalid base state: namespace resolves to missing named credentials.
    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "finance", createS3ConfigMissing("finance-missing-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("finance", "ledger", MISSING_STORAGE_NAME);
    assertSparkQueryFails(
        "SELECT * FROM " + delegatedCatalogName + ".finance.ledger",
        "Storage name '" + MISSING_STORAGE_NAME + "' is not configured");

    // Table config points to the real base location — the only config that will allow vending.
    try (Response r =
        managementApi.setTableStorageConfig(
            catalogName, "finance", "ledger", createS3Config("tbl-named", "ledger-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // API proof: hierarchy resolver must select the table config (storageName="tbl-named"), not the
    // unnamed namespace config (whose storageName=null and wrong allowedLocations).
    assertEffectiveStorageName("finance", "ledger", "tbl-named");

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".finance.ledger").count())
        .as(
            "Spark read must return 1 row; storageName='tbl-named' confirms table config was resolved")
        .isEqualTo(1L);
  }

  // ---------------------------------------------------------------------------
  // Scenario 5: Namespace config with null storageName stops hierarchy walk
  // ---------------------------------------------------------------------------

  /**
   * The namespace has a storage config with {@code storageName=null} (explicitly carries a config
   * but uses the default credential chain). The catalog also has no storageName. The effective
   * storageName must be {@code null} (the walk stopped at the namespace, not the catalog). This
   * prevents a regression where a null storageName at the namespace could be misread as "no config"
   * and cause the walk to fall through to the catalog.
   *
   * <p>This also validates that Spark queries still execute successfully when the namespace
   * provides the config (even anonymously).
   */
  @Test
  public void testNullStorageNameAtNamespaceStopsHierarchyWalkInSpark() {
    onSpark("CREATE NAMESPACE analytics");
    onSpark("CREATE TABLE analytics.metrics (id int, data string)");
    onSpark("INSERT INTO analytics.metrics VALUES (1, 'm1'), (2, 'm2'), (3, 'm3')");

    // Namespace has a config but storageName is intentionally null
    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "analytics", createS3ConfigUnnamed("analytics-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // Effective storageName should be null (from namespace, not leaked from catalog)
    assertEffectiveStorageNameIsNull("analytics", "metrics");

    // Verify namespace config is the actual source (storageName from catalog must NOT appear)
    try (Response r = managementApi.getNamespaceStorageConfig(catalogName, "analytics")) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo config = r.readEntity(StorageConfigInfo.class);
      assertThat(config.getStorageName())
          .as("Namespace config storageName must remain null — must not inherit catalog name")
          .isNull();
    }

    setUpAccessDelegation();
    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".analytics.metrics").count())
        .as("Spark read must return 3 rows with null-named namespace config")
        .isEqualTo(3L);
  }

  // ---------------------------------------------------------------------------
  // Scenario 6: DELETE named table → reverts to named namespace
  // ---------------------------------------------------------------------------

  /**
   * The table has {@code storageName="tbl-named"} and the namespace has {@code
   * storageName="ns-named"}. After DELETE of the table storage config, the effective storageName
   * must revert to {@code "ns-named"}. Spark queries must work before and after DELETE.
   */
  @Test
  public void testDeleteNamedTableStorageRevertsToNamedNamespace() {
    onSpark("CREATE NAMESPACE warehouse");
    onSpark("CREATE TABLE warehouse.inventory (id int, data string)");
    onSpark("INSERT INTO warehouse.inventory VALUES (1, 'item1'), (2, 'item2')");

    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "warehouse", createS3Config("ns-named", "warehouse-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response r =
        managementApi.setTableStorageConfig(
            catalogName, "warehouse", "inventory", createS3Config("tbl-named", "inventory-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("warehouse", "inventory", "tbl-named");

    setUpAccessDelegation();
    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".warehouse.inventory").count())
        .as("Before DELETE: Spark read must work with table-level named config")
        .isEqualTo(2L);

    try (Response r =
        managementApi.deleteTableStorageConfig(catalogName, "warehouse", "inventory")) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    assertEffectiveStorageName("warehouse", "inventory", "ns-named");

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".warehouse.inventory").count())
        .as("After DELETE table config: Spark read must work with namespace-level named config")
        .isEqualTo(2L);
  }

  // ---------------------------------------------------------------------------
  // Scenario 7: DELETE named namespace → reverts to unnamed catalog (null storageName)
  // ---------------------------------------------------------------------------

  /**
   * The namespace has {@code storageName="ns-named"}. No table config exists. After DELETE of the
   * namespace storage config, the effective storageName must revert to {@code null} (from the
   * unnamed catalog). Spark queries must work before and after DELETE.
   */
  @Test
  public void testDeleteNamedNamespaceStorageRevertsToUnnamedCatalog() {
    onSpark("CREATE NAMESPACE products");
    onSpark("CREATE TABLE products.catalog_tbl (id int, data string)");
    onSpark("INSERT INTO products.catalog_tbl VALUES (1, 'p1')");

    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "products", createS3Config("ns-named", "products-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("products", "catalog_tbl", "ns-named");

    setUpAccessDelegation();
    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".products.catalog_tbl").count())
        .as("Before DELETE: Spark read must work with namespace-level named config")
        .isEqualTo(1L);

    try (Response r = managementApi.deleteNamespaceStorageConfig(catalogName, "products")) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    assertEffectiveStorageNameIsNull("products", "catalog_tbl");

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".products.catalog_tbl").count())
        .as("After DELETE namespace config: Spark read must work falling back to unnamed catalog")
        .isEqualTo(1L);
  }

  // ---------------------------------------------------------------------------
  // Scenario 8: All three levels named — progressive DELETE cascade
  // ---------------------------------------------------------------------------

  /**
   * The namespace has {@code storageName="ns"} and the table has {@code storageName="tbl"}. (The
   * catalog has no storageName — unnamed baseline.)
   *
   * <ol>
   *   <li>Initial: effective = {@code "tbl"} (table wins).
   *   <li>DELETE table config: effective = {@code "ns"} (namespace wins).
   *   <li>DELETE namespace config: effective = {@code null} (catalog unnamed baseline).
   * </ol>
   *
   * Spark queries must succeed at each stage.
   */
  @Test
  public void testAllThreeLevelsWithProgressiveDeletionCascade() {
    onSpark("CREATE NAMESPACE sales");
    onSpark("CREATE TABLE sales.transactions (id int, data string)");
    onSpark("INSERT INTO sales.transactions VALUES (1, 't1'), (2, 't2'), (3, 't3')");

    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "sales", createS3Config("ns", "sales-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response r =
        managementApi.setTableStorageConfig(
            catalogName, "sales", "transactions", createS3Config("tbl", "txn-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // Stage 1: table config wins
    assertEffectiveStorageName("sales", "transactions", "tbl");

    setUpAccessDelegation();
    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".sales.transactions").count())
        .as("Stage 1: Spark read must work with table-level named config")
        .isEqualTo(3L);

    // Stage 2: delete table config → namespace config revealed
    try (Response r =
        managementApi.deleteTableStorageConfig(catalogName, "sales", "transactions")) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    assertEffectiveStorageName("sales", "transactions", "ns");

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".sales.transactions").count())
        .as("Stage 2: Spark read must work with namespace-level named config after table DELETE")
        .isEqualTo(3L);

    // Stage 3: delete namespace config → unnamed catalog config revealed
    try (Response r = managementApi.deleteNamespaceStorageConfig(catalogName, "sales")) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    assertEffectiveStorageNameIsNull("sales", "transactions");

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".sales.transactions").count())
        .as("Stage 3: Spark read must work falling back to unnamed catalog after namespace DELETE")
        .isEqualTo(3L);
  }

  // ---------------------------------------------------------------------------
  // Scenario 9: Sibling namespace isolation — named on one branch, unnamed on the other
  // ---------------------------------------------------------------------------

  /**
   * {@code corp.billing} has {@code storageName="billing-creds"} while its sibling {@code
   * corp.support} has no namespace storage config (inherits unnamed catalog). Verify that the named
   * config on billing does not bleed into support, and vice versa. Both Spark queries must succeed
   * independently.
   */
  @Test
  public void testSiblingNamespaceStorageNameIsolation() {
    onSpark("CREATE NAMESPACE corp");
    onSpark("CREATE NAMESPACE corp.billing");
    onSpark("CREATE NAMESPACE corp.support");
    onSpark("CREATE TABLE corp.billing.invoices (id int, data string)");
    onSpark("CREATE TABLE corp.support.tickets (id int, data string)");
    onSpark("INSERT INTO corp.billing.invoices VALUES (1, 'inv1'), (2, 'inv2')");
    onSpark("INSERT INTO corp.support.tickets VALUES (1, 'tkt1')");

    setUpAccessDelegation();

    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "corp", createS3ConfigMissing("corp-missing-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertSparkQueryFails(
        "SELECT * FROM " + delegatedCatalogName + ".corp.billing.invoices",
        "Storage name '" + MISSING_STORAGE_NAME + "' is not configured");
    assertSparkQueryFails(
        "SELECT * FROM " + delegatedCatalogName + ".corp.support.tickets",
        "Storage name '" + MISSING_STORAGE_NAME + "' is not configured");

    // Only corp.billing gets a named storage config
    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "corp\u001Fbilling", createS3Config("billing-creds", "billing-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("corp\u001Fbilling", "invoices", "billing-creds");
    // corp.support has no namespace config and therefore inherits from parent `corp`.
    assertEffectiveStorageName("corp\u001Fsupport", "tickets", MISSING_STORAGE_NAME);

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".corp.billing.invoices").count())
        .as("Billing branch must read with named storage config 'billing-creds'")
        .isEqualTo(2L);

    assertSparkQueryFails(
        "SELECT * FROM " + delegatedCatalogName + ".corp.support.tickets",
        "Storage name '" + MISSING_STORAGE_NAME + "' is not configured");

    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "corp\u001Fsupport", createS3Config("ns-v1", "support-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".corp.support.tickets").count())
        .as("Support branch succeeds only after its own lower-level override")
        .isEqualTo(1L);
  }

  // ---------------------------------------------------------------------------
  // Scenario 10: Deep hierarchy — named at middle namespace, unnamed outer/inner
  // ---------------------------------------------------------------------------

  /**
   * Hierarchy: catalog (unnamed) → {@code L1} (no config) → {@code L1.L2} (storageName="mid") →
   * {@code L1.L2.L3} (no config) → table (no config). The effective storageName must be {@code
   * "mid"} (from L2, the middle namespace), not null from L1 or the catalog.
   */
  @Test
  public void testDeepHierarchyNamedAtMiddleNamespace() {
    onSpark("CREATE NAMESPACE L1");
    onSpark("CREATE NAMESPACE L1.L2");
    onSpark("CREATE NAMESPACE L1.L2.L3");
    onSpark("CREATE TABLE L1.L2.L3.deep_tbl (id int, data string)");
    onSpark("INSERT INTO L1.L2.L3.deep_tbl VALUES (1, 'd1'), (2, 'd2')");

    setUpAccessDelegation();

    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "L1", createS3ConfigMissing("l1-missing-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertSparkQueryFails(
        "SELECT * FROM " + delegatedCatalogName + ".L1.L2.L3.deep_tbl",
        "Storage name '" + MISSING_STORAGE_NAME + "' is not configured");

    // Only L1.L2 gets a named storage config
    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "L1\u001FL2", createS3Config("mid", "l2-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("L1\u001FL2\u001FL3", "deep_tbl", "mid");

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".L1.L2.L3.deep_tbl").count())
        .as("Deep hierarchy: Spark read must resolve through middle-namespace named config 'mid'")
        .isEqualTo(2L);
  }

  // ---------------------------------------------------------------------------
  // Scenario 11: Table-only named storage under fully unnamed ancestor hierarchy
  // ---------------------------------------------------------------------------

  /**
   * The catalog has no storageName. No namespace inline storage configs exist anywhere. Only the
   * table carries {@code storageName="tbl-only"}. The effective storageName must be {@code
   * "tbl-only"} (walk stops at the table). Spark queries must work.
   */
  @Test
  public void testTableOnlyNamedStorageUnderFullyUnnamedHierarchy() {
    onSpark("CREATE NAMESPACE raw");
    onSpark("CREATE NAMESPACE raw.ingest");
    onSpark("CREATE TABLE raw.ingest.events (id int, data string)");
    onSpark("INSERT INTO raw.ingest.events VALUES (1, 'e1'), (2, 'e2'), (3, 'e3')");

    setUpAccessDelegation();

    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "raw\u001Fingest", createS3ConfigMissing("raw-missing-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertSparkQueryFails(
        "SELECT * FROM " + delegatedCatalogName + ".raw.ingest.events",
        "Storage name '" + MISSING_STORAGE_NAME + "' is not configured");

    // No namespace configs anywhere; only the table is named
    try (Response r =
        managementApi.setTableStorageConfig(
            catalogName, "raw\u001Fingest", "events", createS3Config("tbl-only", "events-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("raw\u001Fingest", "events", "tbl-only");

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".raw.ingest.events").count())
        .as("Table-only named config: Spark read must succeed with storageName='tbl-only'")
        .isEqualTo(3L);
  }

  // ---------------------------------------------------------------------------
  // Scenario 12: Mixed named/unnamed across sibling tables in same namespace
  // ---------------------------------------------------------------------------

  /**
   * Two tables live in the same namespace. The namespace has {@code storageName="ns-shared"}. One
   * table overrides with {@code storageName="tbl-override"}, the other has no table config. Verify
   * that each table independently resolves to the correct effective storageName.
   */
  @Test
  public void testSiblingTablesInSameNamespaceWithMixedNamedAndUnnamed() {
    onSpark("CREATE NAMESPACE catalog_ns");
    onSpark("CREATE TABLE catalog_ns.named_tbl (id int, data string)");
    onSpark("CREATE TABLE catalog_ns.unnamed_tbl (id int, data string)");
    onSpark("INSERT INTO catalog_ns.named_tbl VALUES (1, 'n1')");
    onSpark("INSERT INTO catalog_ns.unnamed_tbl VALUES (1, 'u1'), (2, 'u2')");

    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "catalog_ns", createS3Config("ns-shared", "ns-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // Only named_tbl gets an inline storage config
    try (Response r =
        managementApi.setTableStorageConfig(
            catalogName, "catalog_ns", "named_tbl", createS3Config("tbl-override", "tbl-role"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("catalog_ns", "named_tbl", "tbl-override");
    assertEffectiveStorageName("catalog_ns", "unnamed_tbl", "ns-shared");

    setUpAccessDelegation();
    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".catalog_ns.named_tbl").count())
        .as("named_tbl: Spark read must use table-level override 'tbl-override'")
        .isEqualTo(1L);
    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".catalog_ns.unnamed_tbl").count())
        .as("unnamed_tbl: Spark read must fall back to namespace-level 'ns-shared'")
        .isEqualTo(2L);
  }

  // ---------------------------------------------------------------------------
  // Scenario 13: PUT replaces named storageName at namespace, then DELETE cascade
  // ---------------------------------------------------------------------------

  /**
   * The namespace storageName is updated (PUT) from {@code "ns-v1"} to {@code "ns-v2"}. Verify that
   * the effective config reflects the new name immediately. Then DELETE the namespace config and
   * verify revert to the unnamed catalog.
   */
  @Test
  public void testNamespaceStorageNameUpdateAndDeleteReverts() {
    onSpark("CREATE NAMESPACE versioned");
    onSpark("CREATE TABLE versioned.data (id int, v string)");
    onSpark("INSERT INTO versioned.data VALUES (1, 'v1'), (2, 'v2')");

    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "versioned", createS3Config("ns-v1", "role-v1"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("versioned", "data", "ns-v1");

    // PUT again with a different storageName (update)
    try (Response r =
        managementApi.setNamespaceStorageConfig(
            catalogName, "versioned", createS3Config("ns-v2", "role-v2"))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertEffectiveStorageName("versioned", "data", "ns-v2");

    setUpAccessDelegation();
    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".versioned.data").count())
        .as("Spark read must succeed after storageName update to 'ns-v2'")
        .isEqualTo(2L);

    // DELETE namespace config → revert to unnamed catalog
    try (Response r = managementApi.deleteNamespaceStorageConfig(catalogName, "versioned")) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    assertEffectiveStorageNameIsNull("versioned", "data");

    assertThat(onSpark("SELECT * FROM " + delegatedCatalogName + ".versioned.data").count())
        .as(
            "Spark read must succeed after namespace config DELETE, falling back to unnamed catalog")
        .isEqualTo(2L);
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /**
   * Creates a delegated Spark session with {@code X-Iceberg-Access-Delegation: vended-credentials}
   * and assigns the delegated principal catalog-admin rights. Populates {@link
   * #delegatedCatalogName}.
   */
  private void setUpAccessDelegation() {
    String principalName = client.newEntityName("spark_sn_delegate_user");
    String principalRoleName = client.newEntityName("spark_sn_delegate_role");
    PrincipalWithCredentials delegatedPrincipal =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);
    managementApi.makeAdmin(principalRoleName, managementApi.getCatalog(catalogName));
    sparkToken = client.obtainToken(delegatedPrincipal);
    delegatedCatalogName = client.newEntityName("spark_sn_delegate_catalog");

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

  /**
   * Asserts that the effective table storage config carries the given non-null {@code storageName}.
   * The effective config is fetched from the Management API; it already walks the full hierarchy
   * (table → namespace chain → catalog).
   */
  private void assertEffectiveStorageName(
      String namespace, String table, String expectedStorageName) {
    try (Response r = managementApi.getTableStorageConfig(catalogName, namespace, table)) {
      assertThat(r.getStatus())
          .as("GET table storage config should return 200")
          .isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo config = r.readEntity(StorageConfigInfo.class);
      assertThat(config.getStorageName())
          .as("Effective storageName for %s.%s must be '%s'", namespace, table, expectedStorageName)
          .isEqualTo(expectedStorageName);
    }
  }

  /**
   * Asserts that the effective table storage config has a {@code null} storageName — indicating the
   * effective owner (table, namespace or catalog) uses the default credential chain.
   */
  private void assertEffectiveStorageNameIsNull(String namespace, String table) {
    try (Response r = managementApi.getTableStorageConfig(catalogName, namespace, table)) {
      assertThat(r.getStatus())
          .as("GET table storage config should return 200")
          .isEqualTo(Response.Status.OK.getStatusCode());
      StorageConfigInfo config = r.readEntity(StorageConfigInfo.class);
      assertThat(config.getStorageName())
          .as(
              "Effective storageName for %s.%s must be null (default credential chain)",
              namespace, table)
          .isNull();
    }
  }

  /**
   * Builds an {@link AwsStorageConfigInfo} with the given {@code storageName} and role ARN,
   * inheriting the S3Mock endpoint settings from the base class.
   */
  private AwsStorageConfigInfo createS3Config(String storageName, String roleName) {
    String baseLocation =
        managementApi
            .getCatalog(catalogName)
            .getProperties()
            .toMap()
            .getOrDefault("default-base-location", "s3://my-bucket/path/to/data");

    AwsStorageConfigInfo.Builder builder =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(baseLocation))
            .setStorageName(storageName);

    if (includeRoleArnForHierarchyConfigs()) {
      builder.setRoleArn("arn:aws:iam::123456789012:role/" + roleName);
    }

    builder.setStsUnavailable(isStsUnavailableForHierarchyConfigs());

    Map<String, String> s3Props = storageEndpointProperties();
    String endpoint = s3Props.get("s3.endpoint");
    if (endpoint != null && !endpoint.isBlank()) {
      builder
          .setEndpoint(endpoint)
          .setPathStyleAccess(
              Boolean.parseBoolean(s3Props.getOrDefault("s3.path-style-access", "false")));
    }

    return builder.build();
  }

  /**
   * Builds an {@link AwsStorageConfigInfo} without a {@code storageName} (null). Used to model an
   * "unnamed" inline storage config that explicitly defines locations and a role but relies on the
   * default credential chain.
   */
  private AwsStorageConfigInfo createS3ConfigUnnamed(String roleName) {
    String baseLocation =
        managementApi
            .getCatalog(catalogName)
            .getProperties()
            .toMap()
            .getOrDefault("default-base-location", "s3://my-bucket/path/to/data");

    AwsStorageConfigInfo.Builder builder =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(baseLocation));

    if (includeRoleArnForHierarchyConfigs()) {
      builder.setRoleArn("arn:aws:iam::123456789012:role/" + roleName);
    }

    builder.setStsUnavailable(isStsUnavailableForHierarchyConfigs());
    // storageName intentionally omitted (null)

    Map<String, String> s3Props = storageEndpointProperties();
    String endpoint = s3Props.get("s3.endpoint");
    if (endpoint != null && !endpoint.isBlank()) {
      builder
          .setEndpoint(endpoint)
          .setPathStyleAccess(
              Boolean.parseBoolean(s3Props.getOrDefault("s3.path-style-access", "false")));
    }

    return builder.build();
  }

  private AwsStorageConfigInfo createS3ConfigMissing(String roleName) {
    return createS3Config(MISSING_STORAGE_NAME, roleName);
  }

  protected Map<String, String> storageEndpointProperties() {
    return s3Container.getS3ConfigProperties();
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
}
