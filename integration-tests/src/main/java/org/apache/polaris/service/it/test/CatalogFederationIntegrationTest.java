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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.NamespacePrivilege;
import org.apache.polaris.core.admin.model.OAuthClientCredentialsParameters;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.TableGrant;
import org.apache.polaris.core.admin.model.TablePrivilege;
import org.apache.polaris.service.it.env.CatalogApi;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.it.ext.SparkSessionBuilder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test for catalog federation functionality. This test verifies that an external
 * catalog can be created that federates with an internal catalog.
 */
@ExtendWith(PolarisIntegrationTestExtension.class)
public class CatalogFederationIntegrationTest {

  private static PolarisClient client;
  private static CatalogApi catalogApi;
  private static ManagementApi managementApi;
  private static PolarisApiEndpoints endpoints;
  private static SparkSession spark;
  private static String localCatalogName;
  private static String federatedCatalogName;
  private static String localCatalogRoleName;
  private static String federatedCatalogRoleName;

  private static final String PRINCIPAL_NAME = "test-catalog-federation-user";
  private static final String PRINCIPAL_ROLE_NAME = "test-catalog-federation-user-role";
  private static final CatalogGrant defaultCatalogGrant =
      CatalogGrant.builder()
          .setType(GrantResource.TypeEnum.CATALOG)
          .setPrivilege(CatalogPrivilege.CATALOG_MANAGE_CONTENT)
          .build();

  @TempDir static java.nio.file.Path warehouseDir;

  private URI baseLocation;
  private PrincipalWithCredentials newUserCredentials;

  @BeforeAll
  static void setup(PolarisApiEndpoints apiEndpoints, ClientCredentials credentials) {
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    String adminToken = client.obtainToken(credentials);
    managementApi = client.managementApi(adminToken);
    catalogApi = client.catalogApi(adminToken);
  }

  @AfterAll
  static void close() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  @BeforeEach
  void before() {
    setupCatalogs();
    setupExampleNamespacesAndTables();
  }

  @AfterEach
  void after() {
    if (spark != null) {
      SparkSession.clearDefaultSession();
      SparkSession.clearActiveSession();
      spark.close();
    }
    catalogApi.purge(localCatalogName);
    // managementApi.dropCatalog(federatedCatalogName);
    managementApi.dropCatalog(localCatalogName);
    managementApi.deletePrincipalRole(PRINCIPAL_ROLE_NAME);
    managementApi.deletePrincipal(PRINCIPAL_NAME);
  }

  private void setupCatalogs() {
    baseLocation = URI.create("file:///tmp/warehouse");
    newUserCredentials = managementApi.createPrincipalWithRole(PRINCIPAL_NAME, PRINCIPAL_ROLE_NAME);

    FileStorageConfigInfo storageConfig =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of(baseLocation.toString()))
            .build();

    CatalogProperties catalogProperties = new CatalogProperties(baseLocation.toString());

    localCatalogName = "test_catalog_local_" + UUID.randomUUID().toString().replace("-", "");
    localCatalogRoleName = "test-catalog-role_" + UUID.randomUUID().toString().replace("-", "");
    federatedCatalogName = "test_catalog_external_" + UUID.randomUUID().toString().replace("-", "");
    federatedCatalogRoleName = "test-catalog-role_" + UUID.randomUUID().toString().replace("-", "");

    Catalog localCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(localCatalogName)
            .setProperties(catalogProperties)
            .setStorageConfigInfo(storageConfig)
            .build();
    managementApi.createCatalog(localCatalog);
    managementApi.createCatalogRole(localCatalogName, localCatalogRoleName);

    managementApi.addGrant(localCatalogName, localCatalogRoleName, defaultCatalogGrant);
    CatalogRole localCatalogRole =
        managementApi.getCatalogRole(localCatalogName, localCatalogRoleName);
    managementApi.grantCatalogRoleToPrincipalRole(
        PRINCIPAL_ROLE_NAME, localCatalogName, localCatalogRole);

    AuthenticationParameters authParams =
        OAuthClientCredentialsParameters.builder()
            .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.OAUTH)
            .setTokenUri(endpoints.catalogApiEndpoint().toString() + "/v1/oauth/tokens")
            .setClientId(newUserCredentials.getCredentials().getClientId())
            .setClientSecret(newUserCredentials.getCredentials().getClientSecret())
            .setScopes(List.of("PRINCIPAL_ROLE:ALL"))
            .build();
    ConnectionConfigInfo connectionConfig =
        IcebergRestConnectionConfigInfo.builder()
            .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
            .setUri(endpoints.catalogApiEndpoint().toString())
            .setRemoteCatalogName(localCatalogName)
            .setAuthenticationParameters(authParams)
            .build();
    ExternalCatalog externalCatalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName(federatedCatalogName)
            .setConnectionConfigInfo(connectionConfig)
            .setProperties(catalogProperties)
            .setStorageConfigInfo(storageConfig)
            .build();
    managementApi.createCatalog(externalCatalog);
    managementApi.createCatalogRole(federatedCatalogName, federatedCatalogRoleName);

    managementApi.addGrant(federatedCatalogName, federatedCatalogRoleName, defaultCatalogGrant);
    CatalogRole externalCatalogAdminRole =
        managementApi.getCatalogRole(federatedCatalogName, federatedCatalogRoleName);
    managementApi.grantCatalogRoleToPrincipalRole(
        PRINCIPAL_ROLE_NAME, federatedCatalogName, externalCatalogAdminRole);

    String sparkToken = client.obtainToken(newUserCredentials);
    spark =
        SparkSessionBuilder.buildWithTestDefaults()
            .withWarehouse(warehouseDir.toUri())
            .addCatalog(
                localCatalogName, "org.apache.iceberg.spark.SparkCatalog", endpoints, sparkToken)
            .addCatalog(
                federatedCatalogName,
                "org.apache.iceberg.spark.SparkCatalog",
                endpoints,
                sparkToken)
            .getOrCreate();
  }

  private void setupExampleNamespacesAndTables() {
    spark.sql("USE " + localCatalogName);
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ns1");
    spark.sql("CREATE TABLE IF NOT EXISTS ns1.test_table (id int, name string)");
    spark.sql("INSERT INTO ns1.test_table VALUES (1, 'Alice')");
    spark.sql("INSERT INTO ns1.test_table VALUES (2, 'Bob')");

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ns2");
    spark.sql("CREATE TABLE IF NOT EXISTS ns2.test_table (id int, name string)");
    spark.sql("INSERT INTO ns2.test_table VALUES (1, 'Apache Spark')");
    spark.sql("INSERT INTO ns2.test_table VALUES (2, 'Apache Iceberg')");

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ns1.ns1a");
    spark.sql("CREATE TABLE IF NOT EXISTS ns1.ns1a.test_table (id int, name string)");
    spark.sql("INSERT INTO ns1.ns1a.test_table VALUES (1, 'Alice')");

    spark.sql("CREATE TABLE IF NOT EXISTS ns1.ns1a.test_table2 (id int, name string)");
    spark.sql("INSERT INTO ns1.ns1a.test_table2 VALUES (1, 'Apache Iceberg')");
  }

  @Test
  void testFederatedCatalogBasicReadWriteOperations() {
    spark.sql("USE " + federatedCatalogName);
    List<Row> namespaces = spark.sql("SHOW NAMESPACES").collectAsList();
    assertThat(namespaces).hasSize(2);
    List<Row> ns1Data = spark.sql("SELECT * FROM ns1.test_table ORDER BY id").collectAsList();
    List<Row> refNs1Data =
        spark
            .sql(String.format("SELECT * FROM %s.ns1.test_table ORDER BY id", localCatalogName))
            .collectAsList();
    assertThat(ns1Data).isEqualTo(refNs1Data);
    spark.sql("INSERT INTO ns1.test_table VALUES (3, 'Charlie')");

    List<Row> ns2Data = spark.sql("SELECT * FROM ns2.test_table ORDER BY id").collectAsList();
    List<Row> refNs2Data =
        spark
            .sql(String.format("SELECT * FROM %s.ns2.test_table ORDER BY id", localCatalogName))
            .collectAsList();
    assertThat(ns2Data).isEqualTo(refNs2Data);
    spark.sql("INSERT INTO ns2.test_table VALUES (3, 'Apache Polaris')");

    spark.sql(String.format("REFRESH TABLE %s.ns1.test_table", localCatalogName));
    spark.sql(String.format("REFRESH TABLE %s.ns2.test_table", localCatalogName));

    List<Row> updatedNs1Data =
        spark
            .sql(String.format("SELECT * FROM %s.ns1.test_table ORDER BY id", localCatalogName))
            .collectAsList();
    assertThat(updatedNs1Data).hasSize(3);
    assertThat(updatedNs1Data.get(2).getInt(0)).isEqualTo(3);
    assertThat(updatedNs1Data.get(2).getString(1)).isEqualTo("Charlie");
    List<Row> updatedNs2Data =
        spark
            .sql(String.format("SELECT * FROM %s.ns2.test_table ORDER BY id", localCatalogName))
            .collectAsList();
    assertThat(updatedNs2Data).hasSize(3);
    assertThat(updatedNs2Data.get(2).getInt(0)).isEqualTo(3);
    assertThat(updatedNs2Data.get(2).getString(1)).isEqualTo("Apache Polaris");
  }

  @Test
  void testFederatedCatalogWithNamespaceRBAC() {
    managementApi.revokeGrant(federatedCatalogName, federatedCatalogRoleName, defaultCatalogGrant);
    NamespaceGrant namespaceGrant =
        NamespaceGrant.builder()
            .setType(GrantResource.TypeEnum.NAMESPACE)
            .setPrivilege(NamespacePrivilege.TABLE_READ_DATA)
            .setNamespace(List.of("ns1"))
            .build();
    // Grant read to table under namespace ns1 only
    managementApi.addGrant(federatedCatalogName, federatedCatalogRoleName, namespaceGrant);

    spark.sql("USE " + federatedCatalogName);
    // Read should work for tables under ns1 and ns1.ns1a
    List<Row> ns1Data = spark.sql("SELECT * FROM ns1.test_table ORDER BY id").collectAsList();
    assertThat(ns1Data).hasSize(2);
    List<Row> ns1aData = spark.sql("SELECT * FROM ns1.ns1a.test_table ORDER BY id").collectAsList();
    assertThat(ns1aData).hasSize(1);

    // Read should fail for tables under ns2
    assertThatThrownBy(() -> spark.sql("SELECT * FROM ns2.test_table ORDER BY id").collectAsList())
        .isInstanceOf(ForbiddenException.class);

    // Read should work for tables in local catalog
    List<Row> localNs2Data =
        spark
            .sql("SELECT * FROM " + localCatalogName + ".ns2.test_table ORDER BY id")
            .collectAsList();
    assertThat(localNs2Data).hasSize(2);

    // Restore the grant
    managementApi.revokeGrant(federatedCatalogName, federatedCatalogRoleName, namespaceGrant);
    managementApi.addGrant(federatedCatalogName, federatedCatalogRoleName, defaultCatalogGrant);
  }

  @Test
  void testFederatedCatalogWithTableRBAC() {
    managementApi.revokeGrant(federatedCatalogName, federatedCatalogRoleName, defaultCatalogGrant);
    TableGrant tableGrant =
        TableGrant.builder()
            .setType(GrantResource.TypeEnum.TABLE)
            .setPrivilege(TablePrivilege.TABLE_READ_DATA)
            .setNamespace(List.of("ns1"))
            .setTableName("test_table")
            .build();
    // Grant read to table under namespace ns1 only
    managementApi.addGrant(federatedCatalogName, federatedCatalogRoleName, tableGrant);

    spark.sql("USE " + federatedCatalogName);
    // Read should work for tables under ns1
    List<Row> ns1Data = spark.sql("SELECT * FROM ns1.test_table ORDER BY id").collectAsList();
    assertThat(ns1Data).hasSize(2);

    // Read should fail for tables under ns1.ns1a
    assertThatThrownBy(
            () -> spark.sql("SELECT * FROM ns1.ns1a.test_table ORDER BY id").collectAsList())
        .isInstanceOf(ForbiddenException.class);

    // Read should fail for tables under ns2
    assertThatThrownBy(() -> spark.sql("SELECT * FROM ns2.test_table ORDER BY id").collectAsList())
        .isInstanceOf(ForbiddenException.class);

    // Read should work for tables in local catalog
    List<Row> localNs2Data =
        spark
            .sql("SELECT * FROM " + localCatalogName + ".ns2.test_table ORDER BY id")
            .collectAsList();
    assertThat(localNs2Data).hasSize(2);

    // Restore the grant
    managementApi.revokeGrant(federatedCatalogName, federatedCatalogRoleName, tableGrant);
    managementApi.addGrant(federatedCatalogName, federatedCatalogRoleName, defaultCatalogGrant);
  }
}
