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

import java.net.URI;
import java.util.List;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.OAuthClientCredentialsParameters;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
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
  private static ManagementApi managementApi;
  private static PolarisApiEndpoints endpoints;
  private static ClientCredentials adminCredentials;
  private static SparkSession spark;
  private static String sparkToken;
  private static String adminToken;

  private static final String PRINCIPAL_NAME = "test-catalog-federation-user";
  private static final String LOCAL_CATALOG_NAME = "test_catalog_local";
  private static final String EXTERNAL_CATALOG_NAME = "test_catalog_external";
  private static final String CATALOG_ROLE_NAME = "catalog_admin";
  private static final String PRINCIPAL_ROLE_NAME = "service_admin";

  @TempDir static java.nio.file.Path warehouseDir;

  private URI baseLocation;
  private PrincipalWithCredentials newUserCredentials;

  @BeforeAll
  static void setup(PolarisApiEndpoints apiEndpoints, ClientCredentials credentials) {
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    adminToken = client.obtainToken(credentials);
    managementApi = client.managementApi(adminToken);
    adminCredentials = credentials;
    sparkToken = client.obtainToken(credentials);
  }

  @AfterAll
  static void close() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  @BeforeEach
  void before() {
    this.baseLocation = URI.create("file:///tmp/warehouse");
  }

  @AfterEach
  void after() {
    if (spark != null) {
      SparkSession.clearDefaultSession();
      SparkSession.clearActiveSession();
      spark.close();
    }
    managementApi.dropCatalog(EXTERNAL_CATALOG_NAME);
    managementApi.dropCatalog(LOCAL_CATALOG_NAME);
    managementApi.deletePrincipal(PRINCIPAL_NAME);
  }

  @Test
  void testCatalogFederation() {
    newUserCredentials = managementApi.createPrincipal(PRINCIPAL_NAME);

    FileStorageConfigInfo storageConfig =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of(baseLocation.toString()))
            .build();

    CatalogProperties catalogProperties = new CatalogProperties(baseLocation.toString());

    Catalog localCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(LOCAL_CATALOG_NAME)
            .setProperties(catalogProperties)
            .setStorageConfigInfo(storageConfig)
            .build();
    managementApi.createCatalog(localCatalog);

    CatalogGrant catalogGrant =
        CatalogGrant.builder()
            .setType(CatalogGrant.TypeEnum.CATALOG)
            .setPrivilege(CatalogPrivilege.TABLE_WRITE_DATA)
            .build();
    managementApi.addGrant(LOCAL_CATALOG_NAME, CATALOG_ROLE_NAME, catalogGrant);
    managementApi.assignPrincipalRole(PRINCIPAL_NAME, PRINCIPAL_ROLE_NAME);
    CatalogRole localCatalogAdminRole =
        managementApi.getCatalogRole(LOCAL_CATALOG_NAME, CATALOG_ROLE_NAME);
    managementApi.grantCatalogRoleToPrincipalRole(
        PRINCIPAL_ROLE_NAME, LOCAL_CATALOG_NAME, localCatalogAdminRole);

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
            .setRemoteCatalogName(LOCAL_CATALOG_NAME)
            .setAuthenticationParameters(authParams)
            .build();
    ExternalCatalog externalCatalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName(EXTERNAL_CATALOG_NAME)
            .setConnectionConfigInfo(connectionConfig)
            .setProperties(catalogProperties)
            .setStorageConfigInfo(storageConfig)
            .build();
    managementApi.createCatalog(externalCatalog);

    managementApi.addGrant(EXTERNAL_CATALOG_NAME, CATALOG_ROLE_NAME, catalogGrant);
    CatalogRole externalCatalogAdminRole =
        managementApi.getCatalogRole(EXTERNAL_CATALOG_NAME, CATALOG_ROLE_NAME);
    managementApi.grantCatalogRoleToPrincipalRole(
        PRINCIPAL_ROLE_NAME, EXTERNAL_CATALOG_NAME, externalCatalogAdminRole);

    spark =
        SparkSessionBuilder.buildWithTestDefaults()
            .withWarehouse(warehouseDir.toUri())
            .addCatalog(
                LOCAL_CATALOG_NAME, "org.apache.iceberg.spark.SparkCatalog", endpoints, sparkToken)
            .addCatalog(
                EXTERNAL_CATALOG_NAME,
                "org.apache.iceberg.spark.SparkCatalog",
                endpoints,
                sparkToken)
            .getOrCreate();

    spark.sql("USE " + LOCAL_CATALOG_NAME);
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ns1");
    spark.sql("CREATE TABLE IF NOT EXISTS ns1.test_table (id int, name string)");
    spark.sql("INSERT INTO ns1.test_table VALUES (1, 'Alice')");
    spark.sql("INSERT INTO ns1.test_table VALUES (2, 'Bob')");

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ns2");
    spark.sql("CREATE TABLE IF NOT EXISTS ns2.test_table (id int, name string)");
    spark.sql("INSERT INTO ns2.test_table VALUES (1, 'Apache Spark')");
    spark.sql("INSERT INTO ns2.test_table VALUES (2, 'Apache Iceberg')");

    spark.sql("USE " + EXTERNAL_CATALOG_NAME);
    List<Row> namespaces = spark.sql("SHOW NAMESPACES").collectAsList();
    assertThat(namespaces).hasSize(2);

    List<Row> ns1Data = spark.sql("SELECT * FROM ns1.test_table ORDER BY id").collectAsList();
    assertThat(ns1Data).hasSize(2);
    assertThat(ns1Data.get(0).getInt(0)).isEqualTo(1);
    assertThat(ns1Data.get(0).getString(1)).isEqualTo("Alice");
    assertThat(ns1Data.get(1).getInt(0)).isEqualTo(2);
    assertThat(ns1Data.get(1).getString(1)).isEqualTo("Bob");
    spark.sql("INSERT INTO ns1.test_table VALUES (3, 'Charlie')");
    List<Row> ns2Data = spark.sql("SELECT * FROM ns2.test_table ORDER BY id").collectAsList();
    assertThat(ns2Data).hasSize(2);
    assertThat(ns2Data.get(0).getInt(0)).isEqualTo(1);
    assertThat(ns2Data.get(0).getString(1)).isEqualTo("Apache Spark");
    assertThat(ns2Data.get(1).getInt(0)).isEqualTo(2);
    assertThat(ns2Data.get(1).getString(1)).isEqualTo("Apache Iceberg");
    spark.sql("INSERT INTO ns2.test_table VALUES (3, 'Apache Polaris')");

    spark.sql("USE " + LOCAL_CATALOG_NAME);
    spark.sql("REFRESH TABLE ns1.test_table");
    spark.sql("REFRESH TABLE ns2.test_table");
    List<Row> updatedNs1Data =
        spark.sql("SELECT * FROM ns1.test_table ORDER BY id").collectAsList();
    assertThat(updatedNs1Data).hasSize(3);
    assertThat(updatedNs1Data.get(2).getInt(0)).isEqualTo(3);
    assertThat(updatedNs1Data.get(2).getString(1)).isEqualTo("Charlie");
    List<Row> updatedNs2Data =
        spark.sql("SELECT * FROM ns2.test_table ORDER BY id").collectAsList();
    assertThat(updatedNs2Data).hasSize(3);
    assertThat(updatedNs2Data.get(2).getInt(0)).isEqualTo(3);
    assertThat(updatedNs2Data.get(2).getString(1)).isEqualTo("Apache Polaris");

    spark.sql("DROP TABLE ns1.test_table");
    spark.sql("DROP TABLE ns2.test_table");
    spark.sql("DROP NAMESPACE ns1");
    spark.sql("DROP NAMESPACE ns2");
  }
}
