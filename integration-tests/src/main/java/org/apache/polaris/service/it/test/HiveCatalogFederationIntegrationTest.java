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
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.HiveConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ImplicitAuthenticationParameters;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.NamespacePrivilege;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.it.ext.SparkSessionBuilder;
import org.apache.polaris.test.hms.HmsContainer;
import org.apache.polaris.test.rustfs.Rustfs;
import org.apache.polaris.test.rustfs.RustfsAccess;
import org.apache.polaris.test.rustfs.RustfsExtension;
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
 * Integration test for Hive Metastore (HMS) federation. Verifies that an external Polaris catalog
 * with {@code connectionType=HIVE} can federate read/write operations to a backing HMS, while
 * Polaris continues to enforce its own RBAC.
 *
 * <p>This test requires the following features to be configured:
 *
 * <ul>
 *   <li>{@link FeatureConfiguration#ENABLE_CATALOG_FEDERATION} = {@code true}.
 *   <li>{@link FeatureConfiguration#SUPPORTED_CATALOG_CONNECTION_TYPES} must include {@code
 *       "HIVE"}.
 *   <li>{@link FeatureConfiguration#SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES} must include
 *       {@code "IMPLICIT"}; Hive federation only supports {@code IMPLICIT} authentication.
 *   <li>{@link FeatureConfiguration#ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS} = {@code true}:
 *       required by tests that exercise RBAC on securables that live in HMS.
 *   <li>{@link FeatureConfiguration#ALLOW_DROPPING_NON_EMPTY_PASSTHROUGH_FACADE_CATALOG} = {@code
 *       true}: used by cleanup code.
 *   <li>{@link FeatureConfiguration#ALLOW_OVERLAPPING_CATALOG_URLS} = {@code true}.
 * </ul>
 */
@ExtendWith(RustfsExtension.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
public class HiveCatalogFederationIntegrationTest {

  public static final String BUCKET_URI_PREFIX = "/rustfs-test-hive-federation";
  public static final String RUSTFS_ACCESS_KEY = "test-ak-123-hive-federation";
  public static final String RUSTFS_SECRET_KEY = "test-sk-123-hive-federation";

  private static final String PRINCIPAL_NAME = "test-hive-federation-user";
  private static final String PRINCIPAL_ROLE_NAME = "test-hive-federation-user-role";
  private static final String DIRECT_CATALOG_NAME = "hms_direct";

  private static final CatalogGrant CATALOG_ADMIN_GRANT =
      CatalogGrant.builder()
          .setType(GrantResource.TypeEnum.CATALOG)
          .setPrivilege(CatalogPrivilege.CATALOG_MANAGE_CONTENT)
          .build();

  private static PolarisClient client;
  private static ManagementApi managementApi;
  private static PolarisApiEndpoints endpoints;
  private static SparkSession spark;
  private static HmsContainer hmsContainer;
  private static String hmsThriftUri;
  private static URI warehouseLocation;
  private static String s3endpoint;

  @TempDir static Path warehouseDir;

  private String federatedCatalogName;
  private String federatedCatalogRoleName;

  @BeforeAll
  static void setup(
      PolarisApiEndpoints apiEndpoints,
      ClientCredentials credentials,
      @Rustfs(accessKey = RUSTFS_ACCESS_KEY, secretKey = RUSTFS_SECRET_KEY)
          RustfsAccess rustfsAccess) {
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    String adminToken = client.obtainToken(credentials);
    managementApi = client.managementApi(adminToken);
    s3endpoint = rustfsAccess.s3endpoint();
    warehouseLocation = rustfsAccess.s3BucketUri(BUCKET_URI_PREFIX + "/warehouse");

    // HMS validates s3a:// table locations during create-table by talking to the underlying
    // S3 endpoint. RustFS only accepts requests whose Host header matches the value of its
    // RUSTFS_SERVER_DOMAINS env var (set to RustFS's fixed host port via the testcontainer).
    // We therefore connect to RustFS using its exact host:port name from inside HMS, with an
    // extra /etc/hosts entry pointing that hostname at the Docker host gateway ("host-gateway").
    String rustfsHost = rustfsAccess.hostPort().substring(0, rustfsAccess.hostPort().indexOf(':'));
    hmsContainer =
        new HmsContainer()
            .withExtraHost(rustfsHost, "host-gateway")
            .withS3aEndpoint(
                "http://" + rustfsAccess.hostPort(),
                rustfsAccess.accessKey(),
                rustfsAccess.secretKey())
            .withStartupAttempts(3);
    hmsContainer.start();
    hmsThriftUri = hmsContainer.thriftUri();
  }

  @AfterAll
  static void close() throws Exception {
    if (hmsContainer != null) {
      hmsContainer.close();
    }
    if (client != null) {
      client.close();
    }
  }

  @BeforeEach
  void before() {
    federatedCatalogName = "test_catalog_hive_" + System.nanoTime();
    federatedCatalogRoleName = "test-catalog-role_" + System.nanoTime();

    PrincipalWithCredentials newUserCredentials =
        managementApi.createPrincipalWithRole(PRINCIPAL_NAME, PRINCIPAL_ROLE_NAME);

    AwsStorageConfigInfo storageConfig =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setPathStyleAccess(true)
            .setEndpoint(s3endpoint)
            .setAllowedLocations(List.of(warehouseLocation.toString()))
            .build();

    ConnectionConfigInfo connectionConfig =
        HiveConnectionConfigInfo.builder()
            .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.HIVE)
            .setUri(hmsThriftUri)
            .setWarehouse(warehouseLocation.toString())
            .setAuthenticationParameters(
                ImplicitAuthenticationParameters.builder()
                    .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.IMPLICIT)
                    .build())
            .build();

    // Polaris's HIVE federation creates an Iceberg HiveCatalog inside the server. Without an
    // explicit io-impl, HiveCatalog defaults to HadoopFileIO and tries to load S3AFileSystem,
    // which is not on the Polaris runtime classpath. Force S3FileIO and pass the RustFS endpoint
    // + creds so Polaris's in-server HiveCatalog can read the Iceberg metadata JSON when serving
    // loadTable(). These long-lived creds stay server-side: clients use the vended-credentials
    // path (see X-Iceberg-Access-Delegation header below), which overrides the s3.* entries in
    // the loadTable config response with subscoped STS creds.
    CatalogProperties catalogProperties = new CatalogProperties(warehouseLocation.toString());
    catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    catalogProperties.put("s3.endpoint", s3endpoint);
    catalogProperties.put("s3.path-style-access", "true");
    catalogProperties.put("s3.access-key-id", RUSTFS_ACCESS_KEY);
    catalogProperties.put("s3.secret-access-key", RUSTFS_SECRET_KEY);

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
    managementApi.addGrant(federatedCatalogName, federatedCatalogRoleName, CATALOG_ADMIN_GRANT);
    CatalogRole role = managementApi.getCatalogRole(federatedCatalogName, federatedCatalogRoleName);
    managementApi.grantCatalogRoleToPrincipalRole(PRINCIPAL_ROLE_NAME, federatedCatalogName, role);

    String sparkToken = client.obtainToken(newUserCredentials);
    spark =
        SparkSessionBuilder.buildWithTestDefaults()
            .withWarehouse(warehouseDir.toUri())
            .addCatalog(
                federatedCatalogName,
                "org.apache.iceberg.spark.SparkCatalog",
                endpoints,
                sparkToken)
            // Disable the REST-catalog cache so RBAC changes take effect immediately.
            .withConfig("spark.sql.catalog." + federatedCatalogName + ".cache-enabled", "false")
            .withConfig(
                "spark.sql.catalog." + federatedCatalogName + ".header.X-Iceberg-Access-Delegation",
                "vended-credentials")
            // The "direct" catalog talks to HMS directly via Iceberg's HiveCatalog and writes
            // to the same RustFS bucket that Polaris's federated catalog reads from.
            .withConfig(
                "spark.sql.catalog." + DIRECT_CATALOG_NAME, "org.apache.iceberg.spark.SparkCatalog")
            .withConfig("spark.sql.catalog." + DIRECT_CATALOG_NAME + ".type", "hive")
            .withConfig("spark.sql.catalog." + DIRECT_CATALOG_NAME + ".uri", hmsThriftUri)
            .withConfig(
                "spark.sql.catalog." + DIRECT_CATALOG_NAME + ".warehouse",
                warehouseLocation.toString().replace("s3://", "s3a://"))
            .withConfig(
                "spark.sql.catalog." + DIRECT_CATALOG_NAME + ".io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
            .withConfig("spark.sql.catalog." + DIRECT_CATALOG_NAME + ".s3.endpoint", s3endpoint)
            .withConfig(
                "spark.sql.catalog." + DIRECT_CATALOG_NAME + ".s3.path-style-access", "true")
            .withConfig(
                "spark.sql.catalog." + DIRECT_CATALOG_NAME + ".s3.access-key-id", RUSTFS_ACCESS_KEY)
            .withConfig(
                "spark.sql.catalog." + DIRECT_CATALOG_NAME + ".s3.secret-access-key",
                RUSTFS_SECRET_KEY)
            .withConfig("spark.sql.catalog." + DIRECT_CATALOG_NAME + ".cache-enabled", "false")
            .getOrCreate();

    seedHmsTables();
  }

  @AfterEach
  void after() {
    if (spark != null) {
      // Drop everything we created in HMS via the direct catalog so the embedded Derby DB is
      // clean for the next test. The built-in HMS "default" namespace cannot be dropped, so we
      // target ns1/ns2 explicitly.
      for (String ns : List.of("ns1", "ns2")) {
        // No PURGE: the Iceberg DeleteReachableFiles action uses Spark beans that need
        // javax.annotation, which is not on this trimmed-down test classpath.
        spark.sql(String.format("DROP TABLE IF EXISTS %s.%s.test_table", DIRECT_CATALOG_NAME, ns));
        spark.sql(String.format("DROP NAMESPACE IF EXISTS %s.%s", DIRECT_CATALOG_NAME, ns));
      }
      SparkSession.clearDefaultSession();
      SparkSession.clearActiveSession();
      spark.close();
    }
    // We don't catalogApi.purge() here: HMS exposes a non-droppable "default" namespace, and
    // the underlying HMS state has already been cleaned up via the direct catalog above.
    // ALLOW_DROPPING_NON_EMPTY_PASSTHROUGH_FACADE_CATALOG lets dropCatalog succeed regardless.
    managementApi.dropCatalog(federatedCatalogName);
    managementApi.deletePrincipalRole(PRINCIPAL_ROLE_NAME);
    managementApi.deletePrincipal(PRINCIPAL_NAME);
  }

  private void seedHmsTables() {
    spark.sql("USE " + DIRECT_CATALOG_NAME);
    // Namespace and table locations use s3a:// because Hadoop's FileSystem registry maps that
    // scheme to S3AFileSystem, which we added to the apache/hive image (see Dockerfile-hms-version)
    // along with the AWS SDK bundle. The hive-site.xml installed by HmsContainer.withS3aEndpoint
    // points S3AFileSystem at RustFS so HMS can validate the LOCATIONs during create-database /
    // create-table. Iceberg's S3FileIO on the read side accepts s3a:// URIs interchangeably with
    // s3://.
    String s3aWarehouse = warehouseLocation.toString().replace("s3://", "s3a://");
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ns1 LOCATION '" + s3aWarehouse + "/ns1.db'");
    spark.sql(
        "CREATE TABLE IF NOT EXISTS ns1.test_table (id int, name string) USING iceberg "
            + "LOCATION '"
            + s3aWarehouse
            + "/ns1/test_table'");
    spark.sql("INSERT INTO ns1.test_table VALUES (1, 'Alice'), (2, 'Bob')");

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ns2 LOCATION '" + s3aWarehouse + "/ns2.db'");
    spark.sql(
        "CREATE TABLE IF NOT EXISTS ns2.test_table (id int, name string) USING iceberg "
            + "LOCATION '"
            + s3aWarehouse
            + "/ns2/test_table'");
    spark.sql("INSERT INTO ns2.test_table VALUES (1, 'Apache Polaris'), (2, 'Apache Iceberg')");
  }

  @Test
  void testFederatedReadSeesHmsTables() {
    spark.sql("USE " + federatedCatalogName);

    List<Row> namespaces = spark.sql("SHOW NAMESPACES").collectAsList();
    assertThat(namespaces).extracting(r -> r.getString(0)).contains("ns1", "ns2");

    List<Row> ns1Data = spark.sql("SELECT * FROM ns1.test_table ORDER BY id").collectAsList();
    List<Row> ns1Direct =
        spark
            .sql("SELECT * FROM " + DIRECT_CATALOG_NAME + ".ns1.test_table ORDER BY id")
            .collectAsList();
    assertThat(ns1Data).isEqualTo(ns1Direct).hasSize(2);
  }

  @Test
  void testFederatedWriteVisibleInHms() {
    spark.sql("USE " + federatedCatalogName);
    spark.sql("INSERT INTO ns1.test_table VALUES (3, 'Charlie')");

    spark.sql(String.format("REFRESH TABLE %s.ns1.test_table", DIRECT_CATALOG_NAME));
    List<Row> directData =
        spark
            .sql("SELECT * FROM " + DIRECT_CATALOG_NAME + ".ns1.test_table ORDER BY id")
            .collectAsList();
    assertThat(directData).hasSize(3);
    assertThat(directData.get(2).getInt(0)).isEqualTo(3);
    assertThat(directData.get(2).getString(1)).isEqualTo("Charlie");
  }

  @Test
  void testFederatedCatalogEnforcesNamespaceRbac() {
    managementApi.revokeGrant(federatedCatalogName, federatedCatalogRoleName, CATALOG_ADMIN_GRANT);
    NamespaceGrant ns1Read =
        NamespaceGrant.builder()
            .setType(GrantResource.TypeEnum.NAMESPACE)
            .setPrivilege(NamespacePrivilege.TABLE_READ_DATA)
            .setNamespace(List.of("ns1"))
            .build();
    managementApi.addGrant(federatedCatalogName, federatedCatalogRoleName, ns1Read);

    spark.sql("USE " + federatedCatalogName);
    assertThat(spark.sql("SELECT * FROM ns1.test_table").collectAsList()).hasSize(2);

    // No grant on ns2: Polaris must reject the federated read.
    assertThatThrownBy(() -> spark.sql("SELECT * FROM ns2.test_table").collectAsList())
        .isInstanceOf(ForbiddenException.class);
  }
}
