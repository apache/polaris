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

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.google.common.collect.ImmutableMap;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.CatalogApi;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * @implSpec This test expects the server to be configured with the following features enabled:
 *     <ul>
 *       <li>{@link
 *           org.apache.polaris.core.PolarisConfiguration#SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION}:
 *           {@code true}
 *       <li>{@link org.apache.polaris.core.PolarisConfiguration#ALLOW_OVERLAPPING_CATALOG_URLS}:
 *           {@code true}
 *     </ul>
 */
@ExtendWith(PolarisIntegrationTestExtension.class)
public class PolarisSparkIntegrationTest {

  private static final S3MockContainer s3Container =
      new S3MockContainer("3.11.0").withInitialBuckets("my-bucket,my-old-bucket");
  private static SparkSession spark;
  private PolarisApiEndpoints endpoints;
  private PolarisClient client;
  private ManagementApi managementApi;
  private CatalogApi catalogApi;
  private String sparkToken;
  private String catalogName;
  private String externalCatalogName;

  private URI warehouseDir;

  @BeforeAll
  public static void setup() throws IOException {
    s3Container.start();
  }

  @AfterAll
  public static void cleanup() {
    s3Container.stop();
  }

  @BeforeEach
  public void before(
      PolarisApiEndpoints apiEndpoints, ClientCredentials credentials, @TempDir Path tempDir) {
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    sparkToken = client.obtainToken(credentials);
    managementApi = client.managementApi(credentials);
    catalogApi = client.catalogApi(credentials);

    warehouseDir = IntegrationTestsHelper.getTemporaryDirectory(tempDir).resolve("spark-warehouse");

    catalogName = client.newEntityName("spark_catalog");
    externalCatalogName = client.newEntityName("spark_ext_catalog");

    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
            .build();
    CatalogProperties props = new CatalogProperties("s3://my-bucket/path/to/data");
    props.putAll(
        Map.of(
            "table-default.s3.endpoint",
            s3Container.getHttpEndpoint(),
            "table-default.s3.path-style-access",
            "true",
            "table-default.s3.access-key-id",
            "foo",
            "table-default.s3.secret-access-key",
            "bar",
            "s3.endpoint",
            s3Container.getHttpEndpoint(),
            "s3.path-style-access",
            "true",
            "s3.access-key-id",
            "foo",
            "s3.secret-access-key",
            "bar"));
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(props)
            .setStorageConfigInfo(awsConfigModel)
            .build();

    managementApi.createCatalog(catalog);

    CatalogProperties externalProps = new CatalogProperties("s3://my-bucket/path/to/data");
    externalProps.putAll(
        Map.of(
            "table-default.s3.endpoint",
            s3Container.getHttpEndpoint(),
            "table-default.s3.path-style-access",
            "true",
            "table-default.s3.access-key-id",
            "foo",
            "table-default.s3.secret-access-key",
            "bar",
            "s3.endpoint",
            s3Container.getHttpEndpoint(),
            "s3.path-style-access",
            "true",
            "s3.access-key-id",
            "foo",
            "s3.secret-access-key",
            "bar"));
    Catalog externalCatalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName(externalCatalogName)
            .setProperties(externalProps)
            .setStorageConfigInfo(awsConfigModel)
            .setRemoteUrl("http://dummy_url")
            .build();

    managementApi.createCatalog(externalCatalog);

    SparkSession.Builder sessionBuilder =
        SparkSession.builder()
            .master("local[1]")
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3.aws.credentials.provider",
                "org.apache.hadoop.fs.s3.TemporaryAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3.access.key", "foo")
            .config("spark.hadoop.fs.s3.secret.key", "bar")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.ui.showConsoleProgress", false)
            .config("spark.ui.enabled", "false");
    spark =
        withCatalog(withCatalog(sessionBuilder, catalogName), externalCatalogName).getOrCreate();

    onSpark("USE " + catalogName);
  }

  private SparkSession.Builder withCatalog(SparkSession.Builder builder, String catalogName) {
    return builder
        .config(
            String.format("spark.sql.catalog.%s", catalogName),
            "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.warehouse.dir", warehouseDir.toString())
        .config(String.format("spark.sql.catalog.%s.type", catalogName), "rest")
        .config(
            String.format("spark.sql.catalog.%s.uri", catalogName),
            endpoints.catalogApiEndpoint().toString())
        .config(String.format("spark.sql.catalog.%s.warehouse", catalogName), catalogName)
        .config(String.format("spark.sql.catalog.%s.scope", catalogName), "PRINCIPAL_ROLE:ALL")
        .config(
            String.format("spark.sql.catalog.%s.header.realm", catalogName), endpoints.realmId())
        .config(String.format("spark.sql.catalog.%s.token", catalogName), sparkToken)
        .config(String.format("spark.sql.catalog.%s.s3.access-key-id", catalogName), "fakekey")
        .config(
            String.format("spark.sql.catalog.%s.s3.secret-access-key", catalogName), "fakesecret")
        .config(String.format("spark.sql.catalog.%s.s3.region", catalogName), "us-west-2");
  }

  @AfterEach
  public void after() throws Exception {
    cleanupCatalog(catalogName);
    cleanupCatalog(externalCatalogName);
    try {
      SparkSession.clearDefaultSession();
      SparkSession.clearActiveSession();
      spark.close();
    } catch (Exception e) {
      LoggerFactory.getLogger(getClass()).error("Unable to close spark session", e);
    }

    client.close();
  }

  private void cleanupCatalog(String catalogName) {
    onSpark("USE " + catalogName);
    List<Row> namespaces = onSpark("SHOW NAMESPACES").collectAsList();
    for (Row namespace : namespaces) {
      List<Row> tables = onSpark("SHOW TABLES IN " + namespace.getString(0)).collectAsList();
      for (Row table : tables) {
        onSpark("DROP TABLE " + namespace.getString(0) + "." + table.getString(1));
      }
      List<Row> views = onSpark("SHOW VIEWS IN " + namespace.getString(0)).collectAsList();
      for (Row view : views) {
        onSpark("DROP VIEW " + namespace.getString(0) + "." + view.getString(1));
      }
      onSpark("DROP NAMESPACE " + namespace.getString(0));
    }

    managementApi.deleteCatalog(catalogName);
  }

  @Test
  public void testCreateTable() {
    long namespaceCount = onSpark("SHOW NAMESPACES").count();
    assertThat(namespaceCount).isEqualTo(0L);

    onSpark("CREATE NAMESPACE ns1");
    onSpark("USE ns1");
    onSpark("CREATE TABLE tb1 (col1 integer, col2 string)");
    onSpark("INSERT INTO tb1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    long recordCount = onSpark("SELECT * FROM tb1").count();
    assertThat(recordCount).isEqualTo(3);
  }

  @Test
  public void testCreateAndUpdateExternalTable() {
    long namespaceCount = onSpark("SHOW NAMESPACES").count();
    assertThat(namespaceCount).isEqualTo(0L);

    onSpark("CREATE NAMESPACE ns1");
    onSpark("USE ns1");
    onSpark("CREATE TABLE tb1 (col1 integer, col2 string)");
    onSpark("INSERT INTO tb1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    long recordCount = onSpark("SELECT * FROM tb1").count();
    assertThat(recordCount).isEqualTo(3);

    onSpark("USE " + externalCatalogName);
    List<Row> existingNamespaces = onSpark("SHOW NAMESPACES").collectAsList();
    assertThat(existingNamespaces).isEmpty();

    onSpark("CREATE NAMESPACE externalns1");
    onSpark("USE externalns1");
    List<Row> existingTables = onSpark("SHOW TABLES").collectAsList();
    assertThat(existingTables).isEmpty();

    LoadTableResponse tableResponse = loadTable(catalogName, "ns1", "tb1");
    try (Response registerResponse =
        catalogApi
            .request("v1/{cat}/namespaces/externalns1/register", Map.of("cat", externalCatalogName))
            .post(
                Entity.json(
                    ImmutableRegisterTableRequest.builder()
                        .name("mytb1")
                        .metadataLocation(tableResponse.metadataLocation())
                        .build()))) {
      assertThat(registerResponse).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
    }

    long tableCount = onSpark("SHOW TABLES").count();
    assertThat(tableCount).isEqualTo(1);
    List<Row> tables = onSpark("SHOW TABLES").collectAsList();
    assertThat(tables).hasSize(1).extracting(row -> row.getString(1)).containsExactly("mytb1");
    long rowCount = onSpark("SELECT * FROM mytb1").count();
    assertThat(rowCount).isEqualTo(3);
    assertThatThrownBy(() -> onSpark("INSERT INTO mytb1 VALUES (20, 'new_text')"))
        .isInstanceOf(Exception.class);

    onSpark("INSERT INTO " + catalogName + ".ns1.tb1 VALUES (20, 'new_text')");
    tableResponse = loadTable(catalogName, "ns1", "tb1");
    Map<String, Object> updateNotification =
        ImmutableMap.<String, Object>builder()
            .put("table-name", "mytb1")
            .put("timestamp", "" + Instant.now().toEpochMilli())
            .put("table-uuid", tableResponse.tableMetadata().uuid())
            .put("metadata-location", tableResponse.metadataLocation())
            .put("metadata", tableResponse.tableMetadata())
            .build();
    Map<String, Object> notificationRequest =
        ImmutableMap.<String, Object>builder()
            .put("payload", updateNotification)
            .put("notification-type", "UPDATE")
            .build();
    try (Response notifyResponse =
        catalogApi
            .request(
                "v1/{cat}/namespaces/externalns1/tables/mytb1/notifications",
                Map.of("cat", externalCatalogName))
            .post(Entity.json(notificationRequest))) {
      assertThat(notifyResponse)
          .extracting(Response::getStatus)
          .isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    // refresh the table so it queries for the latest metadata.json
    onSpark("REFRESH TABLE mytb1");
    rowCount = onSpark("SELECT * FROM mytb1").count();
    assertThat(rowCount).isEqualTo(4);
  }

  @Test
  public void testCreateView() {
    long namespaceCount = onSpark("SHOW NAMESPACES").count();
    assertThat(namespaceCount).isEqualTo(0L);

    onSpark("CREATE NAMESPACE ns1");
    onSpark("USE ns1");
    onSpark("CREATE TABLE tb1 (col1 integer, col2 string)");
    onSpark("INSERT INTO tb1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    onSpark("CREATE VIEW view1 AS SELECT * FROM tb1");
    long recordCount = onSpark("SELECT * FROM view1").count();
    assertThat(recordCount).isEqualTo(3);
  }

  private LoadTableResponse loadTable(String catalog, String namespace, String table) {
    try (Response response =
        catalogApi
            .request(
                "v1/{cat}/namespaces/{ns}/tables/{table}",
                Map.of("cat", catalog, "ns", namespace, "table", table))
            .get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      return response.readEntity(LoadTableResponse.class);
    }
  }

  private static Dataset<Row> onSpark(@Language("SQL") String sql) {
    return spark.sql(sql);
  }
}
