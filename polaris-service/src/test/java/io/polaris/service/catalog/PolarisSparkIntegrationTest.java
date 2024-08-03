/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.service.catalog;

import static io.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.polaris.core.admin.model.AwsStorageConfigInfo;
import io.polaris.core.admin.model.Catalog;
import io.polaris.core.admin.model.CatalogProperties;
import io.polaris.core.admin.model.ExternalCatalog;
import io.polaris.core.admin.model.PolarisCatalog;
import io.polaris.core.admin.model.StorageConfigInfo;
import io.polaris.service.PolarisApplication;
import io.polaris.service.config.PolarisApplicationConfig;
import io.polaris.service.test.PolarisConnectionExtension;
import io.polaris.service.types.NotificationRequest;
import io.polaris.service.types.NotificationType;
import io.polaris.service.types.TableUpdateNotification;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;

@ExtendWith({DropwizardExtensionsSupport.class, PolarisConnectionExtension.class})
public class PolarisSparkIntegrationTest {
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          ConfigOverride.config(
              "server.applicationConnectors[0].port",
              "0"), // Bind to random port to support parallelism
          ConfigOverride.config(
              "server.adminConnectors[0].port", "0"), // Bind to random port to support parallelism
          ConfigOverride.config(
              "featureConfiguration.ENFORCE_GLOBALLY_UNIQUE_TABLE_LOCATIONS", "false"));

  public static final String CATALOG_NAME = "mycatalog";
  public static final String EXTERNAL_CATALOG_NAME = "external_catalog";
  private static S3MockContainer s3Container =
      new S3MockContainer("3.9.1").withInitialBuckets("my-bucket,my-old-bucket");
  private static PolarisConnectionExtension.PolarisToken polarisToken;
  private static SparkSession spark;
  private static String realm;

  @BeforeAll
  public static void setup(PolarisConnectionExtension.PolarisToken polarisToken) {
    s3Container.start();
    PolarisSparkIntegrationTest.polarisToken = polarisToken;
    realm = PolarisConnectionExtension.getTestRealm(PolarisSparkIntegrationTest.class);
  }

  @AfterAll
  public static void cleanup() {
    s3Container.stop();
  }

  @BeforeEach
  public void before() {
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
            .setName(CATALOG_NAME)
            .setProperties(props)
            .setStorageConfigInfo(awsConfigModel)
            .build();

    try (Response response =
        EXT.client()
            .target(
                String.format("http://localhost:%d/api/management/v1/catalogs", EXT.getLocalPort()))
            .request("application/json")
            .header("Authorization", "BEARER " + polarisToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(catalog))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    CatalogProperties externalProps = new CatalogProperties("s3://my-bucket/path/to/data");
    externalProps.putAll(
        Map.of(
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
            .setName(EXTERNAL_CATALOG_NAME)
            .setProperties(externalProps)
            .setStorageConfigInfo(awsConfigModel)
            .setRemoteUrl("http://dummy_url")
            .build();
    try (Response response =
        EXT.client()
            .target(
                String.format("http://localhost:%d/api/management/v1/catalogs", EXT.getLocalPort()))
            .request("application/json")
            .header("Authorization", "BEARER " + polarisToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(externalCatalog))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
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
        withCatalog(withCatalog(sessionBuilder, CATALOG_NAME), EXTERNAL_CATALOG_NAME).getOrCreate();

    spark.sql("USE " + CATALOG_NAME);
  }

  private SparkSession.Builder withCatalog(SparkSession.Builder builder, String catalogName) {
    return builder
        .config(
            String.format("spark.sql.catalog.%s", catalogName),
            "org.apache.iceberg.spark.SparkCatalog")
        .config(String.format("spark.sql.catalog.%s.type", catalogName), "rest")
        .config(
            String.format("spark.sql.catalog.%s.uri", catalogName),
            "http://localhost:" + EXT.getLocalPort() + "/api/catalog")
        .config(String.format("spark.sql.catalog.%s.warehouse", catalogName), catalogName)
        .config(String.format("spark.sql.catalog.%s.scope", catalogName), "PRINCIPAL_ROLE:ALL")
        .config(String.format("spark.sql.catalog.%s.header.realm", catalogName), realm)
        .config(String.format("spark.sql.catalog.%s.token", catalogName), polarisToken.token())
        .config(String.format("spark.sql.catalog.%s.s3.access-key-id", catalogName), "fakekey")
        .config(
            String.format("spark.sql.catalog.%s.s3.secret-access-key", catalogName), "fakesecret")
        .config(String.format("spark.sql.catalog.%s.s3.region", catalogName), "us-west-2");
  }

  @AfterEach
  public void after() {
    cleanupCatalog(CATALOG_NAME);
    cleanupCatalog(EXTERNAL_CATALOG_NAME);
    try {
      SparkSession.clearDefaultSession();
      SparkSession.clearActiveSession();
      spark.close();
    } catch (Exception e) {
      LoggerFactory.getLogger(getClass()).error("Unable to close spark session", e);
    }
  }

  private void cleanupCatalog(String catalogName) {
    spark.sql("USE " + catalogName);
    List<Row> namespaces = spark.sql("SHOW NAMESPACES").collectAsList();
    for (Row namespace : namespaces) {
      List<Row> tables = spark.sql("SHOW TABLES IN " + namespace.getString(0)).collectAsList();
      for (Row table : tables) {
        spark.sql("DROP TABLE " + namespace.getString(0) + "." + table.getString(1));
      }
      spark.sql("DROP NAMESPACE " + namespace.getString(0));
    }
    try (Response response =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/catalogs/" + catalogName,
                    EXT.getLocalPort()))
            .request("application/json")
            .header("Authorization", "BEARER " + polarisToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .delete()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testCreateTable() {
    long namespaceCount = spark.sql("SHOW NAMESPACES").count();
    assertThat(namespaceCount).isEqualTo(0L);

    spark.sql("CREATE NAMESPACE ns1");
    spark.sql("USE ns1");
    spark.sql("CREATE TABLE tb1 (col1 integer, col2 string)");
    spark.sql("INSERT INTO tb1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    long recordCount = spark.sql("SELECT * FROM tb1").count();
    assertThat(recordCount).isEqualTo(3);
  }

  @Test
  public void testCreateAndUpdateExternalTable() {
    long namespaceCount = spark.sql("SHOW NAMESPACES").count();
    assertThat(namespaceCount).isEqualTo(0L);

    spark.sql("CREATE NAMESPACE ns1");
    spark.sql("USE ns1");
    spark.sql("CREATE TABLE tb1 (col1 integer, col2 string)");
    spark.sql("INSERT INTO tb1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    long recordCount = spark.sql("SELECT * FROM tb1").count();
    assertThat(recordCount).isEqualTo(3);

    spark.sql("USE " + EXTERNAL_CATALOG_NAME);
    List<Row> existingNamespaces = spark.sql("SHOW NAMESPACES").collectAsList();
    assertThat(existingNamespaces).isEmpty();

    spark.sql("CREATE NAMESPACE externalns1");
    spark.sql("USE externalns1");
    List<Row> existingTables = spark.sql("SHOW TABLES").collectAsList();
    assertThat(existingTables).isEmpty();

    LoadTableResponse tableResponse = loadTable(CATALOG_NAME, "ns1", "tb1");
    try (Response registerResponse =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/catalog/v1/"
                        + EXTERNAL_CATALOG_NAME
                        + "/namespaces/externalns1/register",
                    EXT.getLocalPort()))
            .request("application/json")
            .header("Authorization", "BEARER " + polarisToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .post(
                Entity.json(
                    ImmutableRegisterTableRequest.builder()
                        .name("mytb1")
                        .metadataLocation(tableResponse.metadataLocation())
                        .build()))) {
      String reponseBody = registerResponse.readEntity(String.class); // todo remove this
      assertThat(registerResponse).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
    }

    long tableCount = spark.sql("SHOW TABLES").count();
    assertThat(tableCount).isEqualTo(1);
    List<Row> tables = spark.sql("SHOW TABLES").collectAsList();
    assertThat(tables).hasSize(1).extracting(row -> row.getString(1)).containsExactly("mytb1");
    long rowCount = spark.sql("SELECT * FROM mytb1").count();
    assertThat(rowCount).isEqualTo(3);
    assertThatThrownBy(() -> spark.sql("INSERT INTO mytb1 VALUES (20, 'new_text')"))
        .isInstanceOf(Exception.class);

    spark.sql("INSERT INTO " + CATALOG_NAME + ".ns1.tb1 VALUES (20, 'new_text')");
    tableResponse = loadTable(CATALOG_NAME, "ns1", "tb1");
    TableUpdateNotification updateNotification =
        new TableUpdateNotification(
            "mytb1",
            Instant.now().toEpochMilli(),
            tableResponse.tableMetadata().uuid(),
            tableResponse.metadataLocation(),
            tableResponse.tableMetadata());
    NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setPayload(updateNotification);
    notificationRequest.setNotificationType(NotificationType.UPDATE);
    try (Response notifyResponse =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/catalog/v1/%s/namespaces/externalns1/tables/mytb1/notifications",
                    EXT.getLocalPort(), EXTERNAL_CATALOG_NAME))
            .request("application/json")
            .header("Authorization", "BEARER " + polarisToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(notificationRequest))) {
      assertThat(notifyResponse)
          .returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
    // refresh the table so it queries for the latest metadata.json
    spark.sql("REFRESH TABLE mytb1");
    rowCount = spark.sql("SELECT * FROM mytb1").count();
    assertThat(rowCount).isEqualTo(4);
  }

  private LoadTableResponse loadTable(String catalog, String namespace, String table) {
    try (Response response =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/catalog/v1/%s/namespaces/%s/tables/%s",
                    EXT.getLocalPort(), catalog, namespace, table))
            .request("application/json")
            .header("Authorization", "BEARER " + polarisToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      return response.readEntity(LoadTableResponse.class);
    }
  }
}
