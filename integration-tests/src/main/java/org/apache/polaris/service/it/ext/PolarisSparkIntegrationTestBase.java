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
package org.apache.polaris.service.it.ext;

import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

@ExtendWith(PolarisIntegrationTestExtension.class)
public abstract class PolarisSparkIntegrationTestBase {
  protected static final S3MockContainer s3Container =
      new S3MockContainer("3.11.0").withInitialBuckets("my-bucket,my-old-bucket");
  protected static SparkSession spark;
  protected PolarisApiEndpoints endpoints;
  protected PolarisClient client;
  protected ManagementApi managementApi;
  protected CatalogApi catalogApi;
  protected String sparkToken;
  protected String catalogName;
  protected String externalCatalogName;

  protected URI warehouseDir;

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
            "bar",
            "polaris.config.drop-with-purge.enabled",
            "true"));
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
            "bar",
            "polaris.config.drop-with-purge.enabled",
            "true"));
    Catalog externalCatalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName(externalCatalogName)
            .setProperties(externalProps)
            .setStorageConfigInfo(awsConfigModel)
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

  protected SparkSession.Builder withCatalog(SparkSession.Builder builder, String catalogName) {
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

  protected void cleanupCatalog(String catalogName) {
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

  protected static Dataset<Row> onSpark(@Language("SQL") String sql) {
    return spark.sql(sql);
  }
}
