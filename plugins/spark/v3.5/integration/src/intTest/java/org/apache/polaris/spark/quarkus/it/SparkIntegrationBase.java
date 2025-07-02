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
package org.apache.polaris.spark.quarkus.it;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.FormatMethod;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.it.ext.SparkSessionBuilder;
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
public abstract class SparkIntegrationBase {
  protected static final S3MockContainer s3Container =
      new S3MockContainer("3.11.0").withInitialBuckets("my-bucket,my-old-bucket");
  protected static SparkSession spark;
  protected PolarisApiEndpoints endpoints;
  protected PolarisManagementClient client;
  protected ManagementApi managementApi;
  protected String catalogName;
  protected String sparkToken;

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
    client = PolarisManagementClient.managementClient(endpoints);
    sparkToken = client.obtainToken(credentials);
    managementApi = client.managementApi(credentials);

    warehouseDir = IntegrationTestsHelper.getTemporaryDirectory(tempDir).resolve("spark-warehouse");

    catalogName = client.newEntityName("spark_catalog");

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

    spark = buildSparkSession();

    onSpark("USE " + catalogName);
  }

  protected SparkSession buildSparkSession() {
    return SparkSessionBuilder.buildWithTestDefaults()
        .withExtensions(
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
        .withConfig(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .withWarehouse(warehouseDir)
        .addCatalog(catalogName, "org.apache.polaris.spark.SparkCatalog", endpoints, sparkToken)
        .getOrCreate();
  }

  @AfterEach
  public void after() throws Exception {
    cleanupCatalog(catalogName);
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

  @FormatMethod
  protected List<Object[]> sql(String query, Object... args) {
    List<Row> rows = spark.sql(String.format(query, args)).collectAsList();
    if (rows.isEmpty()) {
      return ImmutableList.of();
    }
    return rowsToJava(rows);
  }

  protected List<Object[]> rowsToJava(List<Row> rows) {
    return rows.stream().map(this::toJava).collect(Collectors.toList());
  }

  private Object[] toJava(Row row) {
    return IntStream.range(0, row.size())
        .mapToObj(
            pos -> {
              if (row.isNullAt(pos)) {
                return null;
              }

              Object value = row.get(pos);
              if (value instanceof Row) {
                return toJava((Row) value);
              } else if (value instanceof scala.collection.Seq) {
                return row.getList(pos);
              } else if (value instanceof scala.collection.Map) {
                return row.getJavaMap(pos);
              } else {
                return value;
              }
            })
        .toArray(Object[]::new);
  }

  /** List the name of directories under a given path non-recursively. */
  protected List<String> listDirs(String path) {
    File directory = new File(path);
    return FileUtils.listFilesAndDirs(
            directory, FalseFileFilter.INSTANCE, DirectoryFileFilter.DIRECTORY)
        .stream()
        .map(File::getName)
        .toList();
  }

  /** Generate a string name with given prefix and a random suffix */
  protected String generateName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replaceAll("-", "");
  }

  protected static Dataset<Row> onSpark(@Language("SQL") String sql) {
    return spark.sql(sql);
  }
}
