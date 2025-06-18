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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.io.File;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@QuarkusIntegrationTest
public class SparkHudiIT extends SparkIntegrationBase {

  @Override
  protected SparkSession.Builder withCatalog(SparkSession.Builder builder, String catalogName) {
    return builder
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config(
            String.format("spark.sql.catalog.%s", catalogName),
            "org.apache.polaris.spark.SparkCatalog")
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
        .config(String.format("spark.sql.catalog.%s.s3.region", catalogName), "us-west-2")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        // for intial integration test have disabled for now, to revisit enabling in future
        .config("hoodie.metadata.enable", "false");
  }

  private String defaultNs;
  private String tableRootDir;

  private String getTableLocation(String tableName) {
    return String.format("%s/%s", tableRootDir, tableName);
  }

  private String getTableNameWithRandomSuffix() {
    return generateName("huditb");
  }

  @BeforeEach
  public void createDefaultResources(@TempDir Path tempDir) {
    spark.sparkContext().setLogLevel("INFO");
    defaultNs = generateName("hudi");
    // create a default namespace
    sql("CREATE NAMESPACE %s", defaultNs);
    sql("USE NAMESPACE %s", defaultNs);
    tableRootDir =
        IntegrationTestsHelper.getTemporaryDirectory(tempDir).resolve(defaultNs).getPath();
  }

  @AfterEach
  public void cleanupHudiData() {
    // clean up hudi data
    File dirToDelete = new File(tableRootDir);
    FileUtils.deleteQuietly(dirToDelete);
    sql("DROP NAMESPACE %s", defaultNs);
  }

  @Test
  public void testBasicTableOperations() {
    // create a regular hudi table
    String huditb1 = "huditb1";
    sql(
        "CREATE TABLE %s (id INT, name STRING) USING HUDI LOCATION '%s'",
        huditb1, getTableLocation(huditb1));
    sql("INSERT INTO %s VALUES (1, 'anna'), (2, 'bob')", huditb1);
    List<Object[]> results = sql("SELECT id,name FROM %s WHERE id > 1 ORDER BY id DESC", huditb1);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0)).isEqualTo(new Object[] {2, "bob"});

    // create a hudi table with partition
    String huditb2 = "huditb2";
    sql(
        "CREATE TABLE %s (name String, age INT, country STRING) USING HUDI PARTITIONED BY (country) LOCATION '%s'",
        huditb2, getTableLocation(huditb2));
    sql(
        "INSERT INTO %s VALUES ('anna', 10, 'US'), ('james', 32, 'US'), ('yan', 16, 'CHINA')",
        huditb2);
    results = sql("SELECT name, country FROM %s ORDER BY age", huditb2);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0)).isEqualTo(new Object[] {"anna", "US"});
    assertThat(results.get(1)).isEqualTo(new Object[] {"yan", "CHINA"});
    assertThat(results.get(2)).isEqualTo(new Object[] {"james", "US"});

    // verify the partition dir is created
    List<String> subDirs = listDirs(getTableLocation(huditb2));
    assertThat(subDirs).contains(".hoodie", "country=CHINA", "country=US");

    // test listTables
    List<Object[]> tables = sql("SHOW TABLES");
    assertThat(tables.size()).isEqualTo(2);
    assertThat(tables)
        .contains(
            new Object[] {defaultNs, huditb1, false}, new Object[] {defaultNs, huditb2, false});

    sql("DROP TABLE %s", huditb1);
    sql("DROP TABLE %s", huditb2);
    tables = sql("SHOW TABLES");
    assertThat(tables.size()).isEqualTo(0);
  }

  @Test
  public void testUnsupportedAlterTableOperations() {
    String huditb = getTableNameWithRandomSuffix();
    sql(
        "CREATE TABLE %s (name String, age INT, country STRING) USING HUDI PARTITIONED BY (country) LOCATION '%s'",
        huditb, getTableLocation(huditb));

    // ALTER TABLE ... RENAME TO ... fails
    assertThatThrownBy(() -> sql("ALTER TABLE %s RENAME TO new_hudi", huditb))
        .isInstanceOf(UnsupportedOperationException.class);

    // ALTER TABLE ... SET LOCATION ... fails
    assertThatThrownBy(() -> sql("ALTER TABLE %s SET LOCATION '/tmp/new/path'", huditb))
        .isInstanceOf(UnsupportedOperationException.class);

    sql("DROP TABLE %s", huditb);
  }

  @Test
  public void testUnsupportedTableCreateOperations() {
    String huditb = getTableNameWithRandomSuffix();
    // create hudi table with no location
    assertThatThrownBy(() -> sql("CREATE TABLE %s (id INT, name STRING) USING HUDI", huditb))
        .isInstanceOf(UnsupportedOperationException.class);

    // CTAS fails
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s USING HUDI LOCATION '%s' AS SELECT 1 AS id",
                    huditb, getTableLocation(huditb)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
