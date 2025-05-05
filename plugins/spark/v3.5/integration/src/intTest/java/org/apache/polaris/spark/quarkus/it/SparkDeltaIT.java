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
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.DeltaAnalysisException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@QuarkusIntegrationTest
public class SparkDeltaIT extends SparkIntegrationBase {
  private String defaultNs;
  private String tableRootDir;

  private String getTableLocation(String tableName) {
    return String.format("%s/%s", tableRootDir, tableName);
  }

  private String getTableNameWithRandomSuffix() {
    return generateName("deltatb");
  }

  @BeforeEach
  public void createDefaultResources(@TempDir Path tempDir) {
    spark.sparkContext().setLogLevel("WARN");
    defaultNs = generateName("delta");
    // create a default namespace
    sql("CREATE NAMESPACE %s", defaultNs);
    sql("USE NAMESPACE %s", defaultNs);
    tableRootDir =
        IntegrationTestsHelper.getTemporaryDirectory(tempDir).resolve(defaultNs).getPath();
  }

  @AfterEach
  public void cleanupDeltaData() {
    // clean up delta data
    File dirToDelete = new File(tableRootDir);
    FileUtils.deleteQuietly(dirToDelete);
    sql("DROP NAMESPACE %s", defaultNs);
  }

  @Test
  public void testBasicTableOperations() {
    // create a regular delta table
    String deltatb1 = "deltatb1";
    sql(
        "CREATE TABLE %s (id INT, name STRING) USING DELTA LOCATION '%s'",
        deltatb1, getTableLocation(deltatb1));
    sql("INSERT INTO %s VALUES (1, 'anna'), (2, 'bob')", deltatb1);
    List<Object[]> results = sql("SELECT * FROM %s WHERE id > 1 ORDER BY id DESC", deltatb1);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0)).isEqualTo(new Object[] {2, "bob"});

    // create a detla table with partition
    String deltatb2 = "deltatb2";
    sql(
        "CREATE TABLE %s (name String, age INT, country STRING) USING DELTA PARTITIONED BY (country) LOCATION '%s'",
        deltatb2, getTableLocation(deltatb2));
    sql(
        "INSERT INTO %s VALUES ('anna', 10, 'US'), ('james', 32, 'US'), ('yan', 16, 'CHINA')",
        deltatb2);
    results = sql("SELECT name, country FROM %s ORDER BY age", deltatb2);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0)).isEqualTo(new Object[] {"anna", "US"});
    assertThat(results.get(1)).isEqualTo(new Object[] {"yan", "CHINA"});
    assertThat(results.get(2)).isEqualTo(new Object[] {"james", "US"});

    // verify the partition dir is created
    List<String> subDirs = listDirs(getTableLocation(deltatb2));
    assertThat(subDirs).contains("_delta_log", "country=CHINA", "country=US");

    // test listTables
    List<Object[]> tables = sql("SHOW TABLES");
    assertThat(tables.size()).isEqualTo(2);
    assertThat(tables)
        .contains(
            new Object[] {defaultNs, deltatb1, false}, new Object[] {defaultNs, deltatb2, false});

    sql("DROP TABLE %s", deltatb1);
    sql("DROP TABLE %s", deltatb2);
    tables = sql("SHOW TABLES");
    assertThat(tables.size()).isEqualTo(0);
  }

  @Test
  public void testAlterOperations() {
    String deltatb = getTableNameWithRandomSuffix();
    sql(
        "CREATE TABLE %s (id INT, name STRING) USING DELTA LOCATION '%s'",
        deltatb, getTableLocation(deltatb));
    sql("INSERT INTO %s VALUES (1, 'anna'), (2, 'bob')", deltatb);

    // test alter columns
    // add two new columns to the table
    sql("Alter TABLE %s ADD COLUMNS (city STRING, age INT)", deltatb);
    // add one more row to the table
    sql("INSERT INTO %s VALUES (3, 'john', 'SFO', 20)", deltatb);
    // verify the table now have 4 columns with correct result
    List<Object[]> results = sql("SELECT * FROM %s ORDER BY id", deltatb);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results).contains(new Object[] {1, "anna", null, null});
    assertThat(results).contains(new Object[] {2, "bob", null, null});
    assertThat(results).contains(new Object[] {3, "john", "SFO", 20});

    // drop and rename column require set the delta.columnMapping property
    sql("ALTER TABLE %s SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')", deltatb);
    // drop column age
    sql("Alter TABLE %s DROP COLUMN age", deltatb);
    // verify the table now have 3 columns with correct result
    results = sql("SELECT * FROM %s ORDER BY id", deltatb);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results).contains(new Object[] {1, "anna", null});
    assertThat(results).contains(new Object[] {2, "bob", null});
    assertThat(results).contains(new Object[] {3, "john", "SFO"});

    // rename column city to address
    sql("Alter TABLE %s RENAME COLUMN city TO address", deltatb);
    // verify column address exists
    results = sql("SELECT id, address FROM %s ORDER BY id", deltatb);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results).contains(new Object[] {1, null});
    assertThat(results).contains(new Object[] {2, null});
    assertThat(results).contains(new Object[] {3, "SFO"});

    // test alter properties
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('description' = 'people table', 'test-owner' = 'test-user')",
        deltatb);
    List<Object[]> tableInfo = sql("DESCRIBE TABLE EXTENDED %s", deltatb);
    // find the table properties result
    String properties = null;
    for (Object[] info : tableInfo) {
      if (info[0].equals("Table Properties")) {
        properties = (String) info[1];
        break;
      }
    }
    assertThat(properties).contains("description=people table,test-owner=test-user");
    sql("DROP TABLE %s", deltatb);
  }

  @Test
  public void testUnsupportedAlterTableOperations() {
    String deltatb = getTableNameWithRandomSuffix();
    sql(
        "CREATE TABLE %s (name String, age INT, country STRING) USING DELTA PARTITIONED BY (country) LOCATION '%s'",
        deltatb, getTableLocation(deltatb));

    // ALTER TABLE ... RENAME TO ... fails
    assertThatThrownBy(() -> sql("ALTER TABLE %s RENAME TO new_delta", deltatb))
        .isInstanceOf(UnsupportedOperationException.class);

    // ALTER TABLE ... SET LOCATION ... fails
    assertThatThrownBy(() -> sql("ALTER TABLE %s SET LOCATION '/tmp/new/path'", deltatb))
        .isInstanceOf(DeltaAnalysisException.class);

    sql("DROP TABLE %s", deltatb);
  }

  @Test
  public void testUnsupportedTableCreateOperations() {
    String deltatb = getTableNameWithRandomSuffix();
    // create delta table with no location
    assertThatThrownBy(() -> sql("CREATE TABLE %s (id INT, name STRING) USING DELTA", deltatb))
        .isInstanceOf(UnsupportedOperationException.class);

    // CTAS fails
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s USING DELTA LOCATION '%s' AS SELECT 1 AS id",
                    deltatb, getTableLocation(deltatb)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testDataframeSaveOperations() {
    List<Row> data = Arrays.asList(RowFactory.create("Alice", 30), RowFactory.create("Bob", 25));
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("name", DataTypes.StringType, false, Metadata.empty()),
              new StructField("age", DataTypes.IntegerType, false, Metadata.empty())
            });
    Dataset<Row> df = spark.createDataFrame(data, schema);

    String deltatb = getTableNameWithRandomSuffix();
    // saveAsTable requires support for delta requires CTAS support for third party catalog
    // in delta catalog, which is currently not supported.
    assertThatThrownBy(
            () ->
                df.write()
                    .format("delta")
                    .option("path", getTableLocation(deltatb))
                    .saveAsTable(deltatb))
        .isInstanceOf(IllegalArgumentException.class);

    // verify regular dataframe saving still works
    df.write().format("delta").save(getTableLocation(deltatb));

    // verify the partition dir is created
    List<String> subDirs = listDirs(getTableLocation(deltatb));
    assertThat(subDirs).contains("_delta_log");

    // verify we can create a table out of the exising delta location
    sql("CREATE TABLE %s USING DELTA LOCATION '%s'", deltatb, getTableLocation(deltatb));
    List<Object[]> tables = sql("SHOW TABLES");
    assertThat(tables.size()).isEqualTo(1);
    assertThat(tables).contains(new Object[] {defaultNs, deltatb, false});

    sql("INSERT INTO %s VALUES ('Anna', 11)", deltatb);

    List<Object[]> results = sql("SELECT * FROM %s ORDER BY name", deltatb);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0)).isEqualTo(new Object[] {"Alice", 30});
    assertThat(results.get(1)).isEqualTo(new Object[] {"Anna", 11});
    assertThat(results.get(2)).isEqualTo(new Object[] {"Bob", 25});

    sql("DROP TABLE %s", deltatb);
  }
}
