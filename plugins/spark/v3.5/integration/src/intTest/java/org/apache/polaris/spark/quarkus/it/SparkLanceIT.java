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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@QuarkusIntegrationTest
public class SparkLanceIT extends SparkIntegrationBase {
  private String defaultNs;
  private String tableRootDir;

  private String getTableLocation(String tableName) {
    return String.format("%s/%s", tableRootDir, tableName);
  }

  private String getTableNameWithRandomSuffix() {
    return generateName("lancetb");
  }

  @BeforeEach
  public void createDefaultResources(@TempDir Path tempDir) {
    spark.sparkContext().setLogLevel("WARN");
    defaultNs = generateName("lance");
    // create a default namespace
    sql("CREATE NAMESPACE %s", defaultNs);
    sql("USE NAMESPACE %s", defaultNs);
    tableRootDir =
        IntegrationTestsHelper.getTemporaryDirectory(tempDir).resolve(defaultNs).getPath();
  }

  @AfterEach
  public void cleanupLanceData() {
    // clean up lance data
    File dirToDelete = new File(tableRootDir);
    FileUtils.deleteQuietly(dirToDelete);
    sql("DROP NAMESPACE %s", defaultNs);
  }

  @Test
  public void testBasicTableOperations() {
    // create a regular lance table with provider property
    String lancetb1 = "lancetb1";
    sql(
        "CREATE TABLE %s (id INT, name STRING) LOCATION '%s' TBLPROPERTIES ('provider' = 'lance')",
        lancetb1, getTableLocation(lancetb1));
    sql("INSERT INTO %s VALUES (1, 'anna'), (2, 'bob')", lancetb1);
    List<Object[]> results = sql("SELECT * FROM %s WHERE id > 1 ORDER BY id DESC", lancetb1);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0)).isEqualTo(new Object[] {2, "bob"});

    // Lance doesn't support partitioned tables, so we'll test a table with more complex data types
    String lancetb2 = "lancetb2";
    sql(
        "CREATE TABLE %s (name String, age INT, score DOUBLE, active BOOLEAN) LOCATION '%s' TBLPROPERTIES ('provider' = 'lance')",
        lancetb2, getTableLocation(lancetb2));
    sql(
        "INSERT INTO %s VALUES ('anna', 10, 95.5, true), ('james', 32, 87.2, false), ('yan', 16, 92.0, true)",
        lancetb2);
    results = sql("SELECT name, score FROM %s ORDER BY age", lancetb2);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0)).isEqualTo(new Object[] {"anna", 95.5});
    assertThat(results.get(1)).isEqualTo(new Object[] {"yan", 92.0});
    assertThat(results.get(2)).isEqualTo(new Object[] {"james", 87.2});

    // drop table
    sql("DROP TABLE %s", lancetb1);
    sql("DROP TABLE %s", lancetb2);

    // check drop works
    assertThatThrownBy(() -> sql("SELECT * FROM %s", lancetb1))
        .hasMessageContaining("TABLE_OR_VIEW_NOT_FOUND");
  }

  @Test
  public void testCreateTableWithLocationViaSparkDataFrame() {
    String lancetb1 = getTableNameWithRandomSuffix();
    String lancetb2 = getTableNameWithRandomSuffix();

    // create a dataframe with some data
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("name", DataTypes.StringType, false, Metadata.empty())
            });
    List<Row> data = Arrays.asList(RowFactory.create(1, "anna"), RowFactory.create(2, "bob"));
    Dataset<Row> df = spark.createDataFrame(data, schema);

    // write the dataframe to a lance table
    df.write()
        .mode("overwrite")
        .format("lance")
        .option("path", getTableLocation(lancetb1))
        .saveAsTable(lancetb1);

    // read the table
    List<Object[]> results = sql("SELECT * FROM %s ORDER BY id", lancetb1);
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0)).isEqualTo(new Object[] {1, "anna"});
    assertThat(results.get(1)).isEqualTo(new Object[] {2, "bob"});

    // create another table with different data
    List<Row> data2 = Arrays.asList(RowFactory.create(3, "charlie"), RowFactory.create(4, "david"));
    Dataset<Row> df2 = spark.createDataFrame(data2, schema);
    df2.write()
        .mode("overwrite")
        .format("lance")
        .option("path", getTableLocation(lancetb2))
        .saveAsTable(lancetb2);

    // verify data isolation
    results = sql("SELECT * FROM %s ORDER BY id", lancetb2);
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0)).isEqualTo(new Object[] {3, "charlie"});
    assertThat(results.get(1)).isEqualTo(new Object[] {4, "david"});

    sql("DROP TABLE %s", lancetb1);
    sql("DROP TABLE %s", lancetb2);
  }

  @Test
  public void testComplexDataTypes() {
    String lancetb = getTableNameWithRandomSuffix();

    // Create table with array and struct types
    sql(
        "CREATE TABLE %s (id INT, tags ARRAY<STRING>, info STRUCT<city:STRING, zip:INT>) LOCATION '%s' TBLPROPERTIES ('provider' = 'lance')",
        lancetb, getTableLocation(lancetb));

    // Insert data with complex types
    sql(
        "INSERT INTO %s VALUES (1, array('tag1', 'tag2'), struct('NYC', 10001)), (2, array('tag3'), struct('SF', 94105))",
        lancetb);

    // Query complex types
    List<Object[]> results = sql("SELECT id, tags[0], info.city FROM %s ORDER BY id", lancetb);
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0)[0]).isEqualTo(1);
    assertThat(results.get(0)[1]).isEqualTo("tag1");
    assertThat(results.get(0)[2]).isEqualTo("NYC");
    assertThat(results.get(1)[0]).isEqualTo(2);
    assertThat(results.get(1)[1]).isEqualTo("tag3");
    assertThat(results.get(1)[2]).isEqualTo("SF");

    sql("DROP TABLE %s", lancetb);
  }

  @Test
  public void testReadWritePerformance() {
    String lancetb = getTableNameWithRandomSuffix();

    // Create table
    sql(
        "CREATE TABLE %s (id BIGINT, value DOUBLE, data STRING) LOCATION '%s' TBLPROPERTIES ('provider' = 'lance')",
        lancetb, getTableLocation(lancetb));

    // Insert batch of data
    StringBuilder insertSql = new StringBuilder("INSERT INTO " + lancetb + " VALUES ");
    for (int i = 0; i < 100; i++) {
      if (i > 0) insertSql.append(", ");
      insertSql.append(String.format("(%d, %f, 'data_%d')", i, i * 1.5, i));
    }
    sql(insertSql.toString());

    // Verify count
    List<Object[]> results = sql("SELECT COUNT(*) FROM %s", lancetb);
    assertThat(results.get(0)[0]).isEqualTo(100L);

    // Test filtering
    results = sql("SELECT * FROM %s WHERE id >= 90 ORDER BY id", lancetb);
    assertThat(results.size()).isEqualTo(10);
    assertThat(results.get(0)).isEqualTo(new Object[] {90L, 135.0, "data_90"});

    sql("DROP TABLE %s", lancetb);
  }

  @Test
  public void testMixedTableFormats() {
    // Test that Lance tables can coexist with Iceberg tables
    String icebergTable = "iceberg_table";
    String lanceTable = "lance_table";

    // Create an Iceberg table (default provider)
    sql(
        "CREATE TABLE %s (id INT, name STRING) LOCATION '%s'",
        icebergTable, getTableLocation(icebergTable));
    sql("INSERT INTO %s VALUES (1, 'iceberg_data')", icebergTable);

    // Create a Lance table
    sql(
        "CREATE TABLE %s (id INT, name STRING) LOCATION '%s' TBLPROPERTIES ('provider' = 'lance')",
        lanceTable, getTableLocation(lanceTable));
    sql("INSERT INTO %s VALUES (2, 'lance_data')", lanceTable);

    // Query both tables
    List<Object[]> icebergResults = sql("SELECT * FROM %s", icebergTable);
    assertThat(icebergResults.size()).isEqualTo(1);
    assertThat(icebergResults.get(0)).isEqualTo(new Object[] {1, "iceberg_data"});

    List<Object[]> lanceResults = sql("SELECT * FROM %s", lanceTable);
    assertThat(lanceResults.size()).isEqualTo(1);
    assertThat(lanceResults.get(0)).isEqualTo(new Object[] {2, "lance_data"});

    // Clean up
    sql("DROP TABLE %s", icebergTable);
    sql("DROP TABLE %s", lanceTable);
  }
}
