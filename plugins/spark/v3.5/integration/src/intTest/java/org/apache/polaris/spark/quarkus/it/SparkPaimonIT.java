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
import org.apache.polaris.service.it.ext.SparkSessionBuilder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@QuarkusIntegrationTest
public class SparkPaimonIT extends SparkIntegrationBase {
  private String defaultNs;
  private String paimonWarehouseDir;

  private String getTableNameWithRandomSuffix() {
    return generateName("paimontb");
  }

  @Override
  protected SparkSession buildSparkSession() {
    // Get Paimon warehouse path - use parent of Spark warehouse to create a sibling directory
    paimonWarehouseDir = warehouseDir.resolve("../paimon_warehouse").normalize().toString();

    return SparkSessionBuilder.buildWithTestDefaults()
        .withExtensions(
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                + "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
        .withWarehouse(warehouseDir)
        .addCatalog(catalogName, "org.apache.polaris.spark.SparkCatalog", endpoints, sparkToken)
        // Configure Paimon warehouse for the catalog
        .withConfig("spark.sql.catalog." + catalogName + ".paimon-warehouse", paimonWarehouseDir)
        .getOrCreate();
  }

  @BeforeEach
  public void createDefaultResources(@TempDir Path tempDir) {
    spark.sparkContext().setLogLevel("WARN");
    defaultNs = generateName("paimon");
    // create a default namespace
    sql("CREATE NAMESPACE %s", defaultNs);
    sql("USE NAMESPACE %s", defaultNs);
  }

  @AfterEach
  public void cleanupPaimonData() {
    // clean up paimon data
    if (paimonWarehouseDir != null) {
      File dirToDelete = new File(paimonWarehouseDir);
      FileUtils.deleteQuietly(dirToDelete);
    }
    sql("DROP NAMESPACE %s", defaultNs);
  }

  @Test
  public void testBasicTableOperations() {
    // create a regular paimon table
    // Note: Paimon manages table location internally at warehouse/database.db/table_name
    String paimontb1 = "paimontb1";
    sql("CREATE TABLE %s (id INT, name STRING) USING PAIMON", paimontb1);
    sql("INSERT INTO %s VALUES (1, 'anna'), (2, 'bob')", paimontb1);
    List<Object[]> results =
        sql("SELECT id, name FROM %s WHERE id > 1 ORDER BY id DESC", paimontb1);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0)).isEqualTo(new Object[] {2, "bob"});

    // create a paimon table with partition
    String paimontb2 = "paimontb2";
    sql(
        "CREATE TABLE %s (name STRING, age INT, country STRING) USING PAIMON PARTITIONED BY (country)",
        paimontb2);
    sql(
        "INSERT INTO %s VALUES ('anna', 10, 'US'), ('james', 32, 'US'), ('yan', 16, 'CHINA')",
        paimontb2);
    results = sql("SELECT name, country FROM %s ORDER BY age", paimontb2);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0)).isEqualTo(new Object[] {"anna", "US"});
    assertThat(results.get(1)).isEqualTo(new Object[] {"yan", "CHINA"});
    assertThat(results.get(2)).isEqualTo(new Object[] {"james", "US"});

    // drop tables
    sql("DROP TABLE %s", paimontb1);
    sql("DROP TABLE %s", paimontb2);
  }

  @Test
  public void testTableUpdate() {
    // Note: UPDATE with primary key table has compatibility issues between
    // Paimon 1.0.0 and Spark 3.5's RewriteOperationForRowLineage.
    // This test demonstrates the basic flow: we test INSERT overwrite instead
    // which achieves similar functionality.
    String tableName = getTableNameWithRandomSuffix();
    sql(
        "CREATE TABLE %s (id INT, name STRING) USING PAIMON TBLPROPERTIES ('primary-key' = 'id')",
        tableName);

    // insert initial data
    sql("INSERT INTO %s VALUES (1, 'alice'), (2, 'bob')", tableName);

    // For primary key tables, inserting with same key will update the row
    sql("INSERT INTO %s VALUES (1, 'charlie')", tableName);

    List<Object[]> results = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0)).isEqualTo(new Object[] {1, "charlie"});
    assertThat(results.get(1)).isEqualTo(new Object[] {2, "bob"});

    sql("DROP TABLE %s", tableName);
  }

  @Test
  public void testTableDelete() {
    String tableName = getTableNameWithRandomSuffix();
    // Paimon requires primary key table for DELETE operations
    sql(
        "CREATE TABLE %s (id INT, name STRING) USING PAIMON TBLPROPERTIES ('primary-key' = 'id')",
        tableName);

    sql("INSERT INTO %s VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')", tableName);
    sql("DELETE FROM %s WHERE id = 2", tableName);

    List<Object[]> results = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0)).isEqualTo(new Object[] {1, "alice"});
    assertThat(results.get(1)).isEqualTo(new Object[] {3, "charlie"});

    sql("DROP TABLE %s", tableName);
  }

  @Test
  public void testTableMerge() {
    String targetTable = getTableNameWithRandomSuffix();
    String sourceTable = getTableNameWithRandomSuffix();

    // Paimon requires primary key table for MERGE operations
    sql(
        "CREATE TABLE %s (id INT, name STRING, age INT) USING PAIMON TBLPROPERTIES ('primary-key' = 'id')",
        targetTable);
    sql(
        "CREATE TABLE %s (id INT, name STRING, age INT) USING PAIMON TBLPROPERTIES ('primary-key' = 'id')",
        sourceTable);

    sql("INSERT INTO %s VALUES (1, 'alice', 20), (2, 'bob', 30)", targetTable);
    sql("INSERT INTO %s VALUES (2, 'bobby', 31), (3, 'charlie', 25)", sourceTable);

    // Merge source into target - use column names directly without table alias prefix in SET
    sql(
        "MERGE INTO %s AS t USING %s AS s ON t.id = s.id "
            + "WHEN MATCHED THEN UPDATE SET name = s.name, age = s.age "
            + "WHEN NOT MATCHED THEN INSERT *",
        targetTable, sourceTable);

    List<Object[]> results = sql("SELECT * FROM %s ORDER BY id", targetTable);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0)).isEqualTo(new Object[] {1, "alice", 20});
    assertThat(results.get(1)).isEqualTo(new Object[] {2, "bobby", 31});
    assertThat(results.get(2)).isEqualTo(new Object[] {3, "charlie", 25});

    sql("DROP TABLE %s", targetTable);
    sql("DROP TABLE %s", sourceTable);
  }

  @Test
  public void testShowTables() {
    // Paimon tables created through SparkCatalog (via CREATE TABLE ... USING PAIMON)
    // are registered in Polaris and visible via SHOW TABLES through the unified catalog view.
    // This test verifies Paimon tables appear in the table listing and can be queried correctly.
    String tableName1 = getTableNameWithRandomSuffix();
    String tableName2 = getTableNameWithRandomSuffix();

    sql("CREATE TABLE %s (id INT) USING PAIMON", tableName1);
    sql("CREATE TABLE %s (id INT) USING PAIMON", tableName2);

    // Verify Paimon tables are visible via SHOW TABLES (registered in Polaris)
    List<Object[]> tables = sql("SHOW TABLES IN %s", defaultNs);
    List<Object> tableNames =
        tables.stream().map(r -> r[1]).collect(java.util.stream.Collectors.toList());
    assertThat(tableNames).contains(tableName1, tableName2);

    // Verify tables are accessible by inserting and querying
    sql("INSERT INTO %s VALUES (1)", tableName1);
    sql("INSERT INTO %s VALUES (2)", tableName2);

    List<Object[]> results1 = sql("SELECT * FROM %s", tableName1);
    assertThat(results1.size()).isEqualTo(1);
    assertThat(results1.get(0)).isEqualTo(new Object[] {1});

    List<Object[]> results2 = sql("SELECT * FROM %s", tableName2);
    assertThat(results2.size()).isEqualTo(1);
    assertThat(results2.get(0)).isEqualTo(new Object[] {2});

    sql("DROP TABLE %s", tableName1);
    sql("DROP TABLE %s", tableName2);
  }

  @Test
  public void testDescribeTable() {
    String tableName = getTableNameWithRandomSuffix();
    sql("CREATE TABLE %s (id INT, name STRING, age INT) USING PAIMON", tableName);

    List<Object[]> results = sql("DESCRIBE TABLE %s", tableName);
    assertThat(results.size()).isGreaterThanOrEqualTo(3);

    sql("DROP TABLE %s", tableName);
  }

  @Test
  public void testAlterTableOperations() {
    String tableName = getTableNameWithRandomSuffix();
    sql("CREATE TABLE %s (id INT, name STRING) USING PAIMON", tableName);

    // ALTER TABLE ... ADD COLUMN should work with Paimon
    sql("ALTER TABLE %s ADD COLUMN age INT", tableName);
    sql("INSERT INTO %s VALUES (1, 'alice', 25)", tableName);

    List<Object[]> results = sql("SELECT * FROM %s", tableName);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0)).isEqualTo(new Object[] {1, "alice", 25});

    sql("DROP TABLE %s", tableName);
  }

  @Test
  public void testUnsupportedRenameTable() {
    // Paimon tables created through Polaris SparkCatalog do not support RENAME
    // because renameTable is not implemented for Paimon routing
    String originalName = getTableNameWithRandomSuffix();

    sql("CREATE TABLE %s (id INT, name STRING) USING PAIMON", originalName);
    sql("INSERT INTO %s VALUES (1, 'test')", originalName);

    // RENAME should fail - not supported through Polaris
    assertThatThrownBy(() -> sql("ALTER TABLE %s RENAME TO new_table", originalName))
        .isInstanceOf(UnsupportedOperationException.class);

    sql("DROP TABLE %s", originalName);
  }

  @Test
  public void testDeleteOnAppendOnlyTable() {
    // Paimon append-only tables (without primary key) behavior for DELETE
    // This documents current behavior - Paimon may allow DELETE on append-only with scan
    String tableName = getTableNameWithRandomSuffix();
    sql("CREATE TABLE %s (id INT, name STRING) USING PAIMON", tableName);
    sql("INSERT INTO %s VALUES (1, 'alice'), (2, 'bob')", tableName);

    // Note: Paimon may handle DELETE differently for append-only tables
    // This test documents the current behavior
    sql("DELETE FROM %s WHERE id = 1", tableName);

    List<Object[]> results = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0)).isEqualTo(new Object[] {2, "bob"});

    sql("DROP TABLE %s", tableName);
  }
}
