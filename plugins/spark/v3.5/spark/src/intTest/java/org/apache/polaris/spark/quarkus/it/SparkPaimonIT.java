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

import java.util.List;
import org.apache.polaris.service.it.ext.SparkSessionBuilder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SparkPaimonIT extends SparkIntegrationBase {
  private String namespace;

  @Override
  protected SparkSession buildSparkSession() {
    return SparkSessionBuilder.buildWithTestDefaults()
        .withExtensions(
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                + "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
        .withWarehouse(warehouseDir)
        .addCatalog(catalogName, "org.apache.polaris.spark.SparkCatalog", endpoints, sparkToken)
        .withConfig(
            String.format("spark.sql.catalog.%s.paimon-warehouse", catalogName),
            warehouseDir.resolve("paimon").toString())
        .getOrCreate();
  }

  @BeforeEach
  public void createNamespace() {
    spark.sparkContext().setLogLevel("WARN");
    namespace = generateName("paimon");
    sql("CREATE NAMESPACE %s", namespace);
    sql("USE NAMESPACE %s", namespace);
  }

  @Test
  public void testBasicTableOperations() {
    String table = generateName("paimontb");
    sql(
        "CREATE TABLE %s (id INT, name STRING, country STRING) USING PAIMON "
            + "PARTITIONED BY (country)",
        table);
    sql("INSERT INTO %s VALUES (1, 'anna', 'US'), (2, 'bob', 'UK')", table);

    assertThat(sql("SELECT id, name, country FROM %s ORDER BY id", table))
        .containsExactly(new Object[] {1, "anna", "US"}, new Object[] {2, "bob", "UK"});
    assertThat(sql("SHOW TABLES")).containsExactly(new Object[] {namespace, table, false});

    sql("DROP TABLE %s", table);
    assertThat(sql("SHOW TABLES")).isEmpty();
  }

  @Test
  public void testPrimaryKeyUpdatesAndDeletes() {
    String table = generateName("paimontb");
    sql(
        "CREATE TABLE %s (id INT, name STRING) USING PAIMON "
            + "TBLPROPERTIES ('primary-key' = 'id')",
        table);
    sql("INSERT INTO %s VALUES (1, 'alice'), (2, 'bob')", table);
    sql("INSERT INTO %s VALUES (1, 'anna')", table);

    assertThat(sql("SELECT * FROM %s ORDER BY id", table))
        .containsExactly(new Object[] {1, "anna"}, new Object[] {2, "bob"});

    sql("DELETE FROM %s WHERE id = 2", table);
    assertThat(sql("SELECT * FROM %s", table)).containsExactly(new Object[] {1, "anna"});

    sql("DROP TABLE %s", table);
  }

  @Test
  public void testAlterTable() {
    String table = generateName("paimontb");
    sql("CREATE TABLE %s (id INT, name STRING) USING PAIMON", table);
    sql("INSERT INTO %s VALUES (1, 'anna'), (2, 'bob')", table);

    sql("ALTER TABLE %s ADD COLUMNS (age INT)", table);
    sql("INSERT INTO %s VALUES (3, 'john', 20)", table);

    List<Object[]> rows = sql("SELECT id, name, age FROM %s ORDER BY id", table);
    assertThat(rows)
        .containsExactly(
            new Object[] {1, "anna", null},
            new Object[] {2, "bob", null},
            new Object[] {3, "john", 20});

    sql("DROP TABLE %s", table);
  }

  @Test
  public void testUnsupportedAlterTableOperations() {
    String table = generateName("paimontb");
    sql("CREATE TABLE %s (id INT, name STRING) USING PAIMON", table);

    assertThatThrownBy(() -> sql("ALTER TABLE %s RENAME TO new_paimon", table))
        .isInstanceOf(UnsupportedOperationException.class);

    sql("DROP TABLE %s", table);
  }
}
