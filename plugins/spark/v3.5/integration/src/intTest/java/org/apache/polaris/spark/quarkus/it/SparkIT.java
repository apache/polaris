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
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@QuarkusIntegrationTest
public class SparkIT extends SparkIntegrationBase {
  private String tableRootDir;

  @BeforeEach
  public void createDefaultResources(@TempDir Path tempDir) {
    tableRootDir =
        IntegrationTestsHelper.getTemporaryDirectory(tempDir).resolve("tables").getPath();
  }

  @AfterEach
  public void cleanupDeltaData() {
    File dirToDelete = new File(tableRootDir);
    deleteDirectory(dirToDelete);
  }

  @Test
  public void testNamespaces() {
    List<Object[]> namespaces = sql("SHOW NAMESPACES");
    assertThat(namespaces.size()).isEqualTo(0);

    String[] l1NS = new String[] {"l1ns1", "l1ns2"};
    for (String ns : l1NS) {
      sql("CREATE NAMESPACE %s", ns);
    }
    namespaces = sql("SHOW NAMESPACES");
    assertThat(namespaces.size()).isEqualTo(2);
    for (String ns : l1NS) {
      assertThat(namespaces).contains(new Object[] {ns});
    }
    String l2ns = "l2ns";
    // create a nested namespace
    sql("CREATE NAMESPACE %s.%s", l1NS[0], l2ns);
    // spark show namespace only shows
    namespaces = sql("SHOW NAMESPACES");
    assertThat(namespaces.size()).isEqualTo(2);

    // can not drop l1NS before the nested namespace is dropped
    assertThatThrownBy(() -> sql("DROP NAMESPACE %s", l1NS[0]))
        .hasMessageContaining(String.format("Namespace %s is not empty", l1NS[0]));
    sql("DROP NAMESPACE %s.%s", l1NS[0], l2ns);

    for (String ns : l1NS) {
      sql("DROP NAMESPACE %s", ns);
    }

    // no namespace available after all drop
    namespaces = sql("SHOW NAMESPACES");
    assertThat(namespaces.size()).isEqualTo(0);
  }

  @Test
  public void testCreatDropView() {
    String namespace = "ns";
    // create namespace ns
    sql("CREATE NAMESPACE %s", namespace);
    sql("USE %s", namespace);

    // create two views under the namespace
    String view1Name = "testView1";
    String view2Name = "testView2";
    sql("CREATE VIEW %s AS SELECT 1 AS id", view1Name);
    sql("CREATE VIEW %s AS SELECT 10 AS id", view2Name);
    List<Object[]> views = sql("SHOW VIEWS");
    assertThat(views.size()).isEqualTo(2);
    assertThat(views).contains(new Object[] {namespace, view1Name, false});
    assertThat(views).contains(new Object[] {namespace, view2Name, false});

    // drop the views
    sql("DROP VIEW %s", view1Name);
    views = sql("SHOW VIEWS");
    assertThat(views.size()).isEqualTo(1);
    assertThat(views).contains(new Object[] {namespace, view2Name, false});

    sql("DROP VIEW %s", view2Name);
    views = sql("SHOW VIEWS");
    assertThat(views.size()).isEqualTo(0);
  }

  @Test
  public void renameView() {
    sql("CREATE NAMESPACE ns");
    sql("USE ns");

    String viewName = "originalView";
    String renamedView = "renamedView";
    sql("CREATE VIEW %s AS SELECT 1 AS id", viewName);
    List<Object[]> views = sql("SHOW VIEWS");
    assertThat(views.size()).isEqualTo(1);
    assertThat(views).contains(new Object[] {"ns", viewName, false});

    sql("ALTER VIEW %s RENAME TO %s", viewName, renamedView);
    views = sql("SHOW VIEWS");
    assertThat(views.size()).isEqualTo(1);
    assertThat(views).contains(new Object[] {"ns", renamedView, false});
  }

  @Test
  public void testTableOperations() {
    sql("CREATE NAMESPACE ns");
    sql("USE ns");

    String icebergTable1 = "icebergtb1";
    String icebergTable2 = "icebergtb2";
    String deltaTable = "deltatb";
    String deltaDir = String.format("%s/ns", tableRootDir);
    // create two iceberg table and one delta table
    sql("CREATE TABLE %s (col1 int, col2 string)", icebergTable1);
    sql("INSERT INTO %s VALUES (5, 'a'), (2, 'b')", icebergTable1);
    sql("CREATE TABLE %s (col1 int) using iceberg", icebergTable2);
    sql("INSERT INTO %s VALUES (111), (235), (456)", icebergTable2);
    sql(
        "CREATE TABLE %s (col1 int, col2 int) using delta location '%s/%s'",
        deltaTable, deltaDir, deltaTable);

    // show iceberg tables
    List<Object[]> results = sql("SELECT * FROM %s ORDER BY col1", icebergTable1);
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0)).isEqualTo(new Object[] {2, "b"});
    assertThat(results.get(1)).isEqualTo(new Object[] {5, "a"});

    results = sql("SELECT * FROM %s ORDER BY col1", icebergTable2);
    assertThat(results.size()).isEqualTo(3);

    // show tables shows all tables
    List<Object[]> tables = sql("SHOW TABLES");
    assertThat(tables.size()).isEqualTo(3);
    assertThat(tables)
        .contains(
            new Object[] {"ns", icebergTable1, false},
            new Object[] {"ns", icebergTable2, false},
            new Object[] {"ns", deltaTable, false});

    sql("DROP TABLE %s", icebergTable1);
    tables = sql("SHOW TABLES");
    assertThat(tables.size()).isEqualTo(2);
    assertThat(tables)
        .contains(
            new Object[] {"ns", icebergTable2, false}, new Object[] {"ns", deltaTable, false});

    sql("DROP TABLE %s", icebergTable2);
    sql("DROP TABLE %s", deltaTable);
    tables = sql("SHOW TABLES");
    assertThat(tables.size()).isEqualTo(0);
  }

  @Test
  public void testRenameIcebergTable() {
    sql("CREATE NAMESPACE ns");
    sql("USE ns");

    String icebergTable = "iceberg_table";
    sql("CREATE TABLE %s (col1 int, col2 string)", icebergTable);
    sql("INSERT INTO %s VALUES (5, 'a'), (2, 'b')", icebergTable);

    List<Object[]> tables = sql("SHOW TABLES");
    assertThat(tables.size()).isEqualTo(1);
    assertThat(tables).contains(new Object[] {"ns", icebergTable, false});

    String newIcebergTable = "iceberg_table_new";
    sql("ALTER TABLE %s RENAME TO %s", icebergTable, newIcebergTable);
    // verify the table can not be queried using old name
    assertThatThrownBy(() -> sql("SELECT * FROM %s", icebergTable))
        .isInstanceOf(AnalysisException.class);
    // verify the table can be queried with new name
    List<Object[]> results = sql("SELECT * FROM %s ORDER BY col1", newIcebergTable);
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0)).isEqualTo(new Object[] {2, "b"});
    assertThat(results.get(1)).isEqualTo(new Object[] {5, "a"});
  }

  @Test
  public void testMixedTableAndViews() {
    sql("CREATE NAMESPACE mix_ns");
    sql("USE mix_ns");

    // create one iceberg table, iceberg view and one delta table
    String icebergTable = "icebergtb";
    sql("CREATE TABLE %s (col1 int, col2 String)", icebergTable);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", icebergTable);

    String viewName = "icebergview";
    sql("CREATE VIEW %s AS SELECT col1 + 2 AS col1, col2 FROM %s", viewName, icebergTable);

    String deltaTable = "deltatb";
    String deltaDir = String.format("%s/mix_ns", tableRootDir);
    sql(
        "CREATE TABLE %s (col1 int, col2 int) using delta location '%s/%s'",
        deltaTable, deltaDir, deltaTable);

    // show tables shows all tables
    List<Object[]> tables = sql("SHOW TABLES");
    assertThat(tables.size()).isEqualTo(2);
    assertThat(tables)
        .contains(
            new Object[] {"mix_ns", icebergTable, false},
            new Object[] {"mix_ns", deltaTable, false});

    // verify the table and view content
    List<Object[]> results = sql("SELECT * FROM %s ORDER BY col1", icebergTable);
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0)).isEqualTo(new Object[] {1, "a"});
    assertThat(results.get(1)).isEqualTo(new Object[] {2, "b"});

    // verify the table and view content
    results = sql("SELECT * FROM %s ORDER BY col1", viewName);
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0)).isEqualTo(new Object[] {3, "a"});
    assertThat(results.get(1)).isEqualTo(new Object[] {4, "b"});

    List<Object[]> views = sql("SHOW VIEWS");
    assertThat(views.size()).isEqualTo(1);
    assertThat(views).contains(new Object[] {"mix_ns", viewName, false});
  }
}
