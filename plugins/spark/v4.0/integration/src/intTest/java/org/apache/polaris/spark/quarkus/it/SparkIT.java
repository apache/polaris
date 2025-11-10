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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@QuarkusIntegrationTest
public class SparkIT extends SparkIntegrationBase {
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
    String namespace = generateName("ns");
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

    sql("DROP NAMESPACE %s", namespace);
  }

  @Test
  public void renameIcebergViewAndTable() {
    String namespace = generateName("ns");
    sql("CREATE NAMESPACE %s", namespace);
    sql("USE %s", namespace);

    // create one view and one table
    String viewName = "originalView";
    sql("CREATE VIEW %s AS SELECT 1 AS id", viewName);

    String icebergTable = "iceberg_table";
    sql("CREATE TABLE %s (col1 int, col2 string)", icebergTable);

    // verify view and table is showing correctly
    List<Object[]> views = sql("SHOW VIEWS");
    assertThat(views.size()).isEqualTo(1);
    assertThat(views).contains(new Object[] {namespace, viewName, false});

    List<Object[]> tables = sql("SHOW TABLES");
    assertThat(tables.size()).isEqualTo(1);
    assertThat(tables).contains(new Object[] {namespace, icebergTable, false});

    // rename the view
    String renamedView = "renamedView";
    sql("ALTER VIEW %s RENAME TO %s", viewName, renamedView);
    views = sql("SHOW VIEWS");
    assertThat(views.size()).isEqualTo(1);
    assertThat(views).contains(new Object[] {namespace, renamedView, false});

    // rename the table
    String newIcebergTable = "iceberg_table_new";
    sql("ALTER TABLE %s RENAME TO %s", icebergTable, newIcebergTable);
    tables = sql("SHOW TABLES");
    assertThat(tables.size()).isEqualTo(1);
    assertThat(tables).contains(new Object[] {namespace, newIcebergTable, false});

    // clean up the resources
    sql("DROP VIEW %s", renamedView);
    sql("DROP TABLE %s", newIcebergTable);
    sql("DROP NAMESPACE %s", namespace);
  }

  @Test
  public void testMixedTableAndViews(@TempDir Path tempDir) {
    String namespace = generateName("ns");
    sql("CREATE NAMESPACE %s", namespace);
    sql("USE %s", namespace);

    // create one iceberg table, iceberg view and one delta table
    String icebergTable = "icebergtb";
    sql("CREATE TABLE %s (col1 int, col2 String)", icebergTable);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", icebergTable);

    String viewName = "icebergview";
    sql("CREATE VIEW %s AS SELECT col1 + 2 AS col1, col2 FROM %s", viewName, icebergTable);

    String deltaTable = "deltatb";
    String deltaDir =
        IntegrationTestsHelper.getTemporaryDirectory(tempDir).resolve(namespace).getPath();
    sql(
        "CREATE TABLE %s (col1 int, col2 int) using delta location '%s/%s'",
        deltaTable, deltaDir, deltaTable);
    sql("INSERT INTO %s VALUES (1, 3), (2, 5), (11, 20)", deltaTable);
    // join the iceberg and delta table
    List<Object[]> joinResult =
        sql(
            "SELECT icebergtb.col1 as id, icebergtb.col2 as str_col, deltatb.col2 as int_col from icebergtb inner join deltatb on icebergtb.col1 = deltatb.col1 order by id");
    assertThat(joinResult.get(0)).isEqualTo(new Object[] {1, "a", 3});
    assertThat(joinResult.get(1)).isEqualTo(new Object[] {2, "b", 5});

    // show tables shows all tables
    List<Object[]> tables = sql("SHOW TABLES");
    assertThat(tables.size()).isEqualTo(2);
    assertThat(tables)
        .contains(
            new Object[] {namespace, icebergTable, false},
            new Object[] {namespace, deltaTable, false});

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
    assertThat(views).contains(new Object[] {namespace, viewName, false});

    // drop views and tables
    sql("DROP TABLE %s", icebergTable);
    sql("DROP TABLE %s", deltaTable);
    sql("DROP VIEW %s", viewName);
    sql("DROP NAMESPACE %s", namespace);

    // clean up delta directory
    File dirToDelete = new File(deltaDir);
    FileUtils.deleteQuietly(dirToDelete);
  }
}
