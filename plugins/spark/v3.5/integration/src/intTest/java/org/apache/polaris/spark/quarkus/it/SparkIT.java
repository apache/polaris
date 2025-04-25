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
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

@QuarkusIntegrationTest
public class SparkIT extends SparkIntegrationBase {
  private String defaultNs;

  @BeforeEach
  public void createDefaultResources() {
    defaultNs = generateName("ns");
    // create a default namespace
    sql("CREATE NAMESPACE %s", defaultNs);
    sql("USE NAMESPACE %s", defaultNs);
  }

  @AfterEach
  public void cleanupDeltaData() {
    sql("DROP NAMESPACE %s", defaultNs);
  }

  @Test
  public void testNamespaces() {
    String l1ns2 = generateName("ns2");
    // create another namespace
    sql("CREATE NAMESPACE %s", l1ns2);

    List<Object[]> namespaces = sql("SHOW NAMESPACES");
    assertThat(namespaces.size()).isEqualTo(2);
    assertThat(namespaces).contains(new Object[] {defaultNs}, new Object[] {l1ns2});

    String l2ns = "l2ns";
    // create a nested namespace
    sql("CREATE NAMESPACE %s.%s", defaultNs, l2ns);
    // spark show namespace only shows
    namespaces = sql("SHOW NAMESPACES");
    assertThat(namespaces.size()).isEqualTo(2);

    // can not drop l1NS before the nested namespace is dropped
    assertThatThrownBy(() -> sql("DROP NAMESPACE %s", defaultNs))
        .hasMessageContaining(String.format("Namespace %s is not empty", defaultNs));
    sql("DROP NAMESPACE %s.%s", defaultNs, l2ns);

    sql("DROP NAMESPACE %s", l1ns2);

    // the default namespace will be cleaned during test cleanup
    namespaces = sql("SHOW NAMESPACES");
    assertThat(namespaces.size()).isEqualTo(1);
  }

  @Test
  public void testCreatDropView() {
    // create two views under the namespace
    String view1Name = "testView1";
    String view2Name = "testView2";
    sql("CREATE VIEW %s AS SELECT 1 AS id", view1Name);
    sql("CREATE VIEW %s AS SELECT 10 AS id", view2Name);
    List<Object[]> views = sql("SHOW VIEWS");
    assertThat(views.size()).isEqualTo(2);
    assertThat(views).contains(new Object[] {defaultNs, view1Name, false});
    assertThat(views).contains(new Object[] {defaultNs, view2Name, false});

    // drop the views
    sql("DROP VIEW %s", view1Name);
    views = sql("SHOW VIEWS");
    assertThat(views.size()).isEqualTo(1);
    assertThat(views).contains(new Object[] {defaultNs, view2Name, false});

    sql("DROP VIEW %s", view2Name);
    views = sql("SHOW VIEWS");
    assertThat(views.size()).isEqualTo(0);
  }

  @Test
  public void testRenameView() {
    String viewName = "originalView";
    String renamedView = "renamedView";
    sql("CREATE VIEW %s AS SELECT 1 AS id", viewName);
    List<Object[]> views = sql("SHOW VIEWS");
    assertThat(views.size()).isEqualTo(1);
    assertThat(views).contains(new Object[] {defaultNs, viewName, false});

    sql("ALTER VIEW %s RENAME TO %s", viewName, renamedView);
    views = sql("SHOW VIEWS");
    assertThat(views.size()).isEqualTo(1);
    assertThat(views).contains(new Object[] {defaultNs, renamedView, false});

    // drop the views
    sql("DROP VIEW %s", renamedView);
  }

  @Test
  public void testRenameIcebergTable() {
    String icebergTable = "iceberg_table";
    sql("CREATE TABLE %s (col1 int, col2 string)", icebergTable);
    sql("INSERT INTO %s VALUES (5, 'a'), (2, 'b')", icebergTable);

    List<Object[]> tables = sql("SHOW TABLES");
    assertThat(tables.size()).isEqualTo(1);
    assertThat(tables).contains(new Object[] {defaultNs, icebergTable, false});

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

    // clean up the table and namespace
    sql("DROP TABLE %s", newIcebergTable);
  }

  @Test
  public void testMixedTableAndViews(@TempDir Path tempDir) {
    // create one iceberg table, iceberg view and one delta table
    String icebergTable = "icebergtb";
    sql("CREATE TABLE %s (col1 int, col2 String)", icebergTable);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", icebergTable);

    String viewName = "icebergview";
    sql("CREATE VIEW %s AS SELECT col1 + 2 AS col1, col2 FROM %s", viewName, icebergTable);

    String deltaTable = "deltatb";
    String deltaDir =
        IntegrationTestsHelper.getTemporaryDirectory(tempDir).resolve(defaultNs).getPath();
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
            new Object[] {defaultNs, icebergTable, false},
            new Object[] {defaultNs, deltaTable, false});

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
    assertThat(views).contains(new Object[] {defaultNs, viewName, false});

    // drop views and tables
    sql("DROP TABLE %s", icebergTable);
    sql("DROP TABLE %s", deltaTable);
    sql("DROP VIEW %s", viewName);

    // clean up delta directory
    File dirToDelete = new File(deltaDir);
    FileUtils.deleteQuietly(dirToDelete);
  }
}
