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
import java.util.List;
import org.junit.jupiter.api.Test;

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
}
