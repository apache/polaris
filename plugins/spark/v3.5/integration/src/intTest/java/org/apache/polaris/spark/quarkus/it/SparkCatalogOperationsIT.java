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

import com.google.common.collect.Maps;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.util.Arrays;
import java.util.Map;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.spark.SparkCatalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewChange;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
public class SparkCatalogOperationsIT extends SparkIntegrationBase {
  private static StructType schema = new StructType().add("id", "long").add("name", "string");

  @Test
  void testNamespaceOperations() throws Exception {
    SparkCatalog catalog = loadSparkCatalog();

    String[][] lv1ns = new String[][] {{"l1ns1"}, {"l1ns2"}};
    String[][] lv2ns1 = new String[][] {{"l1ns1", "l2ns1"}, {"l1ns1", "l2ns2"}};
    String[][] lv2ns2 = new String[][] {{"l1ns2", "l2ns3"}};

    // create the namespaces
    for (String[] namespace : lv1ns) {
      catalog.createNamespace(namespace, Maps.newHashMap());
    }
    for (String[] namespace : lv2ns1) {
      catalog.createNamespace(namespace, Maps.newHashMap());
    }
    for (String[] namespace : lv2ns2) {
      catalog.createNamespace(namespace, Maps.newHashMap());
    }

    // list namespaces under root
    String[][] lv1nsResult = catalog.listNamespaces();
    assertThat(lv1nsResult.length).isEqualTo(lv1ns.length);
    for (String[] namespace : lv1ns) {
      assertThat(Arrays.asList(lv1nsResult)).contains(namespace);
    }
    // list namespace under l1ns1
    String[][] lv2ns1Result = catalog.listNamespaces(lv1ns[0]);
    assertThat(lv2ns1Result.length).isEqualTo(lv2ns1.length);
    for (String[] namespace : lv2ns1) {
      assertThat(Arrays.asList(lv2ns1Result)).contains(namespace);
    }
    // list namespace under l1ns2
    String[][] lv2ns2Result = catalog.listNamespaces(lv1ns[1]);
    assertThat(lv2ns2Result.length).isEqualTo(lv2ns2.length);
    for (String[] namespace : lv2ns2) {
      assertThat(Arrays.asList(lv2ns2Result)).contains(namespace);
    }
    // no namespace under l1ns2.l2ns3
    assertThat(catalog.listNamespaces(lv2ns2[0]).length).isEqualTo(0);

    // drop the nested namespace under lv1ns[1]
    catalog.dropNamespace(lv2ns2[0], true);
    assertThat(catalog.listNamespaces(lv1ns[1]).length).isEqualTo(0);
    catalog.dropNamespace(lv1ns[1], true);
    assertThatThrownBy(() -> catalog.listNamespaces(lv1ns[1]))
        .isInstanceOf(NoSuchNamespaceException.class);

    // directly drop lv1ns[0] should fail
    assertThatThrownBy(() -> catalog.dropNamespace(lv1ns[0], true))
        .isInstanceOf(BadRequestException.class);
    for (String[] namespace : lv2ns1) {
      catalog.dropNamespace(namespace, true);
    }
    catalog.dropNamespace(lv1ns[0], true);

    // no more namespace available
    assertThat(catalog.listNamespaces().length).isEqualTo(0);
  }

  @Test
  void testAlterNamespace() throws Exception {
    SparkCatalog catalog = loadSparkCatalog();
    String[] namespace = new String[] {"ns1"};
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put("owner", "user1");

    catalog.createNamespace(namespace, metadata);
    assertThat(catalog.loadNamespaceMetadata(namespace)).contains(Map.entry("owner", "user1"));

    catalog.alterNamespace(namespace, NamespaceChange.setProperty("owner", "new-user"));
    assertThat(catalog.loadNamespaceMetadata(namespace)).contains(Map.entry("owner", "new-user"));

    // drop the namespace
    catalog.dropNamespace(namespace, true);
  }

  @Test
  void testBasicViewOperations() throws Exception {
    SparkCatalog catalog = loadSparkCatalog();
    String[] namespace = new String[] {"ns"};
    catalog.createNamespace(namespace, Maps.newHashMap());

    Identifier viewIdentifier = Identifier.of(namespace, "test-view");
    String viewSql = "select id from test-table where id < 3";
    catalog.createView(
        viewIdentifier,
        viewSql,
        catalogName,
        namespace,
        schema,
        new String[0],
        new String[0],
        new String[0],
        Maps.newHashMap());

    // load the view
    View view = catalog.loadView(viewIdentifier);
    assertThat(view.query()).isEqualTo(viewSql);
    assertThat(view.schema()).isEqualTo(schema);

    // alter the view properties
    catalog.alterView(viewIdentifier, ViewChange.setProperty("owner", "user1"));
    view = catalog.loadView(viewIdentifier);
    assertThat(view.properties()).contains(Map.entry("owner", "user1"));

    // rename the view
    Identifier newIdentifier = Identifier.of(namespace, "new-view");
    catalog.renameView(viewIdentifier, newIdentifier);
    assertThatThrownBy(() -> catalog.loadView(viewIdentifier))
        .isInstanceOf(NoSuchViewException.class);
    view = catalog.loadView(newIdentifier);
    assertThat(view.query()).isEqualTo(viewSql);
    assertThat(view.schema()).isEqualTo(schema);

    // replace the view
    String newSql = "select id from test-table where id == 3";
    Map<String, String> properties = Maps.newHashMap();
    properties.put("owner", "test-user");
    catalog.replaceView(
        newIdentifier,
        newSql,
        catalogName,
        namespace,
        schema,
        new String[0],
        new String[0],
        new String[0],
        properties);
    view = catalog.loadView(newIdentifier);
    assertThat(view.query()).isEqualTo(newSql);
    assertThat(view.properties()).contains(Map.entry("owner", "test-user"));

    // drop the view
    catalog.dropView(newIdentifier);
    assertThatThrownBy(() -> catalog.loadView(newIdentifier))
        .isInstanceOf(NoSuchViewException.class);
  }

  @Test
  void testListViews() throws Exception {
    SparkCatalog catalog = loadSparkCatalog();

    String[] l1ns = new String[] {"ns"};
    catalog.createNamespace(l1ns, Maps.newHashMap());

    // create a new namespace under the default NS
    String[] l2ns = new String[] {"ns", "nsl2"};
    catalog.createNamespace(l2ns, Maps.newHashMap());
    // create one view under l1
    String view1Name = "test-view1";
    String view1SQL = "select id from test-table where id >= 3";
    catalog.createView(
        Identifier.of(l1ns, view1Name),
        view1SQL,
        catalogName,
        l1ns,
        schema,
        new String[0],
        new String[0],
        new String[0],
        Maps.newHashMap());
    // create two views under the l2 namespace
    String[] nsl2ViewNames = new String[] {"test-view2", "test-view3"};
    String[] nsl2ViewSQLs =
        new String[] {
          "select id from test-table where id == 3", "select id from test-table where id < 3"
        };
    for (int i = 0; i < nsl2ViewNames.length; i++) {
      catalog.createView(
          Identifier.of(l2ns, nsl2ViewNames[i]),
          nsl2ViewSQLs[i],
          catalogName,
          l2ns,
          schema,
          new String[0],
          new String[0],
          new String[0],
          Maps.newHashMap());
    }
    // list views under l1ns
    Identifier[] l1Views = catalog.listViews(l1ns);
    assertThat(l1Views.length).isEqualTo(1);
    assertThat(l1Views[0].name()).isEqualTo(view1Name);

    // list views under l2ns
    Identifier[] l2Views = catalog.listViews(l2ns);
    assertThat(l2Views.length).isEqualTo(nsl2ViewSQLs.length);
    for (String name : nsl2ViewNames) {
      assertThat(Arrays.asList(l2Views)).contains(Identifier.of(l2ns, name));
    }

    // drop namespace fails since there are views under it
    assertThatThrownBy(() -> catalog.dropNamespace(l2ns, true))
        .isInstanceOf(BadRequestException.class);
    // drop the views
    for (String name : nsl2ViewNames) {
      catalog.dropView(Identifier.of(l2ns, name));
    }
    catalog.dropNamespace(l2ns, true);
    catalog.dropView(Identifier.of(l1ns, view1Name));
    catalog.dropNamespace(l1ns, true);
  }
}
