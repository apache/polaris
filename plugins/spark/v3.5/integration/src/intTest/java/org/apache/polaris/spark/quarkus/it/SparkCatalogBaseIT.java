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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.util.Arrays;
import java.util.Map;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.spark.SupportsReplaceView;
import org.apache.polaris.spark.utils.PolarisCatalogUtils;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This integration directly performs operations using the SparkCatalog instance, instead of going
 * through Spark SQL interface. This provides a more direct testing capability against the Polaris
 * SparkCatalog operations, some operations like listNamespaces under a namespace can not be
 * triggered through a SQL interface directly with Spark.
 */
@QuarkusIntegrationTest
public abstract class SparkCatalogBaseIT extends SparkIntegrationBase {
  protected static StructType schema = new StructType().add("id", "long").add("name", "string");
  protected StagingTableCatalog tableCatalog = null;
  protected SupportsNamespaces namespaceCatalog = null;
  protected ViewCatalog viewCatalog = null;
  protected SupportsReplaceView replaceViewCatalog = null;

  @BeforeEach
  protected void loadCatalogs() {
    Preconditions.checkArgument(spark != null, "No active spark found");
    Preconditions.checkArgument(catalogName != null, "No catalogName found");
    CatalogPlugin catalogPlugin = spark.sessionState().catalogManager().catalog(catalogName);
    tableCatalog = (StagingTableCatalog) catalogPlugin;
    namespaceCatalog = (SupportsNamespaces) catalogPlugin;
    viewCatalog = (ViewCatalog) catalogPlugin;
    replaceViewCatalog = (SupportsReplaceView) catalogPlugin;
  }

  @Test
  void testNamespaceOperations() throws Exception {
    String l1ns1 = generateName("ns1");
    String l1ns2 = generateName("ns2");
    String[][] lv1ns = new String[][] {{l1ns1}, {l1ns2}};
    String[][] lv2ns1 = new String[][] {{l1ns1, "l2ns1"}, {l1ns1, "l2ns2"}};
    String[][] lv2ns2 = new String[][] {{l1ns2, "l2ns3"}};

    // create the namespaces
    for (String[] namespace : lv1ns) {
      namespaceCatalog.createNamespace(namespace, Maps.newHashMap());
    }
    for (String[] namespace : lv2ns1) {
      namespaceCatalog.createNamespace(namespace, Maps.newHashMap());
    }
    for (String[] namespace : lv2ns2) {
      namespaceCatalog.createNamespace(namespace, Maps.newHashMap());
    }

    // list namespaces under root
    String[][] lv1nsResult = namespaceCatalog.listNamespaces();
    assertThat(lv1nsResult.length).isEqualTo(lv1ns.length);
    for (String[] namespace : lv1ns) {
      assertThat(Arrays.asList(lv1nsResult)).contains(namespace);
    }
    // list namespace under l1ns1
    String[][] lv2ns1Result = namespaceCatalog.listNamespaces(lv1ns[0]);
    assertThat(lv2ns1Result.length).isEqualTo(lv2ns1.length);
    for (String[] namespace : lv2ns1) {
      assertThat(Arrays.asList(lv2ns1Result)).contains(namespace);
    }
    // list namespace under l1ns2
    String[][] lv2ns2Result = namespaceCatalog.listNamespaces(lv1ns[1]);
    assertThat(lv2ns2Result.length).isEqualTo(lv2ns2.length);
    for (String[] namespace : lv2ns2) {
      assertThat(Arrays.asList(lv2ns2Result)).contains(namespace);
    }
    // no namespace under l1ns2.l2ns3
    assertThat(namespaceCatalog.listNamespaces(lv2ns2[0]).length).isEqualTo(0);

    // drop the nested namespace under lv1ns[1]
    namespaceCatalog.dropNamespace(lv2ns2[0], true);
    assertThat(namespaceCatalog.listNamespaces(lv1ns[1]).length).isEqualTo(0);
    namespaceCatalog.dropNamespace(lv1ns[1], true);
    assertThatThrownBy(() -> namespaceCatalog.listNamespaces(lv1ns[1]))
        .isInstanceOf(NoSuchNamespaceException.class);

    // directly drop lv1ns[0] should fail
    assertThatThrownBy(() -> namespaceCatalog.dropNamespace(lv1ns[0], true))
        .isInstanceOf(BadRequestException.class);
    for (String[] namespace : lv2ns1) {
      namespaceCatalog.dropNamespace(namespace, true);
    }
    namespaceCatalog.dropNamespace(lv1ns[0], true);

    // no more namespace available
    assertThat(namespaceCatalog.listNamespaces().length).isEqualTo(0);
  }

  @Test
  void testAlterNamespace() throws Exception {
    String[] namespace = new String[] {generateName("ns")};
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put("owner", "user1");

    namespaceCatalog.createNamespace(namespace, metadata);
    assertThat(namespaceCatalog.loadNamespaceMetadata(namespace))
        .contains(Map.entry("owner", "user1"));

    namespaceCatalog.alterNamespace(namespace, NamespaceChange.setProperty("owner", "new-user"));
    assertThat(namespaceCatalog.loadNamespaceMetadata(namespace))
        .contains(Map.entry("owner", "new-user"));

    // drop the namespace
    namespaceCatalog.dropNamespace(namespace, true);
  }

  @Test
  void testBasicViewOperations() throws Exception {
    String[] namespace = new String[] {generateName("ns")};
    namespaceCatalog.createNamespace(namespace, Maps.newHashMap());

    Identifier viewIdentifier = Identifier.of(namespace, "test-view");
    String viewSql = "select id from test-table where id < 3";
    viewCatalog.createView(
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
    View view = viewCatalog.loadView(viewIdentifier);
    assertThat(view.query()).isEqualTo(viewSql);
    assertThat(view.schema()).isEqualTo(schema);

    // alter the view properties
    viewCatalog.alterView(viewIdentifier, ViewChange.setProperty("owner", "user1"));
    view = viewCatalog.loadView(viewIdentifier);
    assertThat(view.properties()).contains(Map.entry("owner", "user1"));

    // rename the view
    Identifier newIdentifier = Identifier.of(namespace, "new-view");
    viewCatalog.renameView(viewIdentifier, newIdentifier);
    assertThatThrownBy(() -> viewCatalog.loadView(viewIdentifier))
        .isInstanceOf(NoSuchViewException.class);
    view = viewCatalog.loadView(newIdentifier);
    assertThat(view.query()).isEqualTo(viewSql);
    assertThat(view.schema()).isEqualTo(schema);

    // replace the view
    String newSql = "select id from test-table where id == 3";
    Map<String, String> properties = Maps.newHashMap();
    properties.put("owner", "test-user");
    replaceViewCatalog.replaceView(
        newIdentifier,
        newSql,
        catalogName,
        namespace,
        schema,
        new String[0],
        new String[0],
        new String[0],
        properties);
    view = viewCatalog.loadView(newIdentifier);
    assertThat(view.query()).isEqualTo(newSql);
    assertThat(view.properties()).contains(Map.entry("owner", "test-user"));

    // drop the view
    viewCatalog.dropView(newIdentifier);
    assertThatThrownBy(() -> viewCatalog.loadView(newIdentifier))
        .isInstanceOf(NoSuchViewException.class);

    // drop the namespace
    namespaceCatalog.dropNamespace(namespace, true);
  }

  @Test
  void testIcebergTableViewMix() throws Exception {
    // initiate two namespaces with nesting
    String namespace = generateName("ns");
    String[] l1ns = new String[] {namespace};
    namespaceCatalog.createNamespace(l1ns, Maps.newHashMap());
    // create a new namespace under the ns
    String[] l2ns = new String[] {namespace, "nsl2"};
    namespaceCatalog.createNamespace(l2ns, Maps.newHashMap());

    StructType iceberg_schema = new StructType().add("id", "int").add("people", "string");

    // create two iceberg tables under l1ns
    Identifier l1tb1 = Identifier.of(l1ns, "iceberg_table1");
    tableCatalog.createTable(l1tb1, iceberg_schema, new Transform[0], Maps.newHashMap());

    Identifier l1tb2 = Identifier.of(l1ns, "iceberg_table2");
    Map<String, String> icebergProperties = Maps.newHashMap();
    icebergProperties.put(PolarisCatalogUtils.TABLE_PROVIDER_KEY, "iceberg");
    tableCatalog.createTable(l1tb2, iceberg_schema, new Transform[0], icebergProperties);

    // create one iceberg view under ns
    Identifier l1view = Identifier.of(l1ns, "test_view1");
    String view1SQL = "select id from iceberg_table1 where id > 10";
    viewCatalog.createView(
        l1view,
        view1SQL,
        catalogName,
        l1ns,
        iceberg_schema,
        new String[0],
        new String[0],
        new String[0],
        Maps.newHashMap());

    // create one iceberg table under the nested namespace
    Identifier l2tb = Identifier.of(l2ns, "iceberg_table3");
    tableCatalog.createTable(l2tb, iceberg_schema, new Transform[0], Maps.newHashMap());

    // create one iceberg view under the nested namespace
    Identifier l2view = Identifier.of(l2ns, "test_view2");
    String view2SQL = "select id from iceberg_table3 where id < 10";
    viewCatalog.createView(
        l2view,
        view2SQL,
        catalogName,
        l2ns,
        iceberg_schema,
        new String[0],
        new String[0],
        new String[0],
        Maps.newHashMap());

    // list table and views under l1ns
    Identifier[] tables = tableCatalog.listTables(l1ns);
    assertThat(tables.length).isEqualTo(2);
    assertThat(tables).contains(l1tb1, l1tb2);
    Identifier[] views = viewCatalog.listViews(l1ns);
    assertThat(views.length).isEqualTo(1);
    assertThat(views).contains(l1view);

    // list tables and views under l2ns
    tables = tableCatalog.listTables(l2ns);
    assertThat(tables.length).isEqualTo(1);
    assertThat(tables).contains(l2tb);
    views = viewCatalog.listViews(l2ns);
    assertThat(views.length).isEqualTo(1);
    assertThat(views).contains(l2view);

    // drop table under l1ns and view under l2ns
    tableCatalog.dropTable(l1tb1);
    viewCatalog.dropView(l2view);
    assertThat(viewCatalog.listViews(l2ns).length).isEqualTo(0);
    assertThat(tableCatalog.listTables(l2ns).length).isEqualTo(1);
    assertThat(viewCatalog.listViews(l1ns).length).isEqualTo(1);
    assertThat(tableCatalog.listTables(l1ns).length).isEqualTo(1);

    // drop the rest table and views
    tableCatalog.dropTable(l2tb);
    assertThat(tableCatalog.listTables(l2ns).length).isEqualTo(0);
    namespaceCatalog.dropNamespace(l2ns, true);
    assertThatThrownBy(() -> tableCatalog.listTables(l2ns))
        .isInstanceOf(org.apache.iceberg.exceptions.NoSuchNamespaceException.class);
    tableCatalog.dropTable(l1tb2);
    viewCatalog.dropView(l1view);
    assertThat(viewCatalog.listViews(l1ns).length).isEqualTo(0);
    assertThat(tableCatalog.listTables(l1ns).length).isEqualTo(0);
    namespaceCatalog.dropNamespace(l1ns, true);
  }
}
