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
package org.apache.polaris.spark;

import static org.apache.iceberg.CatalogProperties.CATALOG_IMPL;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class SparkCatalogTest {
  private SparkCatalog catalog;
  private String catalogName;

  private static final String[] defaultNS = new String[] {"ns"};
  private static final Schema defaultSchema =
      new Schema(
          5,
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get()));

  @BeforeEach
  public void setup() throws Exception {
    catalogName = "test_" + UUID.randomUUID();
    Map<String, String> catalogConfig = Maps.newHashMap();
    catalogConfig.put(CATALOG_IMPL, "org.apache.iceberg.inmemory.InMemoryCatalog");
    catalogConfig.put("cache-enabled", "false");

    catalog = new SparkCatalog();
    Configuration conf = new Configuration();
    try (MockedStatic<SparkSession> mockedStaticSparkSession =
            Mockito.mockStatic(SparkSession.class);
        MockedStatic<SparkUtil> mockedSparkUtil = Mockito.mockStatic(SparkUtil.class)) {
      SparkSession mockedSession = Mockito.mock(SparkSession.class);
      mockedStaticSparkSession.when(SparkSession::active).thenReturn(mockedSession);
      mockedSparkUtil
          .when(() -> SparkUtil.hadoopConfCatalogOverrides(mockedSession, catalogName))
          .thenReturn(conf);
      SparkContext mockedContext = Mockito.mock(SparkContext.class);
      Mockito.when(mockedSession.sparkContext()).thenReturn(mockedContext);
      Mockito.when(mockedContext.applicationId()).thenReturn("appId");
      Mockito.when(mockedContext.sparkUser()).thenReturn("test-user");
      Mockito.when(mockedContext.version()).thenReturn("3.5");

      catalog.initialize(catalogName, new CaseInsensitiveStringMap(catalogConfig));
    }
    catalog.createNamespace(defaultNS, Maps.newHashMap());
  }

  @Test
  void testCreateAndLoadNamespace() throws Exception {
    String[] namespace = new String[] {"ns1"};
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put("key1", "value1");

    // no namespace can be found
    assertThatThrownBy(() -> catalog.loadNamespaceMetadata(namespace))
        .isInstanceOf(NoSuchNamespaceException.class);

    // create the namespace
    catalog.createNamespace(namespace, metadata);

    Map<String, String> nsMetadata = catalog.loadNamespaceMetadata(namespace);
    assertThat(nsMetadata).contains(Map.entry("key1", "value1"));
  }

  @Test
  void testDropAndListNamespaces() throws Exception {
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
    assertThat(lv1nsResult.length).isEqualTo(lv1ns.length + 1);
    assertThat(Arrays.asList(lv1nsResult)).contains(defaultNS);
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

    // drop l1ns2
    catalog.dropNamespace(lv2ns2[0], true);
    assertThat(catalog.listNamespaces(lv1ns[1]).length).isEqualTo(0);

    catalog.dropNamespace(lv1ns[1], true);
    assertThatThrownBy(() -> catalog.listNamespaces(lv1ns[1]))
        .isInstanceOf(NoSuchNamespaceException.class);
  }

  @Test
  void testAlterNamespace() throws Exception {
    String[] namespace = new String[] {"ns1"};
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put("orig_key1", "orig_value1");

    catalog.createNamespace(namespace, metadata);
    assertThat(catalog.loadNamespaceMetadata(namespace))
        .contains(Map.entry("orig_key1", "orig_value1"));

    catalog.alterNamespace(namespace, NamespaceChange.setProperty("new_key", "new_value"));
    assertThat(catalog.loadNamespaceMetadata(namespace))
        .contains(Map.entry("new_key", "new_value"));
  }

  @Test
  void testStageOperations() throws Exception {
    Identifier createId = Identifier.of(defaultNS, "iceberg-table-create");
    Map<String, String> icebergProperties = Maps.newHashMap();
    icebergProperties.put("provider", "iceberg");
    icebergProperties.put(TableCatalog.PROP_LOCATION, "file:///tmp/path/to/iceberg-table/");
    StructType iceberg_schema = new StructType().add("boolType", "boolean");

    catalog.stageCreate(createId, iceberg_schema, new Transform[0], icebergProperties);

    catalog.stageCreateOrReplace(createId, iceberg_schema, new Transform[0], icebergProperties);
  }

  @Test
  void testBasicViewOperations() throws Exception {
    Identifier viewIdentifier = Identifier.of(defaultNS, "test-view");
    String viewSql = "select id from test-table where id < 3";
    StructType schema = new StructType().add("id", "long");
    catalog.createView(
        viewIdentifier,
        viewSql,
        catalogName,
        defaultNS,
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
    catalog.alterView(viewIdentifier, ViewChange.setProperty("view_key1", "view_value1"));
    view = catalog.loadView(viewIdentifier);
    assertThat(view.properties()).contains(Map.entry("view_key1", "view_value1"));

    // rename the view
    Identifier newIdentifier = Identifier.of(defaultNS, "new-view");
    catalog.renameView(viewIdentifier, newIdentifier);
    assertThatThrownBy(() -> catalog.loadView(viewIdentifier))
        .isInstanceOf(NoSuchViewException.class);
    view = catalog.loadView(newIdentifier);
    assertThat(view.query()).isEqualTo(viewSql);
    assertThat(view.schema()).isEqualTo(schema);

    // replace the view
    String newSql = "select id from test-table where id == 3";
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "value1");
    catalog.replaceView(
        newIdentifier,
        newSql,
        catalogName,
        defaultNS,
        schema,
        new String[0],
        new String[0],
        new String[0],
        properties);
    view = catalog.loadView(newIdentifier);
    assertThat(view.query()).isEqualTo(newSql);
    assertThat(view.properties()).contains(Map.entry("key1", "value1"));

    // drop the view
    catalog.dropView(newIdentifier);
    assertThatThrownBy(() -> catalog.loadView(newIdentifier))
        .isInstanceOf(NoSuchViewException.class);
  }

  @Test
  void testListViews() throws Exception {
    // create a new namespace under the default NS
    String[] namespace = new String[] {"ns", "nsl2"};
    catalog.createNamespace(namespace, Maps.newHashMap());
    // table schema
    StructType schema = new StructType().add("id", "long").add("name", "string");
    // create  under defaultNS
    String view1Name = "test-view1";
    String view1SQL = "select id from test-table where id >= 3";
    catalog.createView(
        Identifier.of(defaultNS, view1Name),
        view1SQL,
        catalogName,
        defaultNS,
        schema,
        new String[0],
        new String[0],
        new String[0],
        Maps.newHashMap());
    // create two views under ns.nsl2
    String[] nsl2ViewNames = new String[] {"test-view2", "test-view3"};
    String[] nsl2ViewSQLs =
        new String[] {
          "select id from test-table where id == 3", "select id from test-table where id < 3"
        };
    for (int i = 0; i < nsl2ViewNames.length; i++) {
      catalog.createView(
          Identifier.of(namespace, nsl2ViewNames[i]),
          nsl2ViewSQLs[i],
          catalogName,
          namespace,
          schema,
          new String[0],
          new String[0],
          new String[0],
          Maps.newHashMap());
    }
    // list views under defaultNS
    Identifier[] l1Views = catalog.listViews(defaultNS);
    assertThat(l1Views.length).isEqualTo(1);
    assertThat(l1Views[0].name()).isEqualTo(view1Name);

    // list views under ns1.nsl2
    Identifier[] l2Views = catalog.listViews(namespace);
    assertThat(l2Views.length).isEqualTo(nsl2ViewSQLs.length);
    for (String name : nsl2ViewNames) {
      assertThat(Arrays.asList(l2Views)).contains(Identifier.of(namespace, name));
    }
  }

  @Test
  public void testUnsupportedOperations() {
    String[] namespace = new String[] {"ns1"};
    Identifier identifier = Identifier.of(namespace, "table1");
    Identifier new_identifier = Identifier.of(namespace, "table2");
    // table methods
    assertThatThrownBy(() -> catalog.loadTable(identifier))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(
            () -> catalog.createTable(identifier, Mockito.mock(StructType.class), null, null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.alterTable(identifier))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.dropTable(identifier))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.renameTable(identifier, new_identifier))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.listTables(namespace))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.invalidateTable(identifier))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.purgeTable(identifier))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
