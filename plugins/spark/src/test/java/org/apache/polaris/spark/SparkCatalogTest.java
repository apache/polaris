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

import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class SparkCatalogTest {
  private SparkCatalog catalog;
  private String catalogName;

  @BeforeEach
  public void setup() {
    catalogName = "test_" + UUID.randomUUID();
    Map<String, String> catalogConfig = Maps.newHashMap();
    catalogConfig.put(CATALOG_IMPL, "org.apache.iceberg.inmemory.InMemoryCatalog");
    catalogConfig.put("cache-enabled", "false");
    try (MockedStatic<SparkUtil> mockSparkUtil = Mockito.mockStatic(SparkUtil.class);
        MockedStatic<SparkSession> mockSparkSession = Mockito.mockStatic(SparkSession.class)) {
      Configuration conf = new Configuration();
      mockSparkUtil
          .when(() -> SparkUtil.hadoopConfCatalogOverrides(Mockito.any(), Mockito.any()))
          .thenReturn(conf);
      SparkSession mockActiveSession = Mockito.mock(SparkSession.class);
      SparkContext mockContext = Mockito.mock(SparkContext.class);
      Mockito.when(mockContext.applicationId()).thenReturn("appId");
      Mockito.when(mockContext.sparkUser()).thenReturn("testUser");
      Mockito.when(mockContext.version()).thenReturn("0.0.0");
      Mockito.when(mockActiveSession.sparkContext()).thenReturn(mockContext);

      mockSparkSession.when(() -> SparkSession.active()).thenReturn(mockActiveSession);

      catalog = new SparkCatalog();
      catalog.initialize(catalogName, new CaseInsensitiveStringMap(catalogConfig));
    }
  }

  @Test
  public void testLoadNamespaceMetadata() {
    String[] namespace = new String[] {"ns1", "ns2"};
    Assertions.assertThrows(
        NoSuchNamespaceException.class, () -> catalog.loadNamespaceMetadata(namespace));
  }

  @Test
  public void testUnsupportedOperations() {
    String[] namespace = new String[] {"ns1"};
    Identifier identifier = Identifier.of(namespace, "table1");
    Identifier new_identifier = Identifier.of(namespace, "table2");
    // table methods
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> catalog.loadTable(identifier));
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> catalog.createTable(identifier, Mockito.mock(StructType.class), null, null));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> catalog.alterTable(identifier));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> catalog.dropTable(identifier));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> catalog.renameTable(identifier, new_identifier));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> catalog.listTables(namespace));

    // namespace methods
    Assertions.assertThrows(UnsupportedOperationException.class, () -> catalog.listNamespaces());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> catalog.listNamespaces(namespace));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> catalog.createNamespace(namespace, null));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> catalog.alterNamespace(namespace));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> catalog.dropNamespace(namespace, false));

    // view methods
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> catalog.listViews(namespace));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> catalog.loadView(identifier));
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> catalog.createView(identifier, null, null, null, null, null, null, null, null));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> catalog.alterView(identifier));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> catalog.dropView(identifier));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> catalog.renameView(identifier, new_identifier));
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> catalog.replaceView(identifier, null, null, null, null, null, null, null, null));
  }
}
