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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
    catalog = new SparkCatalog();
    catalog.initialize(catalogName, new CaseInsensitiveStringMap(catalogConfig));
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

    // namespace methods
    assertThatThrownBy(() -> catalog.loadNamespaceMetadata(namespace))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.listNamespaces())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.listNamespaces(namespace))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.createNamespace(namespace, null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.alterNamespace(namespace))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.dropNamespace(namespace, false))
        .isInstanceOf(UnsupportedOperationException.class);

    // view methods
    assertThatThrownBy(() -> catalog.listViews(namespace))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.loadView(identifier))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(
            () -> catalog.createView(identifier, null, null, null, null, null, null, null, null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.alterView(identifier))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.dropView(identifier))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.renameView(identifier, new_identifier))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
