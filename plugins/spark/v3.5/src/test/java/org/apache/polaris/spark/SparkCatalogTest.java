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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.polaris.spark.utils.DeltaHelper;
import org.apache.polaris.spark.utils.PolarisCatalogUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
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
    catalogConfig.put(PolarisCatalogUtils.ENABLE_IN_MEMORY_CATALOG_KEY, "true");
    catalogConfig.put(
        DeltaHelper.DELTA_CATALOG_IMPL_KEY, "org.apache.polaris.spark.InMemoryDeltaCatalog");
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
  }

  @Test
  void testCreateAndLoadNamespace()
      throws NoSuchNamespaceException, NamespaceAlreadyExistsException {
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
  public void testUnsupportedOperations() {
    String[] namespace = new String[] {"ns1"};
    Identifier identifier = Identifier.of(namespace, "table1");
    Identifier new_identifier = Identifier.of(namespace, "table2");
    // table methods
    assertThatThrownBy(() -> catalog.alterTable(identifier))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.dropTable(identifier))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.renameTable(identifier, new_identifier))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.listTables(namespace))
        .isInstanceOf(UnsupportedOperationException.class);

    // namespace methods
    assertThatThrownBy(() -> catalog.listNamespaces())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> catalog.listNamespaces(namespace))
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
