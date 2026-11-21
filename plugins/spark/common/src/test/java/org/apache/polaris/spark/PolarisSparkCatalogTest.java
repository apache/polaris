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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.spark.utils.PolarisCatalogUtils;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class PolarisSparkCatalogTest {
  private static final StructType SCHEMA = new StructType().add("id", "long");
  private static final Identifier IDENT = Identifier.of(new String[] {"ns"}, "tbl");
  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of("ns", "tbl");

  private PolarisInMemoryCatalog polarisCatalog;
  private PolarisSparkCatalog sparkCatalog;
  private MockedStatic<PolarisCatalogUtils> mockedCatalogUtils;

  @BeforeEach
  public void setup() {
    polarisCatalog = new PolarisInMemoryCatalog();
    polarisCatalog.initialize("test", Map.of());
    polarisCatalog.createNamespace(Namespace.of("ns"));
    sparkCatalog = new PolarisSparkCatalog(polarisCatalog, null);
    // Loading the created Spark table needs an active SparkSession and the format's data source,
    // which are irrelevant to the properties stored in Polaris.
    mockedCatalogUtils = Mockito.mockStatic(PolarisCatalogUtils.class);
  }

  @AfterEach
  public void tearDown() {
    sparkCatalog.clearDelegatedCreateProperties();
    mockedCatalogUtils.close();
  }

  @Test
  public void testCreateTableMergesDelegatedCreateProperties() throws Exception {
    sparkCatalog.setDelegatedCreateProperties(
        Map.of(
            "test-owner", "test-user",
            PolarisCatalogUtils.TABLE_PROVIDER_KEY, "user-provider",
            TableCatalog.PROP_LOCATION, "file:///user/location"));

    // The delegating catalog drops the user properties and passes its own values.
    sparkCatalog.createTable(
        IDENT,
        SCHEMA,
        new Transform[0],
        Map.of(
            PolarisCatalogUtils.TABLE_PROVIDER_KEY, "delta",
            TableCatalog.PROP_LOCATION, "file:///delta/location"));

    assertThat(polarisCatalog.loadGenericTable(TABLE_IDENT).properties())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "test-owner", "test-user",
                PolarisCatalogUtils.TABLE_PROVIDER_KEY, "delta",
                TableCatalog.PROP_LOCATION, "file:///delta/location"));
  }

  @Test
  public void testCreateTableIgnoresClearedDelegatedCreateProperties() throws Exception {
    sparkCatalog.setDelegatedCreateProperties(Map.of("test-owner", "test-user"));
    sparkCatalog.clearDelegatedCreateProperties();

    Map<String, String> properties =
        Map.of(
            PolarisCatalogUtils.TABLE_PROVIDER_KEY, "delta",
            TableCatalog.PROP_LOCATION, "file:///delta/location");
    sparkCatalog.createTable(IDENT, SCHEMA, new Transform[0], properties);

    assertThat(polarisCatalog.loadGenericTable(TABLE_IDENT).properties())
        .containsExactlyInAnyOrderEntriesOf(properties);
  }
}
