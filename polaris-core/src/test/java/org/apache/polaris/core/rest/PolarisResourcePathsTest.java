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
package org.apache.polaris.core.rest;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PolarisResourcePathsTest {
  private final String testPrefix = "polaris-test";

  private PolarisResourcePaths paths;

  @BeforeEach
  public void setUp() {
    Map<String, String> properties = new HashMap<>();
    properties.put(PolarisResourcePaths.PREFIX, testPrefix);
    paths = PolarisResourcePaths.forCatalogProperties(properties);
  }

  @Test
  public void testGenericTablesPath() {
    Namespace ns = Namespace.of("ns1", "ns2");
    String genericTablesPath = paths.genericTables(ns);
    String expectedPath =
        String.format("polaris/v1/%s/namespaces/%s/generic-tables", testPrefix, "ns1%1Fns2");
    Assertions.assertThat(genericTablesPath).isEqualTo(expectedPath);
  }

  @Test
  public void testGenericTablePath() {
    Namespace ns = Namespace.of("ns1");
    TableIdentifier ident = TableIdentifier.of(ns, "test-table");
    String genericTablePath = paths.genericTable(ident);
    String expectedPath =
        String.format(
            "polaris/v1/%s/namespaces/%s/generic-tables/%s", testPrefix, "ns1", "test-table");
    Assertions.assertThat(genericTablePath).isEqualTo(expectedPath);
  }
}
