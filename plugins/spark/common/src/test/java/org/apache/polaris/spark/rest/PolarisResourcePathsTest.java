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
package org.apache.polaris.spark.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PolarisResourcePathsTest {
  private static final String testPrefix = "polaris-test";

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
    String expectedPath =
        String.format("polaris/v1/%s/namespaces/%s/generic-tables", testPrefix, "ns1%1Fns2");
    assertThat(paths.genericTables(ns)).isEqualTo(expectedPath);
  }

  @Test
  public void testGenericTablePath() {
    Namespace ns = Namespace.of("ns1");
    TableIdentifier ident = TableIdentifier.of(ns, "test-table");
    String expectedPath =
        String.format(
            "polaris/v1/%s/namespaces/%s/generic-tables/%s", testPrefix, "ns1", "test-table");
    assertThat(paths.genericTable(ident)).isEqualTo(expectedPath);
  }

  @Test
  public void testGenericTablesPathWithSpecialChars() {
    Namespace ns = Namespace.of("ns 1", "ns+2");
    String expectedPath =
        String.format("polaris/v1/%s/namespaces/%s/generic-tables", testPrefix, "ns%201%1Fns%2B2");
    assertThat(paths.genericTables(ns)).isEqualTo(expectedPath);
  }

  @Test
  public void testGenericTablePathWithSpecialChars() {
    Namespace ns = Namespace.of("ns 1", "ns+2");
    TableIdentifier ident = TableIdentifier.of(ns, "test table+1");
    String expectedPath =
        String.format(
            "polaris/v1/%s/namespaces/%s/generic-tables/%s",
            testPrefix, "ns%201%1Fns%2B2", "test%20table%2B1");
    assertThat(paths.genericTable(ident)).isEqualTo(expectedPath);
  }
}
