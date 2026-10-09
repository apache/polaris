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

  @Test
  public void testCredentialsPath() {
    Namespace ns = Namespace.of("ns1", "ns2");
    TableIdentifier ident = TableIdentifier.of(ns, "test-table");
    String credentialsPath = paths.credentialsPath(ident);
    String expectedPath =
        String.format(
            "v1/%s/namespaces/%s/tables/%s/credentials", testPrefix, "ns1%1Fns2", "test-table");
    Assertions.assertThat(credentialsPath).isEqualTo(expectedPath);
  }

  @Test
  public void testGenericTablesPathWithSpecialChars() {
    Namespace ns = Namespace.of("ns 1", "ns+2");
    String genericTablesPath = paths.genericTables(ns);
    String expectedPath =
        String.format("polaris/v1/%s/namespaces/%s/generic-tables", testPrefix, "ns%201%1Fns%2B2");
    Assertions.assertThat(genericTablesPath).isEqualTo(expectedPath);
  }

  @Test
  public void testGenericTablePathWithSpecialChars() {
    Namespace ns = Namespace.of("ns 1", "ns+2");
    TableIdentifier ident = TableIdentifier.of(ns, "test table+1");
    String genericTablePath = paths.genericTable(ident);
    String expectedPath =
        String.format(
            "polaris/v1/%s/namespaces/%s/generic-tables/%s",
            testPrefix, "ns%201%1Fns%2B2", "test%20table%2B1");
    Assertions.assertThat(genericTablePath).isEqualTo(expectedPath);
  }

  @Test
  public void testCredentialsPathWithSpecialChars() {
    Namespace ns = Namespace.of("ns 1", "ns+2");
    TableIdentifier ident = TableIdentifier.of(ns, "test table+1");
    String credentialsPath = paths.credentialsPath(ident);
    String expectedPath =
        String.format(
            "v1/%s/namespaces/%s/tables/%s/credentials",
            testPrefix, "ns%201%1Fns%2B2", "test%20table%2B1");
    Assertions.assertThat(credentialsPath).isEqualTo(expectedPath);
  }

  @Test
  public void testDirectoriesPath() {
    Namespace ns = Namespace.of("ns1", "ns2");
    String directoriesPath = paths.directories(ns);
    String expectedPath =
        String.format("polaris/v1/%s/namespaces/%s/directories", testPrefix, "ns1%1Fns2");
    Assertions.assertThat(directoriesPath).isEqualTo(expectedPath);
  }

  @Test
  public void testDirectoryPath() {
    Namespace ns = Namespace.of("ns1");
    TableIdentifier ident = TableIdentifier.of(ns, "test-dir");
    String directoryPath = paths.directory(ident);
    String expectedPath =
        String.format("polaris/v1/%s/namespaces/%s/directories/%s", testPrefix, "ns1", "test-dir");
    Assertions.assertThat(directoryPath).isEqualTo(expectedPath);
  }

  @Test
  public void testDirectoriesPathWithSpecialChars() {
    Namespace ns = Namespace.of("ns 1", "ns+2");
    String directoriesPath = paths.directories(ns);
    String expectedPath =
        String.format("polaris/v1/%s/namespaces/%s/directories", testPrefix, "ns%201%1Fns%2B2");
    Assertions.assertThat(directoriesPath).isEqualTo(expectedPath);
  }

  @Test
  public void testDirectoryPathWithSpecialChars() {
    Namespace ns = Namespace.of("ns 1", "ns+2");
    TableIdentifier ident = TableIdentifier.of(ns, "test dir+1");
    String directoryPath = paths.directory(ident);
    String expectedPath =
        String.format(
            "polaris/v1/%s/namespaces/%s/directories/%s",
            testPrefix, "ns%201%1Fns%2B2", "test%20dir%2B1");
    Assertions.assertThat(directoryPath).isEqualTo(expectedPath);
  }
}
