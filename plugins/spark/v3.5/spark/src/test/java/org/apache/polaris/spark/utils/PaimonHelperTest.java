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
package org.apache.polaris.spark.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import org.apache.polaris.spark.NoopPaimonCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

public class PaimonHelperTest {

  private static final String TEST_CATALOG_NAME = "test_catalog";
  private static final String TEST_WAREHOUSE = "/tmp/test-paimon-warehouse";

  @Test
  public void testLoadPaimonCatalogWithNoopPaimonCatalog() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                PaimonHelper.PAIMON_CATALOG_IMPL_KEY,
                "org.apache.polaris.spark.NoopPaimonCatalog",
                PaimonHelper.PAIMON_WAREHOUSE_KEY,
                TEST_WAREHOUSE));
    PaimonHelper helper = new PaimonHelper(options);

    TableCatalog paimonCatalog = helper.loadPaimonCatalog(TEST_CATALOG_NAME);

    assertThat(paimonCatalog).isNotNull();
    assertThat(paimonCatalog).isInstanceOf(NoopPaimonCatalog.class);
    assertThat(paimonCatalog).isInstanceOf(TableCatalog.class);
    assertThat(paimonCatalog).isInstanceOf(SupportsNamespaces.class);
  }

  @Test
  public void testLoadPaimonCatalogCachesInstance() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                PaimonHelper.PAIMON_CATALOG_IMPL_KEY,
                "org.apache.polaris.spark.NoopPaimonCatalog",
                PaimonHelper.PAIMON_WAREHOUSE_KEY,
                TEST_WAREHOUSE));
    PaimonHelper helper = new PaimonHelper(options);

    TableCatalog paimonCatalog1 = helper.loadPaimonCatalog(TEST_CATALOG_NAME);
    TableCatalog paimonCatalog2 = helper.loadPaimonCatalog(TEST_CATALOG_NAME);

    // Should return the same cached instance
    assertThat(paimonCatalog1).isSameAs(paimonCatalog2);
  }

  @Test
  public void testLoadPaimonCatalogWithNonExistentClass() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                PaimonHelper.PAIMON_CATALOG_IMPL_KEY,
                "com.example.NonExistentPaimonCatalog",
                PaimonHelper.PAIMON_WAREHOUSE_KEY,
                TEST_WAREHOUSE));
    PaimonHelper helper = new PaimonHelper(options);

    assertThatThrownBy(() -> helper.loadPaimonCatalog(TEST_CATALOG_NAME))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot initialize Paimon Catalog")
        .hasMessageContaining("com.example.NonExistentPaimonCatalog");
  }

  @Test
  public void testLoadPaimonCatalogWithCaseInsensitiveOptions() {
    // Test that options are case-insensitive
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                "PAIMON-CATALOG-IMPL",
                "org.apache.polaris.spark.NoopPaimonCatalog",
                "PAIMON-WAREHOUSE",
                TEST_WAREHOUSE));
    PaimonHelper helper = new PaimonHelper(options);

    TableCatalog paimonCatalog = helper.loadPaimonCatalog(TEST_CATALOG_NAME);

    assertThat(paimonCatalog).isNotNull();
    assertThat(paimonCatalog).isInstanceOf(NoopPaimonCatalog.class);
  }

  @Test
  public void testLoadPaimonCatalogWithoutWarehouseThrowsException() {
    // Test that missing warehouse configuration throws appropriate error
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                PaimonHelper.PAIMON_CATALOG_IMPL_KEY,
                "org.apache.polaris.spark.NoopPaimonCatalog"));
    PaimonHelper helper = new PaimonHelper(options);

    assertThatThrownBy(() -> helper.loadPaimonCatalog(TEST_CATALOG_NAME))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Paimon warehouse path is required")
        .hasMessageContaining("paimon-warehouse");
  }

  @Test
  public void testEnsureNamespaceExists() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                PaimonHelper.PAIMON_CATALOG_IMPL_KEY,
                "org.apache.polaris.spark.NoopPaimonCatalog",
                PaimonHelper.PAIMON_WAREHOUSE_KEY,
                TEST_WAREHOUSE));
    PaimonHelper helper = new PaimonHelper(options);

    // Load the catalog first
    TableCatalog paimonCatalog = helper.loadPaimonCatalog(TEST_CATALOG_NAME);

    // Ensure namespace exists should not throw
    String[] namespace = new String[] {"test_db"};
    helper.ensureNamespaceExists(namespace);

    // Verify namespace was created
    assertThat(paimonCatalog).isInstanceOf(SupportsNamespaces.class);
    SupportsNamespaces nsSupport = (SupportsNamespaces) paimonCatalog;
    assertThat(nsSupport.namespaceExists(namespace)).isTrue();
  }

  @Test
  public void testEnsureNamespaceExistsIdempotent() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                PaimonHelper.PAIMON_CATALOG_IMPL_KEY,
                "org.apache.polaris.spark.NoopPaimonCatalog",
                PaimonHelper.PAIMON_WAREHOUSE_KEY,
                TEST_WAREHOUSE));
    PaimonHelper helper = new PaimonHelper(options);
    helper.loadPaimonCatalog(TEST_CATALOG_NAME);

    String[] namespace = new String[] {"test_db"};

    // Calling ensureNamespaceExists multiple times should not throw
    helper.ensureNamespaceExists(namespace);
    helper.ensureNamespaceExists(namespace);
    helper.ensureNamespaceExists(namespace);
  }
}
