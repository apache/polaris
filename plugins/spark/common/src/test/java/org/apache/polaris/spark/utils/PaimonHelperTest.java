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
import java.util.Map;
import org.apache.polaris.spark.NoopPaimonCatalog;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

public class PaimonHelperTest {
  private static final String CATALOG_NAME = "test_catalog";
  private static final String WAREHOUSE = "/tmp/test-paimon-warehouse";

  public static class ConcurrentNamespaceCreationCatalog extends NoopPaimonCatalog {
    private boolean initialCheck = true;

    @Override
    public boolean namespaceExists(String[] namespace) {
      if (initialCheck) {
        initialCheck = false;
        return false;
      }
      return true;
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
        throws NamespaceAlreadyExistsException {
      throw new NamespaceAlreadyExistsException(namespace);
    }
  }

  public static class FailingNamespaceCreationCatalog extends NoopPaimonCatalog {
    @Override
    public boolean namespaceExists(String[] namespace) {
      return false;
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata) {
      throw new UnsupportedOperationException("namespace creation failed");
    }
  }

  @Test
  public void testLoadStandaloneCatalogAndCacheInstance() {
    PaimonHelper helper = helperFor(NoopPaimonCatalog.class);

    TableCatalog first = helper.loadPaimonCatalog();
    TableCatalog second = helper.loadPaimonCatalog();

    assertThat(first).isInstanceOf(NoopPaimonCatalog.class);
    assertThat(first).isInstanceOf(SupportsNamespaces.class);
    assertThat(second).isSameAs(first);
  }

  @Test
  public void testLoadPaimonCatalogWithoutWarehouseFails() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                PaimonHelper.PAIMON_CATALOG_IMPL_KEY, NoopPaimonCatalog.class.getName()));
    PaimonHelper helper = new PaimonHelper(CATALOG_NAME, options);

    assertThatThrownBy(helper::loadPaimonCatalog)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("paimon-warehouse");
  }

  @Test
  public void testEnsureNamespaceExistsIgnoresConfirmedConcurrentCreation() {
    PaimonHelper helper = helperFor(ConcurrentNamespaceCreationCatalog.class);
    helper.loadPaimonCatalog();

    helper.ensureNamespaceExists(new String[] {"concurrent_db"});
  }

  @Test
  public void testEnsureNamespaceExistsPropagatesCreationFailure() {
    PaimonHelper helper = helperFor(FailingNamespaceCreationCatalog.class);
    helper.loadPaimonCatalog();

    assertThatThrownBy(() -> helper.ensureNamespaceExists(new String[] {"failing_db"}))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("namespace creation failed");
  }

  private static PaimonHelper helperFor(Class<? extends TableCatalog> implementation) {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                PaimonHelper.PAIMON_CATALOG_IMPL_KEY,
                implementation.getName(),
                PaimonHelper.PAIMON_WAREHOUSE_KEY,
                WAREHOUSE));
    return new PaimonHelper(CATALOG_NAME, options);
  }
}
