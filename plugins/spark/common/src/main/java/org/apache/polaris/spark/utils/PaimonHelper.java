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

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.common.DynConstructors;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Helper class for integrating Apache Paimon table functionality with Polaris Spark Catalog.
 *
 * <p>This class dynamically loads a standalone Paimon Spark catalog. Paimon owns its physical table
 * metadata and data under a separately configured warehouse, while Polaris stores a Generic Table
 * registration used for unified discovery.
 *
 * <p>Apache Paimon is a streaming data lake platform with high-speed data ingestion, changelog
 * tracking and efficient real-time analytics. This helper enables Polaris to manage Paimon tables
 * alongside Iceberg, Delta, and Hudi tables in a unified catalog.
 */
public class PaimonHelper {
  public static final String PAIMON_CATALOG_IMPL_KEY = "paimon-catalog-impl";
  public static final String PAIMON_WAREHOUSE_KEY = "paimon-warehouse";
  private static final String DEFAULT_PAIMON_CATALOG_CLASS = "org.apache.paimon.spark.SparkCatalog";

  private TableCatalog paimonCatalog = null;
  private final String paimonCatalogImpl;
  private final String paimonWarehouse;
  private final String catalogName;

  public PaimonHelper(String catalogName, CaseInsensitiveStringMap options) {
    this.catalogName = catalogName;
    if (options.get(PAIMON_CATALOG_IMPL_KEY) != null) {
      this.paimonCatalogImpl = options.get(PAIMON_CATALOG_IMPL_KEY);
    } else {
      this.paimonCatalogImpl = DEFAULT_PAIMON_CATALOG_CLASS;
    }
    this.paimonWarehouse = options.get(PAIMON_WAREHOUSE_KEY);
  }

  /**
   * Load and configure the standalone Paimon catalog.
   *
   * @return the configured Paimon TableCatalog
   * @throws IllegalArgumentException if paimon-warehouse is not configured
   */
  public synchronized TableCatalog loadPaimonCatalog() {
    if (this.paimonCatalog != null) {
      return this.paimonCatalog;
    }

    if (this.paimonWarehouse == null) {
      throw new IllegalArgumentException(
          "Paimon warehouse path is required. Please configure 'paimon-warehouse' option "
              + "for the catalog. Example: spark.sql.catalog.<catalog-name>.paimon-warehouse=/path/to/warehouse");
    }

    DynConstructors.Ctor<TableCatalog> ctor;
    try {
      ctor = DynConstructors.builder(TableCatalog.class).impl(paimonCatalogImpl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize Paimon Catalog %s: %s", paimonCatalogImpl, e.getMessage()),
          e);
    }

    TableCatalog catalog;
    try {
      catalog = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize Paimon Catalog, %s does not implement TableCatalog.",
              paimonCatalogImpl),
          e);
    }

    Map<String, String> paimonOptions = new HashMap<>();
    paimonOptions.put("warehouse", this.paimonWarehouse);
    ((CatalogPlugin) catalog)
        .initialize(this.catalogName + "_paimon", new CaseInsensitiveStringMap(paimonOptions));
    this.paimonCatalog = catalog;

    return this.paimonCatalog;
  }

  /** Ensure the namespace exists in the standalone Paimon catalog before creating a table. */
  public void ensureNamespaceExists(String[] namespace) {
    if (this.paimonCatalog instanceof SupportsNamespaces) {
      SupportsNamespaces nsSupport = (SupportsNamespaces) this.paimonCatalog;
      if (!nsSupport.namespaceExists(namespace)) {
        try {
          nsSupport.createNamespace(namespace, new HashMap<>());
        } catch (NamespaceAlreadyExistsException e) {
          // Suppress only a confirmed concurrent-creation race.
          if (!nsSupport.namespaceExists(namespace)) {
            throw new IllegalStateException(
                "Paimon namespace still does not exist after a concurrent creation failure", e);
          }
        }
      }
    }
  }
}
