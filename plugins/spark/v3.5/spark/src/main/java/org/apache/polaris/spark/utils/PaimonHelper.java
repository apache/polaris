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
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Helper class for integrating Apache Paimon table functionality with Polaris Spark Catalog.
 *
 * <p>This class is responsible for dynamically loading and configuring a Paimon SparkCatalog
 * implementation to work with Polaris. Paimon SparkCatalog manages its own table metadata at the
 * warehouse location following the pattern: warehouse/database.db/table_name.
 *
 * <p>Unlike Delta and Hudi which use DelegatingCatalogExtension pattern, Paimon's SparkCatalog is a
 * standalone catalog that requires proper initialization with a warehouse path. This helper ensures
 * Paimon is correctly configured using the Polaris warehouse location.
 *
 * <p>Apache Paimon is a streaming data lake platform with high-speed data ingestion, changelog
 * tracking and efficient real-time analytics. This helper enables Polaris to manage Paimon tables
 * alongside Iceberg, Delta, and Hudi tables in a unified catalog.
 *
 * <p>Configuration options:
 *
 * <ul>
 *   <li>{@code paimon-warehouse}: The warehouse path for Paimon tables. This is required for Paimon
 *       to manage table metadata at the location: warehouse/database.db/table_name
 *   <li>{@code paimon-catalog-impl}: Optional custom Paimon SparkCatalog implementation class.
 *       Defaults to org.apache.paimon.spark.SparkCatalog
 * </ul>
 */
public class PaimonHelper {
  public static final String PAIMON_CATALOG_IMPL_KEY = "paimon-catalog-impl";
  public static final String PAIMON_WAREHOUSE_KEY = "paimon-warehouse";
  private static final String DEFAULT_PAIMON_CATALOG_CLASS = "org.apache.paimon.spark.SparkCatalog";

  private CatalogPlugin paimonCatalog = null;
  private final String paimonCatalogImpl;
  private final String paimonWarehouse;

  public PaimonHelper(CaseInsensitiveStringMap options) {
    if (options.get(PAIMON_CATALOG_IMPL_KEY) != null) {
      this.paimonCatalogImpl = options.get(PAIMON_CATALOG_IMPL_KEY);
    } else {
      this.paimonCatalogImpl = DEFAULT_PAIMON_CATALOG_CLASS;
    }
    // Get the Paimon-specific warehouse path from options
    this.paimonWarehouse = options.get(PAIMON_WAREHOUSE_KEY);
  }

  /**
   * Load and configure the Paimon SparkCatalog with the configured warehouse path.
   *
   * <p>Paimon SparkCatalog requires a warehouse path to manage table locations. This method
   * initializes Paimon with the warehouse path configured via the paimon-warehouse option.
   *
   * @param catalogName the name of the catalog
   * @return the configured Paimon TableCatalog
   * @throws IllegalArgumentException if paimon-warehouse is not configured
   */
  public TableCatalog loadPaimonCatalog(String catalogName) {
    if (this.paimonCatalog != null) {
      return (TableCatalog) this.paimonCatalog;
    }

    if (this.paimonWarehouse == null) {
      throw new IllegalArgumentException(
          "Paimon warehouse path is required. Please configure 'paimon-warehouse' option "
              + "for the catalog. Example: spark.sql.catalog.<catalog-name>.paimon-warehouse=/path/to/warehouse");
    }

    DynConstructors.Ctor<CatalogPlugin> ctor;
    try {
      ctor = DynConstructors.builder(CatalogPlugin.class).impl(paimonCatalogImpl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize Paimon Catalog %s: %s", paimonCatalogImpl, e.getMessage()),
          e);
    }

    CatalogPlugin plugin;
    try {
      plugin = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize Paimon Catalog, %s does not implement CatalogPlugin.",
              paimonCatalogImpl),
          e);
    }

    // Build options for Paimon initialization
    Map<String, String> paimonOptions = new HashMap<>();
    paimonOptions.put("warehouse", this.paimonWarehouse);

    // Initialize Paimon catalog with the catalog name and options
    plugin.initialize(catalogName + "_paimon", new CaseInsensitiveStringMap(paimonOptions));
    this.paimonCatalog = plugin;

    return (TableCatalog) this.paimonCatalog;
  }

  /**
   * Ensure the namespace exists in Paimon catalog before creating a table.
   *
   * @param namespace the namespace to ensure exists
   */
  public void ensureNamespaceExists(String[] namespace) {
    if (this.paimonCatalog instanceof SupportsNamespaces) {
      SupportsNamespaces nsSupport = (SupportsNamespaces) this.paimonCatalog;
      if (!nsSupport.namespaceExists(namespace)) {
        try {
          nsSupport.createNamespace(namespace, new HashMap<>());
        } catch (Exception e) {
          // Namespace might already exist due to race condition, ignore
        }
      }
    }
  }
}
