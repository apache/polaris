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

import org.apache.iceberg.common.DynConstructors;
import org.apache.polaris.spark.PolarisSparkCatalog;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Helper class for integrating Apache Paimon table functionality with Polaris Spark Catalog.
 *
 * <p>This class is responsible for dynamically loading and configuring a Paimon Catalog
 * implementation to work with Polaris. It sets up the Paimon Catalog as a delegating catalog
 * extension with Polaris Spark Catalog as the delegate, enabling Paimon table operations through
 * Polaris.
 *
 * <p>Apache Paimon is a streaming data lake platform with high-speed data ingestion, changelog
 * tracking and efficient real-time analytics. This helper enables Polaris to manage Paimon tables
 * alongside Iceberg, Delta, and Hudi tables in a unified catalog.
 */
public class PaimonHelper {
  public static final String PAIMON_CATALOG_IMPL_KEY = "paimon-catalog-impl";
  private static final String DEFAULT_PAIMON_CATALOG_CLASS = "org.apache.paimon.spark.SparkCatalog";

  private TableCatalog paimonCatalog = null;
  private String paimonCatalogImpl = DEFAULT_PAIMON_CATALOG_CLASS;

  public PaimonHelper(CaseInsensitiveStringMap options) {
    if (options.get(PAIMON_CATALOG_IMPL_KEY) != null) {
      this.paimonCatalogImpl = options.get(PAIMON_CATALOG_IMPL_KEY);
    }
  }

  /**
   * Load and configure the Paimon catalog with Polaris Spark Catalog as the delegate.
   *
   * @param polarisSparkCatalog the Polaris Spark Catalog to set as delegate
   * @return the configured Paimon TableCatalog
   */
  public TableCatalog loadPaimonCatalog(PolarisSparkCatalog polarisSparkCatalog) {
    if (this.paimonCatalog != null) {
      return this.paimonCatalog;
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

    try {
      this.paimonCatalog = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize Paimon Catalog, %s does not implement TableCatalog.",
              paimonCatalogImpl),
          e);
    }

    // Set the Polaris Spark Catalog as the delegate catalog of Paimon Catalog.
    // This allows Paimon to use Polaris for metadata management while handling
    // Paimon-specific operations like manifest and snapshot management.
    if (this.paimonCatalog instanceof DelegatingCatalogExtension) {
      ((DelegatingCatalogExtension) this.paimonCatalog).setDelegateCatalog(polarisSparkCatalog);
    }

    return this.paimonCatalog;
  }
}
