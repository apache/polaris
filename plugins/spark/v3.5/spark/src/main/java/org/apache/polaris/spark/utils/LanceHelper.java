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
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LanceHelper {
  private static final Logger LOG = LoggerFactory.getLogger(LanceHelper.class);

  public static final String LANCE_CATALOG_IMPL_KEY = "lance-catalog-impl";
  private static final String DEFAULT_LANCE_CATALOG_CLASS =
      "com.lancedb.lance.spark.LanceNamespaceSparkCatalog";

  private TableCatalog lanceCatalog = null;
  private String lanceCatalogImpl = DEFAULT_LANCE_CATALOG_CLASS;
  private CaseInsensitiveStringMap options;

  public LanceHelper(CaseInsensitiveStringMap options) {
    this.options = options;
    if (options.get(LANCE_CATALOG_IMPL_KEY) != null) {
      this.lanceCatalogImpl = options.get(LANCE_CATALOG_IMPL_KEY);
    }
  }

  public TableCatalog loadLanceCatalog(PolarisSparkCatalog polarisSparkCatalog) {
    if (this.lanceCatalog != null) {
      return this.lanceCatalog;
    }

    DynConstructors.Ctor<TableCatalog> ctor;
    try {
      ctor = DynConstructors.builder(TableCatalog.class).impl(lanceCatalogImpl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize Lance Catalog %s: %s", lanceCatalogImpl, e.getMessage()),
          e);
    }

    try {
      this.lanceCatalog = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize Lance Catalog, %s does not implement TableCatalog.",
              lanceCatalogImpl),
          e);
    }

    // Initialize the Lance catalog with Polaris configuration
    // Lance catalog uses its own initialization pattern
    this.lanceCatalog.initialize(polarisSparkCatalog.name(), this.options);

    LOG.info("Successfully loaded Lance catalog: {}", lanceCatalogImpl);
    return this.lanceCatalog;
  }
}
