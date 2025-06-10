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

public class HudiHelper {
  public static final String HUDI_CATALOG_IMPL_KEY = "hudi-catalog-impl";
  private static final String DEFAULT_HUDI_CATALOG_CLASS =
      "org.apache.spark.sql.hudi.catalog.HoodieCatalog";

  private TableCatalog hudiCatalog = null;
  private String hudiCatalogImpl = DEFAULT_HUDI_CATALOG_CLASS;

  public HudiHelper(CaseInsensitiveStringMap options) {
    if (options.get(HUDI_CATALOG_IMPL_KEY) != null) {
      this.hudiCatalogImpl = options.get(HUDI_CATALOG_IMPL_KEY);
    }
  }

  public TableCatalog loadHudiCatalog(PolarisSparkCatalog polarisSparkCatalog) {
    if (this.hudiCatalog != null) {
      return this.hudiCatalog;
    }

    DynConstructors.Ctor<TableCatalog> ctor;
    try {
      ctor = DynConstructors.builder(TableCatalog.class).impl(hudiCatalogImpl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize Hudi Catalog %s: %s", hudiCatalogImpl, e.getMessage()),
          e);
    }

    try {
      this.hudiCatalog = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize Hudi Catalog, %s does not implement Table Catalog.",
              hudiCatalogImpl),
          e);
    }

    // set the polaris spark catalog as the delegate catalog of hudi catalog
    // will be used in HoodieCatalog's loadTable
    ((DelegatingCatalogExtension) this.hudiCatalog).setDelegateCatalog(polarisSparkCatalog);
    return this.hudiCatalog;
  }
}
