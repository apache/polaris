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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.apache.iceberg.common.DynConstructors;
import org.apache.polaris.spark.PolarisSparkCatalog;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for integrating Delta table functionality with Polaris Spark Catalog.
 *
 * <p>This class is responsible for dynamically loading and configuring a Delta Catalog
 * implementation to work with Polaris. It sets up the Delta Catalog as a delegating catalog
 * extension with Polaris Spark Catalog as the delegate, enabling Delta table operations through
 * Polaris.
 *
 * <p>The class uses reflection to configure the Delta Catalog to behave identically to Unity
 * Catalog, as the current Delta Catalog implementation is hardcoded for Unity Catalog. This is a
 * temporary workaround until Delta extends support for other catalog implementations (see
 * https://github.com/delta-io/delta/issues/4306).
 */
public class DeltaHelper {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaHelper.class);

  public static final String DELTA_CATALOG_IMPL_KEY = "delta-catalog-impl";
  private static final String DEFAULT_DELTA_CATALOG_CLASS =
      "org.apache.spark.sql.delta.catalog.DeltaCatalog";

  private TableCatalog deltaCatalog = null;
  private String deltaCatalogImpl = DEFAULT_DELTA_CATALOG_CLASS;

  public DeltaHelper(CaseInsensitiveStringMap options) {
    if (options.get(DELTA_CATALOG_IMPL_KEY) != null) {
      this.deltaCatalogImpl = options.get(DELTA_CATALOG_IMPL_KEY);
    }
  }

  public TableCatalog loadDeltaCatalog(PolarisSparkCatalog polarisSparkCatalog) {
    if (this.deltaCatalog != null) {
      return this.deltaCatalog;
    }

    DynConstructors.Ctor<TableCatalog> ctor;
    try {
      ctor = DynConstructors.builder(TableCatalog.class).impl(deltaCatalogImpl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize Delta Catalog %s: %s", deltaCatalogImpl, e.getMessage()),
          e);
    }

    try {
      this.deltaCatalog = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize Delta Catalog, %s does not implement Table Catalog.",
              deltaCatalogImpl),
          e);
    }

    // set the polaris spark catalog as the delegate catalog of delta catalog
    ((DelegatingCatalogExtension) this.deltaCatalog).setDelegateCatalog(polarisSparkCatalog);

    // We want to behave exactly the same as unity catalog for Delta. However, DeltaCatalog
    // implementation today is hard coded for unity catalog. Following issue is used to track
    // the extension of the usage https://github.com/delta-io/delta/issues/4306.
    // Here, we use reflection to set the isUnityCatalog to true for exactly same behavior as
    // unity catalog for now.
    try {
      // isUnityCatalog is a lazy val, access the compute method for the lazy val
      // make sure the method is triggered before the value is set, otherwise, the
      // value will be overwritten later when the method is triggered.
      String methodGetName = "isUnityCatalog" + "$lzycompute";
      Method method = this.deltaCatalog.getClass().getDeclaredMethod(methodGetName);
      method.setAccessible(true);
      // invoke the lazy methods before it is set
      method.invoke(this.deltaCatalog);
    } catch (NoSuchMethodException e) {
      LOG.warn("No lazy compute method found for variable isUnityCatalog");
    } catch (Exception e) {
      throw new RuntimeException("Failed to invoke the lazy compute methods for isUnityCatalog", e);
    }

    try {
      Field field = this.deltaCatalog.getClass().getDeclaredField("isUnityCatalog");
      field.setAccessible(true);
      field.set(this.deltaCatalog, true);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(
          "Failed find the isUnityCatalog field, delta-spark version >= 3.2.1 is required", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed to set the isUnityCatalog field", e);
    }

    return this.deltaCatalog;
  }
}
