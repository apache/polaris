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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaHelper {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaHelper.class);

  private static final String DELTA_CATALOG_CLASS =
      "org.apache.spark.sql.delta.catalog.DeltaCatalog";
  private TableCatalog deltaCatalog = null;

  public TableCatalog loadDeltaCatalog(PolarisSparkCatalog polarisSparkCatalog) {
    if (this.deltaCatalog != null) {
      return this.deltaCatalog;
    }

    DynConstructors.Ctor<TableCatalog> ctor;
    try {
      ctor = DynConstructors.builder(TableCatalog.class).impl(DELTA_CATALOG_CLASS).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize Delta Catalog %s: %s", DELTA_CATALOG_CLASS, e.getMessage()),
          e);
    }

    try {
      this.deltaCatalog = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize Delta Catalog, %s does not implement Table Catalog.",
              DELTA_CATALOG_CLASS),
          e);
    }

    // set the polaris spark catalog as the delegate catalog of delta catalog
    ((DelegatingCatalogExtension) this.deltaCatalog).setDelegateCatalog(polarisSparkCatalog);

    // https://github.com/delta-io/delta/issues/4306
    try {
      // Access the lazy val field's underlying method
      String methodGetName = "isUnityCatalog" + "$lzycompute";
      Method method = this.deltaCatalog.getClass().getDeclaredMethod(methodGetName);
      method.setAccessible(true);
      // invoke the lazy methods before it is set
      method.invoke(this.deltaCatalog);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(
          "Failed to find lazy compute method for isUnityCatalog, delta-spark version >= 3.2.1 is required",
          e);
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
