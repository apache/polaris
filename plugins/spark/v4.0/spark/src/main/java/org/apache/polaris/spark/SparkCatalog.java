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
package org.apache.polaris.spark;

import org.apache.iceberg.spark.SupportsReplaceView;
import org.apache.polaris.spark.utils.PolarisCatalogSpark4Utils;
import org.apache.polaris.spark.utils.PolarisCatalogUtils;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.connector.catalog.ViewInfo;

/** Spark 4.0 SparkCatalog implementation. */
public class SparkCatalog extends BasePolarisSparkCatalog
    implements ViewCatalog, SupportsReplaceView {

  @Override
  protected PolarisCatalogUtils createCatalogUtils() {
    return new PolarisCatalogSpark4Utils();
  }

  @Override
  public View createView(ViewInfo viewInfo)
      throws ViewAlreadyExistsException, NoSuchNamespaceException {
    return this.icebergsSparkCatalog.createView(viewInfo);
  }

  @Override
  public View replaceView(ViewInfo viewInfo, boolean orCreate)
      throws NoSuchNamespaceException, NoSuchViewException {
    return this.icebergsSparkCatalog.replaceView(viewInfo, orCreate);
  }
}
