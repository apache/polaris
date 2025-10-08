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

package org.apache.polaris.service.catalog.common;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;

/** Utility methods for working with Polaris catalog entities. */
public class CatalogUtils {

  /**
   * Find the resolved entity path that may contain storage information
   *
   * @param resolvedEntityView The resolved entity view containing catalog entities.
   * @param tableIdentifier The table identifier for which to find storage information.
   * @return The resolved path wrapper that may contain storage information.
   */
  public static PolarisResolvedPathWrapper findResolvedStorageEntity(
      PolarisResolutionManifestCatalogView resolvedEntityView, TableIdentifier tableIdentifier) {
    PolarisResolvedPathWrapper resolvedTableEntities =
        resolvedEntityView.getResolvedPath(
            tableIdentifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE);
    if (resolvedTableEntities != null) {
      return resolvedTableEntities;
    }
    return resolvedEntityView.getResolvedPath(tableIdentifier.namespace());
  }
}
