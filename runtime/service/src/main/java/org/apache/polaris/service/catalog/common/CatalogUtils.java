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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;

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

  /**
   * Validates that the specified {@code locations} are valid for whatever storage config is found
   * for the given entity's parent hierarchy.
   *
   * @param realmConfig the realm configuration
   * @param identifier the table identifier (for error messages)
   * @param locations the set of locations to validate (base location + write.data.path +
   *     write.metadata.path)
   * @param resolvedStorageEntity the resolved path wrapper containing storage configuration
   * @throws ForbiddenException if any location is outside the allowed locations or if file
   *     locations are not allowed
   */
  public static void validateLocationsForTableLike(
      RealmConfig realmConfig,
      TableIdentifier identifier,
      Set<String> locations,
      PolarisResolvedPathWrapper resolvedStorageEntity) {

    PolarisStorageConfigurationInfo.getLocationRestrictionsForEntityPath(
            realmConfig, resolvedStorageEntity.getRawFullPath())
        .ifPresentOrElse(
            restrictions -> restrictions.validate(realmConfig, identifier, locations),
            () -> {
              List<String> allowedStorageTypes =
                  realmConfig.getConfig("SUPPORTED_CATALOG_STORAGE_TYPES");
              if (allowedStorageTypes != null
                  && !allowedStorageTypes.contains(StorageConfigInfo.StorageTypeEnum.FILE.name())) {
                List<String> invalidLocations =
                    locations.stream()
                        .filter(
                            location -> location.startsWith("file:") || location.startsWith("http"))
                        .collect(Collectors.toList());
                if (!invalidLocations.isEmpty()) {
                  throw new ForbiddenException(
                      "Invalid locations '%s' for identifier '%s': File locations are not allowed",
                      invalidLocations, identifier);
                }
              }
            });
  }
}
