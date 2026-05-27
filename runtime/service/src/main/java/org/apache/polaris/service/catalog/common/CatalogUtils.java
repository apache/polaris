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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisEntityUtils;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.StorageLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods for working with Polaris catalog entities. */
public class CatalogUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogUtils.class);

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
            ResolvedPathKey.ofTableLike(tableIdentifier), PolarisEntitySubType.ICEBERG_TABLE);
    if (resolvedTableEntities != null) {
      return resolvedTableEntities;
    }
    return resolvedEntityView.getResolvedPath(
        ResolvedPathKey.ofNamespace(tableIdentifier.namespace()));
  }

  /**
   * Validates that the specified {@code location} is valid for whatever storage config is found for
   * this TableLike's parent hierarchy. Resolves the storage entity from the given entity view.
   */
  public static void validateLocationForTableLike(
      PolarisResolutionManifestCatalogView resolvedEntityView,
      RealmConfig realmConfig,
      TableIdentifier identifier,
      String location) {
    PolarisResolvedPathWrapper resolvedStorageEntity =
        resolvedEntityView.getResolvedPath(
            ResolvedPathKey.ofTableLike(identifier), PolarisEntitySubType.ANY_SUBTYPE);
    if (resolvedStorageEntity == null) {
      resolvedStorageEntity =
          resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(identifier.namespace()));
    }
    if (resolvedStorageEntity == null) {
      resolvedStorageEntity =
          resolvedEntityView.getPassthroughResolvedPath(
              ResolvedPathKey.ofNamespace(identifier.namespace()));
    }

    validateLocationsForTableLike(realmConfig, identifier, Set.of(location), resolvedStorageEntity);
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

    PolarisStorageConfigurationInfo.forEntityPath(
            realmConfig, resolvedStorageEntity.getRawFullPath())
        .ifPresentOrElse(
            restrictions -> restrictions.validate(realmConfig, identifier, locations),
            () -> {
              List<String> allowedStorageTypes =
                  realmConfig.getConfig(FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES);
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

  /**
   * Validate no location overlap exists between the given entity and its sibling entities. This
   * lists all siblings at the same level (namespaces if the parent is the catalog, namespaces and
   * table-like entities otherwise) and checks the base-location property of each. The entity's base
   * location may not be a prefix or a suffix of any sibling entity's base location.
   *
   * <p>When an optimized sibling check is supported by the persistence layer, it is attempted
   * first. Otherwise, all siblings are listed and checked individually.
   *
   * @param realmConfig the realm configuration
   * @param metaStoreManager the meta store manager for entity queries
   * @param polarisCallContext the polaris call context
   * @param entity the entity whose location is being validated
   * @param parentPath the resolved parent entity path
   */
  public static <T extends PolarisEntity & LocationBasedEntity> void validateNoLocationOverlap(
      RealmConfig realmConfig,
      PolarisMetaStoreManager metaStoreManager,
      PolarisCallContext polarisCallContext,
      T entity,
      List<PolarisEntity> parentPath) {

    String location = entity.getBaseLocation();
    String name = entity.getName();

    boolean useOptimizedSiblingCheck =
        realmConfig.getConfig(FeatureConfiguration.OPTIMIZED_SIBLING_CHECK);
    if (useOptimizedSiblingCheck) {
      Optional<Optional<String>> result =
          metaStoreManager.hasOverlappingSiblings(polarisCallContext, entity);
      if (result.isPresent()) {
        if (result.get().isPresent()) {
          throw new ForbiddenException(
              "Unable to create entity at location '%s' because it conflicts with "
                  + "existing table or namespace at %s",
              location, result.get().get());
        }
        return;
      }
    }

    StorageLocation targetLocation = StorageLocation.of(location);
    var coreParentPath = PolarisEntity.toCoreList(parentPath);

    // If the parent path has more than just the catalog, check table-like siblings too
    if (parentPath.size() > 1) {
      ListEntitiesResult siblingTablesResult =
          metaStoreManager.listEntities(
              polarisCallContext,
              coreParentPath,
              PolarisEntityType.TABLE_LIKE,
              PolarisEntitySubType.ANY_SUBTYPE,
              PageToken.readEverything());
      if (siblingTablesResult.isSuccess() && siblingTablesResult.getEntities() != null) {
        for (EntityNameLookupRecord sibling : siblingTablesResult.getEntities()) {
          if (sibling.getName().equals(name)) {
            continue;
          }
          checkEntityLocationOverlap(
              metaStoreManager, polarisCallContext, sibling, targetLocation, location);
        }
      }
    }

    ListEntitiesResult siblingNamespacesResult =
        metaStoreManager.listEntities(
            polarisCallContext,
            coreParentPath,
            PolarisEntityType.NAMESPACE,
            PolarisEntitySubType.ANY_SUBTYPE,
            PageToken.readEverything());
    if (siblingNamespacesResult.isSuccess() && siblingNamespacesResult.getEntities() != null) {
      for (EntityNameLookupRecord sibling : siblingNamespacesResult.getEntities()) {
        if (sibling.getName().equals(name)) {
          continue;
        }
        checkEntityLocationOverlap(
            metaStoreManager, polarisCallContext, sibling, targetLocation, location);
      }
    }
  }

  private static void checkEntityLocationOverlap(
      PolarisMetaStoreManager metaStoreManager,
      PolarisCallContext polarisCallContext,
      EntityNameLookupRecord sibling,
      StorageLocation targetLocation,
      String location) {
    EntityResult loadResult =
        metaStoreManager.loadEntity(
            polarisCallContext,
            sibling.getCatalogId(),
            sibling.getId(),
            PolarisEntityType.fromCode(sibling.getTypeCode()));
    if (!loadResult.isSuccess() || loadResult.getEntity() == null) {
      return;
    }
    PolarisEntity siblingEntity = new PolarisEntity(loadResult.getEntity());
    PolarisEntityUtils.asLocationBasedEntity(siblingEntity)
        .map(LocationBasedEntity::getBaseLocation)
        .map(StorageLocation::of)
        .ifPresent(
            siblingLocation -> {
              if (targetLocation.isChildOf(siblingLocation)
                  || siblingLocation.isChildOf(targetLocation)) {
                throw new ForbiddenException(
                    "Unable to create entity at location '%s' because it conflicts with "
                        + "existing table or namespace at location '%s'",
                    location, siblingLocation);
              }
            });
  }
}
