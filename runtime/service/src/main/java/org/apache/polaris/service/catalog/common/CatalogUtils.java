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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisEntityUtils;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
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
   * Validate no location overlap exists between the entity path and its sibling entities. This
   * resolves all siblings at the same level as the target entity (namespaces if the target entity
   * is a namespace whose parent is the catalog, namespaces and tables otherwise) and checks the
   * base-location property of each. The target entity's base location may not be a prefix or a
   * suffix of any sibling entity's base location.
   */
  public static <T extends PolarisEntity & LocationBasedEntity> void validateNoLocationOverlap(
      RealmConfig realmConfig,
      PolarisMetaStoreManager metaStoreManager,
      PolarisCallContext polarisCallContext,
      ResolutionManifestFactory resolutionManifestFactory,
      PolarisPrincipal principal,
      T entity,
      List<PolarisEntity> parentPath) {

    String location = entity.getBaseLocation();
    String name = entity.getName();

    boolean useOptimizedSiblingCheck =
        realmConfig.getConfig(FeatureConfiguration.OPTIMIZED_SIBLING_CHECK);
    if (useOptimizedSiblingCheck) {
      Optional<Optional<String>> directSiblingCheckResult =
          metaStoreManager.hasOverlappingSiblings(polarisCallContext, entity);
      if (directSiblingCheckResult.isPresent()) {
        if (directSiblingCheckResult.get().isPresent()) {
          throw new ForbiddenException(
              "Unable to create entity at location '%s' because it conflicts with "
                  + "existing table or namespace at %s",
              location, directSiblingCheckResult.get().get());
        } else {
          return;
        }
      }
    }

    Optional<NamespaceEntity> parentNamespace =
        parentPath.size() > 1
            ? Optional.of(NamespaceEntity.of(parentPath.getLast()))
            : Optional.empty();

    ListEntitiesResult siblingNamespacesResult =
        metaStoreManager.listEntities(
            polarisCallContext,
            PolarisEntity.toCoreList(parentPath),
            PolarisEntityType.NAMESPACE,
            PolarisEntitySubType.ANY_SUBTYPE,
            PageToken.readEverything());
    if (!siblingNamespacesResult.isSuccess()) {
      throw new IllegalStateException(
          "Unable to resolve siblings entities to validate location - could not list namespaces");
    }

    List<TableIdentifier> siblingTables =
        parentNamespace
            .map(
                ns -> {
                  ListEntitiesResult siblingTablesResult =
                      metaStoreManager.listEntities(
                          polarisCallContext,
                          PolarisEntity.toCoreList(parentPath),
                          PolarisEntityType.TABLE_LIKE,
                          PolarisEntitySubType.ANY_SUBTYPE,
                          PageToken.readEverything());
                  if (!siblingTablesResult.isSuccess()) {
                    throw new IllegalStateException(
                        "Unable to resolve siblings entities to validate location"
                            + " - could not list tables");
                  }
                  return siblingTablesResult.getEntities().stream()
                      .map(tbl -> TableIdentifier.of(ns.asNamespace(), tbl.getName()))
                      .collect(Collectors.toList());
                })
            .orElse(List.of());

    List<Namespace> siblingNamespaces =
        siblingNamespacesResult.getEntities().stream()
            .map(
                ns -> {
                  String[] nsLevels =
                      parentNamespace
                          .map(parent -> parent.asNamespace().levels())
                          .orElse(new String[0]);
                  String[] newLevels = Arrays.copyOf(nsLevels, nsLevels.length + 1);
                  newLevels[nsLevels.length] = ns.getName();
                  return Namespace.of(newLevels);
                })
            .toList();
    List<ResolvedPathKey> pathsToResolve =
        new ArrayList<>(siblingTables.size() + siblingNamespaces.size());
    siblingTables.forEach(
        tbl -> {
          if (!tbl.name().equals(name)) {
            pathsToResolve.add(ResolvedPathKey.ofTableLike(tbl));
          }
        });
    siblingNamespaces.forEach(
        ns -> {
          if (!ns.level(ns.length() - 1).equals(name)) {
            pathsToResolve.add(ResolvedPathKey.ofNamespace(ns));
          }
        });

    StorageLocation targetLocation = StorageLocation.of(location);
    for (PolarisEntity entityToCheck :
        resolveOptionalPaths(
            resolutionManifestFactory,
            principal,
            pathsToResolve,
            parentPath.getFirst().getName())) {
      PolarisEntityUtils.asLocationBasedEntity(entityToCheck)
          .map(LocationBasedEntity::getBaseLocation)
          .map(StorageLocation::of)
          .ifPresent(
              siblingLocation -> {
                if (targetLocation.isChildOf(siblingLocation)
                    || siblingLocation.isChildOf(targetLocation)) {
                  throw new ForbiddenException(
                      "Unable to create entity at location '%s' because it conflicts with "
                          + "existing table or namespace at location '%s'",
                      targetLocation, siblingLocation);
                }
              });
    }
  }

  static List<PolarisEntity> resolveOptionalPaths(
      ResolutionManifestFactory resolutionManifestFactory,
      PolarisPrincipal principal,
      List<ResolvedPathKey> keys,
      String catalogName) {
    LOGGER.debug("Resolving {} sibling entities to validate location", keys.size());

    PolarisResolutionManifest resolutionManifest =
        resolutionManifestFactory.createResolutionManifest(principal, catalogName);

    keys.forEach(k -> resolutionManifest.addPath(new ResolverPath(k, true)));

    ResolverStatus status = resolutionManifest.resolveAll();

    if (status.getStatus() != ResolverStatus.StatusEnum.SUCCESS) {
      String message =
          "Unable to resolve sibling entities to validate location - " + status.getStatus();
      if (status.getStatus().equals(ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED)) {
        message += ". Could not resolve entity: " + status.getFailedToResolvedEntityName();
      } else if (status
          .getStatus()
          .equals(ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED)) {
        ResolverPath path = status.getFailedToResolvePath();
        if (path != null) {
          message += ". path: " + String.join(".", path.entityNames());
          message += ", failed index: " + status.getFailedToResolvedEntityIndex();
        }
      }

      throw new CommitConflictException(message);
    }

    List<PolarisEntity> result = new ArrayList<>(keys.size());
    keys.forEach(
        k -> {
          PolarisResolvedPathWrapper path = resolutionManifest.getResolvedPath(k);
          if (path != null) {
            PolarisEntity resolvedEntity = path.getRawLeafEntity();
            if (resolvedEntity != null) {
              result.add(resolvedEntity);
            }
          }
        });

    return result;
  }
}
