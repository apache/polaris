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

import static java.util.Objects.requireNonNull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisEntityUtils;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.persistence.LocationOverlapResult;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.Resolvable;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.StorageUtil;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Checks a proposed storage location against the catalog's existing entities. */
public final class LocationOverlapValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocationOverlapValidator.class);

  private final CallContext callContext;
  private final ResolutionManifestFactory resolutionManifestFactory;
  private final PolarisPrincipal principal;

  public LocationOverlapValidator(
      CallContext callContext,
      ResolutionManifestFactory resolutionManifestFactory,
      PolarisPrincipal principal) {
    this.callContext = callContext;
    this.resolutionManifestFactory = resolutionManifestFactory;
    this.principal = principal;
  }

  public <T extends PolarisEntity & LocationBasedEntity> void validate(
      PolarisMetaStoreManager metaStoreManager,
      T entity,
      List<PolarisEntity> parentPath,
      @Nullable StorageLocation searchRoot) {

    String location = entity.getBaseLocation();
    String name = entity.getName();
    PolarisCallContext callCtx = callContext.getPolarisCallContext();
    RealmConfig realmConfig = callContext.getRealmConfig();
    StorageLocation targetLocation = StorageLocation.of(location);

    // Attempt to directly query for siblings
    if (realmConfig.getConfig(FeatureConfiguration.OPTIMIZED_SIBLING_CHECK)) {
      // The entity's own parents may contain its location without being reported.
      Set<Long> parentPathIds =
          parentPath.stream().map(PolarisEntity::getId).collect(Collectors.toSet());
      switch (metaStoreManager.hasOverlappingSiblings(callCtx, entity, parentPathIds, searchRoot)) {
        case LocationOverlapResult.Overlap overlap ->
            throw new ForbiddenException(
                "Unable to create entity at location '%s' because it conflicts with existing table or namespace at %s",
                location, overlap.conflictingLocation());
        case LocationOverlapResult.NoOverlap ignored -> {
          CatalogEntity catalog = requireNonNull(CatalogEntity.of(parentPath.getFirst()));
          validateGenericLocations(
              metaStoreManager, callCtx, catalog, entity.getId(), targetLocation, searchRoot);
          // List sibling write paths when they may extend outside the indexed base location.
          if (!realmConfig.getConfig(
                  FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION, catalog)
              && catalog.getCatalogType() != Catalog.TypeEnum.EXTERNAL) {
            return;
          }
        }
        case LocationOverlapResult.Unsupported ignored -> {
          // Continue with list-based overlap validation below.
        }
      }
    }

    // if the entity path has more than just the catalog, check for tables as well as other
    // namespaces
    Optional<NamespaceEntity> parentNamespace =
        parentPath.size() > 1
            ? Optional.of(NamespaceEntity.of(parentPath.getLast()))
            : Optional.empty();

    // Fall through by listing everything:
    ListEntitiesResult siblingNamespacesResult =
        metaStoreManager.listEntities(
            callCtx,
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
            .map(ns -> {
              ListEntitiesResult siblingTablesResult =
                  metaStoreManager.listEntities(
                      callCtx,
                      PolarisEntity.toCoreList(parentPath),
                      PolarisEntityType.TABLE_LIKE,
                      PolarisEntitySubType.ANY_SUBTYPE,
                      PageToken.readEverything());
              if (!siblingTablesResult.isSuccess()) {
                throw new IllegalStateException(
                    "Unable to resolve siblings entities to validate location - could not list tables");
              }
              return siblingTablesResult.getEntities().stream()
                  .map(tbl -> TableIdentifier.of(ns.asNamespace(), tbl.getName()))
                  .collect(Collectors.toList());
            })
            .orElse(List.of());

    List<Namespace> siblingNamespaces =
        siblingNamespacesResult.getEntities().stream()
            .map(ns -> {
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

    // Skip the entity itself, which is listed among its siblings when it is being updated. A table
    // and a namespace may share a name, so only skip a sibling of the same type as the entity.
    boolean skipSelfTable = entity.getType() == PolarisEntityType.TABLE_LIKE;
    for (TableIdentifier tbl : siblingTables) {
      if (skipSelfTable && tbl.name().equals(name)) {
        continue;
      }
      pathsToResolve.add(ResolvedPathKey.ofTableLike(tbl));
    }

    boolean skipSelfNamespace = entity.getType() == PolarisEntityType.NAMESPACE;
    for (Namespace ns : siblingNamespaces) {
      if (skipSelfNamespace && ns.level(ns.length() - 1).equals(name)) {
        continue;
      }
      pathsToResolve.add(ResolvedPathKey.ofNamespace(ns));
    }

    // A parent namespace naturally contains the entity's location when locations follow the
    // namespace tree, as default locations do. That nesting is allowed, but the entity may not sit
    // at exactly a containing namespace's location: such a location is a prefix of every sibling
    // created later under that namespace (blocking all of them) and over-scopes any credentials
    // vended for the entity to the whole namespace subtree. The optimized sibling check already
    // rejects this; mirror the exact-location case here so the default list-based path has the same
    // coverage. Only the equality case is rejected (and only against namespaces, not the catalog):
    // an entity may legitimately sit at the catalog's own base location, and strict containment in
    // either direction is handled by normal nesting and the sibling checks below.
    for (PolarisEntity ancestor : parentPath) {
      if (ancestor.getType() != PolarisEntityType.NAMESPACE) {
        continue;
      }
      PolarisEntityUtils.asLocationBasedEntity(ancestor)
          .map(LocationBasedEntity::getBaseLocation)
          .filter(loc -> loc != null && !loc.isBlank())
          .map(StorageLocation::of)
          .ifPresent(ancestorLocation -> {
            if (targetLocation.isSameLocation(ancestorLocation)) {
              throw new ForbiddenException(
                  "Unable to create entity at location '%s' because it conflicts with existing table or namespace at "
                      + "location '%s'",
                  targetLocation, ancestorLocation);
            }
          });
    }

    for (PolarisEntity entityToCheck :
        resolveOptionalPaths(pathsToResolve, parentPath.getFirst().getName())) {
      for (String siblingLocation : getLocationsUsedBySibling(entityToCheck)) {
        checkOverlap(targetLocation, StorageLocation.of(siblingLocation));
      }
    }
  }

  private static Set<String> getLocationsUsedBySibling(PolarisEntity sibling) {
    String baseLocation =
        PolarisEntityUtils.asLocationBasedEntity(sibling)
            .map(LocationBasedEntity::getBaseLocation)
            .orElse(null);
    if (baseLocation == null || baseLocation.isEmpty()) {
      return Set.of();
    }
    if (sibling.getSubType() == PolarisEntitySubType.ICEBERG_TABLE) {
      return StorageUtil.getLocationsUsedByTable(
          baseLocation, sibling.getInternalPropertiesAsMap());
    }
    return Set.of(baseLocation);
  }

  private static void checkOverlap(StorageLocation target, StorageLocation existing) {
    if (target.isChildOf(existing) || existing.isChildOf(target)) {
      throw new ForbiddenException(
          "Unable to create entity at location '%s' because it conflicts with existing table or namespace at location '%s'",
          target, existing);
    }
  }

  private static void validateGenericLocations(
      PolarisMetaStoreManager metaStoreManager,
      PolarisCallContext callCtx,
      PolarisEntity catalog,
      long entityId,
      StorageLocation target,
      @Nullable StorageLocation searchRoot) {
    if (searchRoot != null && !target.isChildOf(searchRoot) && !searchRoot.isChildOf(target)) {
      return;
    }

    // Generic Table base locations live in internal properties and are not indexed, even for new
    // tables.
    Deque<List<PolarisEntityCore>> pending = new ArrayDeque<>();
    pending.add(List.of(PolarisEntity.toCore(catalog)));
    while (!pending.isEmpty()) {
      List<PolarisEntityCore> path = pending.removeLast();
      for (PolarisBaseEntity table :
          metaStoreManager.listFullEntitiesAll(
              callCtx, path, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.GENERIC_TABLE)) {
        if (table.getId() == entityId) {
          continue;
        }
        String location = GenericTableEntity.of(table).getBaseLocation();
        if (location == null || location.isEmpty()) {
          continue;
        }
        StorageLocation existing = StorageLocation.of(location);
        if (searchRoot == null || existing.isChildOf(searchRoot)) {
          checkOverlap(target, existing);
        }
      }

      // A table may live outside its logical namespace's storage location. Only the namespace
      // identity is needed to visit its children.
      ListEntitiesResult childNamespaces =
          metaStoreManager.listEntities(
              callCtx,
              path,
              PolarisEntityType.NAMESPACE,
              PolarisEntitySubType.ANY_SUBTYPE,
              PageToken.readEverything());
      if (!childNamespaces.isSuccess()) {
        throw new IllegalStateException(
            "Unable to validate Generic Table locations - could not list namespaces: "
                + childNamespaces.getReturnStatus());
      }
      for (EntityNameLookupRecord namespace : childNamespaces.getEntities()) {
        List<PolarisEntityCore> childPath = new ArrayList<>(path);
        childPath.add(
            new PolarisEntityCore.Builder<>()
                .catalogId(namespace.getCatalogId())
                .id(namespace.getId())
                .parentId(namespace.getParentId())
                .typeCode(namespace.getTypeCode())
                .name(namespace.getName())
                .build());
        pending.add(childPath);
      }
    }
  }

  List<PolarisEntity> resolveOptionalPaths(List<ResolvedPathKey> keys, String catalogName) {
    LOGGER.debug("Resolving {} sibling entities to validate location", keys.size());

    PolarisResolutionManifest resolutionManifest =
        resolutionManifestFactory.createResolutionManifest(principal, catalogName);

    keys.forEach(k -> {
      resolutionManifest.addPath(new ResolverPath(k, true)); // optional path
    });

    ResolverStatus status =
        resolutionManifest.resolveSelections(
            Set.of(Resolvable.REFERENCE_CATALOG, Resolvable.REQUESTED_PATHS));

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
    keys.forEach(k -> {
      PolarisResolvedPathWrapper path = resolutionManifest.getResolvedPath(k);
      if (path != null) {
        PolarisEntity entity = path.getRawLeafEntity();
        if (entity != null) {
          result.add(entity);
        }
      }
    });

    return result;
  }
}
