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
package org.apache.polaris.core.persistence.resolver;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds a collection of related resolved PolarisEntity and associated grants including caller
 * Principal/PrincipalRoles/CatalogRoles and target securables that will participate in any given
 * operation.
 *
 * <p>Implemented as a wrapper around a Resolver with helper methods and book-keeping to better
 * function as a lookup manifest for downstream callers.
 */
public class PolarisResolutionManifest implements PolarisResolutionManifestCatalogView {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisResolutionManifest.class);

  private final ResolverFactory resolverFactory;
  private final PolarisPrincipal principal;
  private final RealmContext realmContext;
  private final String catalogName;
  private final Resolver primaryResolver;
  private final PolarisDiagnostics diagnostics;

  private final Map<ResolvedPathKey, Integer> pathLookup = new HashMap<>();
  private final List<ResolverPath> addedPaths = new ArrayList<>();
  private final Multimap<String, PolarisEntityType> addedTopLevelNames = HashMultimap.create();

  private final Map<ResolvedPathKey, ResolverPath> passthroughPaths = new HashMap<>();

  private int currentPathIndex = 0;

  // Set when resolveAll is called
  private ResolverStatus primaryResolverStatus = null;

  private boolean isResolveAllSucceeded() {
    diagnostics.checkNotNull(
        primaryResolverStatus,
        "resolver_not_run_before_access",
        "resolveAll() must be called before reading resolution results");
    return primaryResolverStatus.getStatus() == ResolverStatus.StatusEnum.SUCCESS;
  }

  public PolarisResolutionManifest(
      PolarisDiagnostics diagnostics,
      RealmContext realmContext,
      ResolverFactory resolverFactory,
      PolarisPrincipal principal,
      String catalogName) {
    this.realmContext = realmContext;
    this.resolverFactory = resolverFactory;
    this.catalogName = catalogName;
    this.diagnostics = diagnostics;
    this.diagnostics.checkNotNull(principal, "null_principal_for_resolution_manifest");
    this.principal = principal;
    this.primaryResolver = resolverFactory.createResolver(principal, catalogName);

    // TODO: Make the rootContainer lookup no longer optional in the persistence store.
    // For now, we'll try to resolve the rootContainer as "optional", and only if we fail to find
    // it, we'll use the "simulated" rootContainer entity.
    addTopLevelName(PolarisEntityConstants.getRootContainerName(), PolarisEntityType.ROOT, true);
  }

  /**
   * Adds a named entity to be resolved via the resolver's name-based registration surface.
   *
   * <p>This includes top-level entities (Catalog, Principal, PrincipalRole), and also {@code
   * CATALOG_ROLE}. For {@code CATALOG_ROLE}, a reference catalog must be present on the
   * manifest/resolver context.
   */
  public void addTopLevelName(String entityName, PolarisEntityType entityType, boolean isOptional) {
    addedTopLevelNames.put(entityName, entityType);
    if (isOptional) {
      primaryResolver.addOptionalEntityByName(entityType, entityName);
    } else {
      primaryResolver.addEntityByName(entityType, entityName);
    }
  }

  /** Adds a statically resolved path using canonical registration semantics only. */
  public void addPath(ResolverPath path) {
    primaryResolver.addPath(path);
    // Preserve prior manifest lookup behavior: re-registering the same lookup key overwrites the
    // previous mapping (last-write-wins).
    pathLookup.put(ResolvedPathKey.of(path), currentPathIndex);
    addedPaths.add(path);
    ++currentPathIndex;
  }

  /** Adds a passthrough path using canonical registration semantics only. */
  public void addPassthroughPath(ResolverPath path) {
    addPath(path);
    passthroughPaths.put(ResolvedPathKey.of(path), path);
  }

  public ResolverStatus resolveAll() {
    primaryResolverStatus = primaryResolver.resolveAll();
    // TODO: This could be a race condition where a Principal is dropped after initial authn
    // but before the resolution attempt; consider whether 403 forbidden is more appropriate.
    diagnostics.check(
        primaryResolverStatus.getStatus()
            != ResolverStatus.StatusEnum.CALLER_PRINCIPAL_DOES_NOT_EXIST,
        "caller_principal_does_not_exist_at_resolution_time");

    return primaryResolverStatus;
  }

  /**
   * Resolves explicitly requested components.
   *
   * <p>Selections control which resolver components are executed. Callers are expected to add paths
   * or top-level entity names before invoking this method.
   */
  public ResolverStatus resolveSelections(Set<Resolvable> selections) {
    diagnostics.checkNotNull(selections, "resolver_selections_is_null");
    primaryResolverStatus = primaryResolver.resolveSelections(selections);
    diagnostics.check(
        primaryResolverStatus.getStatus()
            != ResolverStatus.StatusEnum.CALLER_PRINCIPAL_DOES_NOT_EXIST,
        "caller_principal_does_not_exist_at_resolution_time");
    return primaryResolverStatus;
  }

  public boolean getIsPassthroughFacade() {
    return primaryResolver.getIsPassthroughFacade();
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedReferenceCatalogEntity() {
    return getResolvedReferenceCatalogEntity(false);
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(ResolvedPathKey key) {
    return getResolvedPath(key, false);
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(
      ResolvedPathKey key, PolarisEntitySubType subType) {
    return getResolvedPath(key, subType, false);
  }

  @Override
  public PolarisResolvedPathWrapper getPassthroughResolvedPath(ResolvedPathKey key) {
    ResolverPath requestedPath = passthroughPaths.get(key);
    diagnostics.check(
        requestedPath != null,
        "invalid_path_key_for_passthrough_resolved_path",
        "key={} passthroughPaths={}",
        key,
        passthroughPaths);

    // Run a single-use Resolver for this path.
    Resolver passthroughResolver = resolverFactory.createResolver(principal, catalogName);
    passthroughResolver.addPath(requestedPath);
    ResolverStatus status = passthroughResolver.resolveAll();

    if (status.getStatus() != ResolverStatus.StatusEnum.SUCCESS) {
      LOGGER.debug(
          "Returning null for path {} due to resolver status {}",
          requestedPath,
          status.getStatus());
      return null;
    }

    List<ResolvedPolarisEntity> resolvedPath = passthroughResolver.getResolvedPath();
    if (requestedPath.optional() && !getIsPassthroughFacade()) {
      if (resolvedPath.size() != requestedPath.entityNames().size()) {
        LOGGER.debug(
            "Returning null due to size mismatch from getPassthroughResolvedPath "
                + "resolvedPath: {}, requestedPath.getEntityNames(): {}",
            resolvedPath,
            requestedPath.entityNames());
        return null;
      }
    }

    List<ResolvedPolarisEntity> resolvedEntities = new ArrayList<>();
    resolvedEntities.add(passthroughResolver.getResolvedReferenceCatalog());
    resolvedPath.forEach(resolvedEntity -> resolvedEntities.add(resolvedEntity));
    LOGGER.debug(
        "Returning resolvedEntities from getPassthroughResolvedPath: {}", resolvedEntities);
    return new PolarisResolvedPathWrapper(resolvedEntities);
  }

  @Override
  public PolarisResolvedPathWrapper getPassthroughResolvedPath(
      ResolvedPathKey key, PolarisEntitySubType subType) {
    PolarisResolvedPathWrapper resolvedPath = getPassthroughResolvedPath(key);
    if (resolvedPath == null) {
      return null;
    }
    if (resolvedPath.getRawLeafEntity() != null
        && subType != PolarisEntitySubType.ANY_SUBTYPE
        && resolvedPath.getRawLeafEntity().getSubType() != subType) {
      return null;
    }
    return resolvedPath;
  }

  public Set<PolarisBaseEntity> getAllActivatedCatalogRoleAndPrincipalRoles() {
    Set<PolarisBaseEntity> activatedRoles = new HashSet<>();
    primaryResolver.getResolvedCallerPrincipalRoles().stream()
        .map(ResolvedPolarisEntity::getEntity)
        .forEach(activatedRoles::add);
    if (primaryResolver.getResolvedCatalogRoles() != null) {
      primaryResolver.getResolvedCatalogRoles().values().stream()
          .map(ResolvedPolarisEntity::getEntity)
          .forEach(activatedRoles::add);
    }
    return activatedRoles;
  }

  public Set<PolarisBaseEntity> getAllActivatedPrincipalRoleEntities() {
    Set<PolarisBaseEntity> activatedEntities = new HashSet<>();
    primaryResolver.getResolvedCallerPrincipalRoles().stream()
        .map(ResolvedPolarisEntity::getEntity)
        .forEach(activatedEntities::add);
    return activatedEntities;
  }

  private @Nullable ResolvedPolarisEntity getResolvedRootContainerEntity() {
    if (!isResolveAllSucceeded()) {
      return null;
    }
    ResolvedPolarisEntity resolvedEntity =
        primaryResolver.getResolvedEntity(
            PolarisEntityType.ROOT, PolarisEntityConstants.getRootContainerName());
    if (resolvedEntity == null) {
      LOGGER.warn(
          "Failed to find rootContainer for realm: {} and catalog: {}",
          realmContext.getRealmIdentifier(),
          catalogName);
    }
    return resolvedEntity;
  }

  public PolarisResolvedPathWrapper getResolvedRootContainerEntityAsPath() {
    return new PolarisResolvedPathWrapper(List.of(getResolvedRootContainerEntity()));
  }

  public PolarisResolvedPathWrapper getResolvedReferenceCatalogEntity(
      boolean prependRootContainer) {
    // This is a server error instead of being able to legitimately return null, since this means
    // a callsite failed to incorporate a reference catalog into its authorization flow but is
    // still trying to perform operations on the (nonexistence) reference catalog.
    diagnostics.checkNotNull(catalogName, "null_catalog_name_for_resolved_reference_catalog");
    ResolvedPolarisEntity resolvedReferenceCatalog = primaryResolver.getResolvedReferenceCatalog();
    if (resolvedReferenceCatalog == null) {
      return null;
    }
    if (prependRootContainer) {
      // Operations directly on Catalogs also consider the root container to be a parent of its
      // authorization chain.
      // TODO: Throw appropriate Catalog NOT_FOUND exception before any call to
      // getResolvedReferenceCatalogEntity().
      return new PolarisResolvedPathWrapper(
          List.of(getResolvedRootContainerEntity(), resolvedReferenceCatalog));
    } else {
      return new PolarisResolvedPathWrapper(List.of(resolvedReferenceCatalog));
    }
  }

  public PolarisEntitySubType getLeafSubType(ResolvedPathKey key) {
    diagnostics.check(
        pathLookup.containsKey(key),
        "never_registered_key_for_resolved_path",
        "key={} pathLookup={}",
        key,
        pathLookup);
    int index = pathLookup.get(key);
    List<ResolvedPolarisEntity> resolved = primaryResolver.getResolvedPaths().get(index);
    if (resolved.isEmpty()) {
      return PolarisEntitySubType.NULL_SUBTYPE;
    }
    return resolved.get(resolved.size() - 1).getEntity().getSubType();
  }

  /** Resolves a registered path using a canonical lookup key. */
  public PolarisResolvedPathWrapper getResolvedPath(
      ResolvedPathKey key, boolean prependRootContainer) {
    diagnostics.check(
        pathLookup.containsKey(key),
        "never_registered_key_for_resolved_path",
        "key={} pathLookup={}",
        key,
        pathLookup);
    int index = pathLookup.get(key);
    if (!isResolveAllSucceeded()) {
      return null;
    }

    // Return null for a partially-resolved "optional" path.
    ResolverPath requestedPath = addedPaths.get(index);
    List<ResolvedPolarisEntity> resolvedPath = primaryResolver.getResolvedPaths().get(index);
    // If the catalog is a passthrough facade, we can go ahead and just return only as much of
    // the parent path as was successfully found.
    // TODO: For passthrough facade semantics, consider whether this should be where we generate
    // the JIT-created entities that would get committed after we find them in the remote
    // catalog.
    if (requestedPath.optional() && !getIsPassthroughFacade()) {
      if (resolvedPath.size() != requestedPath.entityNames().size()) {
        return null;
      }
    }

    List<ResolvedPolarisEntity> resolvedEntities = new ArrayList<>();
    if (prependRootContainer) {
      resolvedEntities.add(getResolvedRootContainerEntity());
    }
    resolvedEntities.add(primaryResolver.getResolvedReferenceCatalog());
    resolvedPath.forEach(resolvedEntity -> resolvedEntities.add(resolvedEntity));
    return new PolarisResolvedPathWrapper(resolvedEntities);
  }

  public PolarisResolvedPathWrapper getResolvedPath(
      ResolvedPathKey key, PolarisEntitySubType subType, boolean prependRootContainer) {
    PolarisResolvedPathWrapper resolvedPath = getResolvedPath(key, prependRootContainer);
    if (resolvedPath == null) {
      return null;
    }
    if (!getIsPassthroughFacade()
        && resolvedPath.getRawLeafEntity() != null
        && subType != PolarisEntitySubType.ANY_SUBTYPE
        && resolvedPath.getRawLeafEntity().getSubType() != subType) {
      return null;
    }
    return resolvedPath;
  }

  public PolarisResolvedPathWrapper getResolvedTopLevelEntity(
      String entityName, PolarisEntityType entityType) {
    // For now, all top-level entities will have the root container prepended so we don't have
    // a variation of this method that allows specifying whether to prepend the root container.
    diagnostics.check(
        addedTopLevelNames.containsEntry(entityName, entityType),
        "never_registered_top_level_name_and_type_for_resolved_entity",
        "entityName={} entityType={} addedTopLevelNames={}",
        entityName,
        entityType,
        addedTopLevelNames);

    if (!isResolveAllSucceeded()) {
      return null;
    }

    ResolvedPolarisEntity resolvedEntity =
        primaryResolver.getResolvedEntity(entityType, entityName);
    if (resolvedEntity == null) {
      return null;
    }

    ResolvedPolarisEntity resolvedRootContainerEntity = getResolvedRootContainerEntity();
    return resolvedRootContainerEntity == null
        ? new PolarisResolvedPathWrapper(List.of(resolvedEntity))
        : new PolarisResolvedPathWrapper(List.of(resolvedRootContainerEntity, resolvedEntity));
  }
}
