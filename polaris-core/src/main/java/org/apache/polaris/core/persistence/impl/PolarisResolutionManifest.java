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
package org.apache.polaris.core.persistence.impl;

import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.cache.EntityCacheEntry;
import org.apache.polaris.core.persistence.resolution.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolution.ResolutionManifest;
import org.apache.polaris.core.persistence.resolution.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolverBuilder;
import org.apache.polaris.core.persistence.resolver.ResolverException;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
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
final class PolarisResolutionManifest implements ResolutionManifest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisResolutionManifest.class);

  private final Supplier<ResolverBuilder> resolverSupplier;
  private final String catalogName;
  private final Resolver primaryResolver;
  private final PolarisDiagnostics diagnostics;

  private final Map<Object, Integer> pathLookup;
  private final List<ResolverPath> addedPaths;
  private final Multimap<String, PolarisEntityType> addedTopLevelNames;
  private final Map<Object, ResolverPath> passthroughPaths;

  // For applicable operations, this represents the topmost root entity which services as an
  // authorization parent for all other entities that reside at the root level, such as
  // Catalog, Principal, and PrincipalRole.
  // This simulated entity will be used if the actual resolver fails to resolve the rootContainer
  // on the backend due to compatibility mismatches.
  private final ResolvedPolarisEntity rootContainerEntity;

  PolarisResolutionManifest(
      Supplier<ResolverBuilder> resolverSupplier,
      String catalogName,
      Resolver primaryResolver,
      PolarisDiagnostics diagnostics,
      Map<Object, Integer> pathLookup,
      List<ResolverPath> addedPaths,
      Multimap<String, PolarisEntityType> addedTopLevelNames,
      Map<Object, ResolverPath> passthroughPaths,
      ResolvedPolarisEntity rootContainerEntity) {
    this.resolverSupplier = resolverSupplier;
    this.catalogName = catalogName;
    this.primaryResolver = primaryResolver;
    this.diagnostics = diagnostics;
    this.pathLookup = pathLookup;
    this.addedPaths = addedPaths;
    this.addedTopLevelNames = addedTopLevelNames;
    this.passthroughPaths = passthroughPaths;
    this.rootContainerEntity = rootContainerEntity;
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedReferenceCatalogEntity() {
    // This is a server error instead of being able to legitimately return null, since this means
    // a callsite failed to incorporate a reference catalog into its authorization flow but is
    // still trying to perform operations on the (nonexistence) reference catalog.
    diagnostics.checkNotNull(catalogName, "null_catalog_name_for_resolved_reference_catalog");
    EntityCacheEntry resolvedCachedCatalog = primaryResolver.getResolvedReferenceCatalog();
    if (resolvedCachedCatalog == null) {
      return null;
    }
    return new PolarisResolvedPathWrapper(
        List.of(new ResolvedPolarisEntity(resolvedCachedCatalog)));
  }

  /**
   * @param key the key associated with the path to retrieve that was specified in addPath
   * @return null if the path resolved for {@code key} isn't fully-resolved when specified as
   *     "optional"
   */
  @Override
  public PolarisResolvedPathWrapper getPassthroughResolvedPath(Namespace key) {
    return getPassthroughResolvedPathInternal(key);
  }

  private PolarisResolvedPathWrapper getPassthroughResolvedPathInternal(Object key) {
    diagnostics.check(
        passthroughPaths.containsKey(key),
        "invalid_key_for_passthrough_resolved_path",
        "key={} passthroughPaths={}",
        key,
        passthroughPaths);
    ResolverPath requestedPath = passthroughPaths.get(key);

    // Run a single-use Resolver for this path.
    Resolver passthroughResolver;
    try {
      passthroughResolver = resolverSupplier.get().addPath(requestedPath).buildResolved();
    } catch (ResolverException e) {
      LOGGER.debug(
          "Returning null for key {} due to resolver status {}", key, e.getClass().getSimpleName());
      return null;
    }

    List<EntityCacheEntry> resolvedPath = passthroughResolver.getResolvedPath();
    if (requestedPath.isOptional()) {
      if (resolvedPath.size() != requestedPath.getEntityNames().size()) {
        LOGGER.debug(
            "Returning null for key {} due to size mismatch from getPassthroughResolvedPath "
                + "resolvedPath: {}, requestedPath.getEntityNames(): {}",
            key,
            resolvedPath.stream().map(ResolvedPolarisEntity::new).collect(Collectors.toList()),
            requestedPath.getEntityNames());
        return null;
      }
    }

    List<ResolvedPolarisEntity> resolvedEntities = new ArrayList<>();
    resolvedEntities.add(
        new ResolvedPolarisEntity(passthroughResolver.getResolvedReferenceCatalog()));
    resolvedPath.forEach(cacheEntry -> resolvedEntities.add(new ResolvedPolarisEntity(cacheEntry)));
    LOGGER.debug(
        "Returning resolvedEntities from getPassthroughResolvedPath: {}", resolvedEntities);
    return new PolarisResolvedPathWrapper(resolvedEntities);
  }

  /**
   * @return null if the path resolved for {@code key} isn't fully-resolved when specified as
   *     "optional", or if it was resolved but the subType doesn't match the specified subType.
   */
  @Override
  public PolarisResolvedPathWrapper getPassthroughResolvedPath(
      TableIdentifier key, PolarisEntitySubType subType) {
    PolarisResolvedPathWrapper resolvedPath = getPassthroughResolvedPathInternal(key);
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

  @Override
  public Set<PolarisBaseEntity> getAllActivatedCatalogRoleAndPrincipalRoles() {
    Set<PolarisBaseEntity> activatedRoles = new HashSet<>();
    primaryResolver.getResolvedCallerPrincipalRoles().stream()
        .map(EntityCacheEntry::getEntity)
        .forEach(activatedRoles::add);
    if (primaryResolver.getResolvedCatalogRoles() != null) {
      primaryResolver.getResolvedCatalogRoles().values().stream()
          .map(EntityCacheEntry::getEntity)
          .forEach(activatedRoles::add);
    }
    return activatedRoles;
  }

  @Override
  public Set<PolarisBaseEntity> getAllActivatedPrincipalRoleEntities() {
    Set<PolarisBaseEntity> activatedEntities = new HashSet<>();
    primaryResolver.getResolvedCallerPrincipalRoles().stream()
        .map(EntityCacheEntry::getEntity)
        .forEach(activatedEntities::add);
    return activatedEntities;
  }

  private ResolvedPolarisEntity getResolvedRootContainerEntity() {
    EntityCacheEntry resolvedCacheEntry =
        primaryResolver.getResolvedEntity(
            PolarisEntityType.ROOT, PolarisEntityConstants.getRootContainerName());
    if (resolvedCacheEntry == null) {
      LOGGER.debug("Failed to find rootContainer, so using simulated rootContainer instead.");
      return rootContainerEntity;
    }
    return new ResolvedPolarisEntity(resolvedCacheEntry);
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedRootContainerEntityAsPath() {
    return new PolarisResolvedPathWrapper(List.of(getResolvedRootContainerEntity()));
  }

  @Override
  public PolarisEntitySubType getLeafSubType(TableIdentifier tableIdentifier) {
    diagnostics.check(
        pathLookup.containsKey(tableIdentifier),
        "never_registered_key_for_resolved_path",
        "key={} pathLookup={}",
        tableIdentifier,
        pathLookup);
    int index = pathLookup.get(tableIdentifier);
    List<EntityCacheEntry> resolved = primaryResolver.getResolvedPaths().get(index);
    if (resolved.isEmpty()) {
      return PolarisEntitySubType.NULL_SUBTYPE;
    }
    return resolved.get(resolved.size() - 1).getEntity().getSubType();
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(
      Namespace namespace, boolean prependRootContainer) {
    return getResolvedPathInternal(namespace, prependRootContainer);
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(
      TableIdentifier tableIdentifier, boolean prependRootContainer) {
    return getResolvedPathInternal(tableIdentifier, prependRootContainer);
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(
      TableIdentifier tableIdentifier, PolarisEntitySubType subType, boolean prependRootContainer) {
    return getResolvedPathInternal(tableIdentifier, prependRootContainer);
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(String name, boolean prependRootContainer) {
    return getResolvedPathInternal(name, prependRootContainer);
  }

  private PolarisResolvedPathWrapper getResolvedPathInternal(
      Object key, boolean prependRootContainer) {
    diagnostics.check(
        pathLookup.containsKey(key),
        "never_registered_key_for_resolved_path",
        "key={} pathLookup={}",
        key,
        pathLookup);

    int index = pathLookup.get(key);

    // Return null for a partially-resolved "optional" path.
    ResolverPath requestedPath = addedPaths.get(index);
    List<EntityCacheEntry> resolvedPath = primaryResolver.getResolvedPaths().get(index);
    if (requestedPath.isOptional()) {
      if (resolvedPath.size() != requestedPath.getEntityNames().size()) {
        return null;
      }
    }

    List<ResolvedPolarisEntity> resolvedEntities = new ArrayList<>();
    if (prependRootContainer) {
      resolvedEntities.add(getResolvedRootContainerEntity());
    }
    resolvedEntities.add(new ResolvedPolarisEntity(primaryResolver.getResolvedReferenceCatalog()));
    resolvedPath.forEach(cacheEntry -> resolvedEntities.add(new ResolvedPolarisEntity(cacheEntry)));
    return new PolarisResolvedPathWrapper(resolvedEntities);
  }

  @Override
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

    EntityCacheEntry resolvedCacheEntry = primaryResolver.getResolvedEntity(entityType, entityName);
    if (resolvedCacheEntry == null) {
      return null;
    }

    ResolvedPolarisEntity resolvedRootContainerEntity = getResolvedRootContainerEntity();
    return resolvedRootContainerEntity == null
        ? new PolarisResolvedPathWrapper(List.of(new ResolvedPolarisEntity(resolvedCacheEntry)))
        : new PolarisResolvedPathWrapper(
            List.of(resolvedRootContainerEntity, new ResolvedPolarisEntity(resolvedCacheEntry)));
  }
}
