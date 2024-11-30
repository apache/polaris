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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.cache.EntityCacheEntry;
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

  private final PolarisEntityManager entityManager;
  private final CallContext callContext;
  private final AuthenticatedPolarisPrincipal authenticatedPrincipal;
  private final String catalogName;
  private final Resolver primaryResolver;
  private final PolarisDiagnostics diagnostics;

  private final Map<Object, Integer> pathLookup = new HashMap<>();
  private final List<ResolverPath> addedPaths = new ArrayList<>();
  private final Multimap<String, PolarisEntityType> addedTopLevelNames = HashMultimap.create();

  private final Map<Object, ResolverPath> passthroughPaths = new HashMap<>();
  private final List<Getter> primaryTargets = new ArrayList<>();
  private final List<Getter> secondaryTargets = new ArrayList<>();

  // For applicable operations, this represents the topmost root entity which services as an
  // authorization parent for all other entities that reside at the root level, such as
  // Catalog, Principal, and PrincipalRole.
  // This simulated entity will be used if the actual resolver fails to resolve the rootContainer
  // on the backend due to compatibility mismatches.
  private ResolvedPolarisEntity simulatedResolvedRootContainerEntity = null;

  private int currentPathIndex = 0;

  // Set when resolveAll is called
  private ResolverStatus primaryResolverStatus = null;

  public PolarisResolutionManifest(
      CallContext callContext,
      PolarisEntityManager entityManager,
      AuthenticatedPolarisPrincipal authenticatedPrincipal,
      String catalogName) {
    this.entityManager = entityManager;
    this.callContext = callContext;
    this.authenticatedPrincipal = authenticatedPrincipal;
    this.catalogName = catalogName;
    this.primaryResolver =
        entityManager.prepareResolver(callContext, authenticatedPrincipal, catalogName);
    this.diagnostics = callContext.getPolarisCallContext().getDiagServices();

    // TODO: Make the rootContainer lookup no longer optional in the persistence store.
    // For now, we'll try to resolve the rootContainer as "optional", and only if we fail to find
    // it, we'll use the "simulated" rootContainer entity.
    addTopLevelName(PolarisEntityConstants.getRootContainerName(), PolarisEntityType.ROOT, true);
  }

  public List<PolarisResolvedPathWrapper> authorizationTargets() {
    return primaryTargets.stream().map(g -> g.get(this)).collect(Collectors.toList());
  }

  public List<PolarisResolvedPathWrapper> authorizationSecondaries() {
    return secondaryTargets.stream().map(g -> g.get(this)).collect(Collectors.toList());
  }

  public void addRootContainer(AuthorizationTargetType targetType) {
    targetType.addTo(this, Getter.rootContainer());
  }

  /** Adds a name of a top-level entity (Catalog, Principal, PrincipalRole) to be resolved. */
  public void addTopLevelName(String entityName, PolarisEntityType entityType, boolean isOptional) {
    addedTopLevelNames.put(entityName, entityType);
    if (isOptional) {
      primaryResolver.addOptionalEntityByName(entityType, entityName);
    } else {
      primaryResolver.addEntityByName(entityType, entityName);
    }
  }

  public void addTopLevelName(
      AuthorizationTargetType targetType,
      String entityName,
      PolarisEntityType entityType,
      boolean isOptional) {
    addTopLevelName(entityName, entityType, isOptional);
    targetType.addTo(this, Getter.topLevelEntity(entityName, entityType));
  }

  /**
   * Adds a path that will be statically resolved with the primary Resolver when resolveAll() is
   * called, and which contributes to the resolution status of whether all paths have successfully
   * resolved.
   *
   * @param key the friendly lookup key for retrieving resolvedPaths after resolveAll(); typically
   *     might be a Namespace or TableIdentifier object.
   */
  public void addPath(ResolverPath path, Object key) {
    primaryResolver.addPath(path);
    pathLookup.put(key, currentPathIndex);
    addedPaths.add(path);
    ++currentPathIndex;
  }

  public void addPath(AuthorizationTargetType targetType, ResolverPath path, Object key) {
    addPath(path, key);
    targetType.addTo(this, Getter.path(key));
  }

  public void addPath(
      AuthorizationTargetType targetType, ResolverPath path, Object key, Runnable notFoundHandler) {
    addPath(path, key);
    targetType.addTo(this, Getter.path(key, notFoundHandler));
  }

  public void addPath(
      AuthorizationTargetType targetType,
      ResolverPath path,
      Object key,
      PolarisEntitySubType subType) {
    addPath(path, key);
    targetType.addTo(this, Getter.path(key, subType));
  }

  /**
   * Adds a path that is allowed to be dynamically resolved with a new Resolver when
   * getPassthroughResolvedPath is called. These paths are also included in the primary static
   * resolution set resolved during resolveAll().
   */
  public void addPassthroughPath(ResolverPath path, Object key) {
    addPath(path, key);
    passthroughPaths.put(key, path);
  }

  public void addPassthroughPath(
      AuthorizationTargetType targetType,
      ResolverPath path,
      Object key,
      PolarisEntitySubType subType,
      Runnable notFoundHandler) {
    addPassthroughPath(path, key);
    targetType.addTo(this, Getter.path(key, subType, notFoundHandler));
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

  @Override
  public PolarisResolvedPathWrapper getResolvedReferenceCatalogEntity() {
    return getResolvedReferenceCatalogEntity(false);
  }

  /**
   * @param key the key associated with the path to retrieve that was specified in addPath
   * @return null if the path resolved for {@code key} isn't fully-resolved when specified as
   *     "optional"
   */
  @Override
  public PolarisResolvedPathWrapper getResolvedPath(Object key) {
    return getResolvedPath(key, false);
  }

  /**
   * @return null if the path resolved for {@code key} isn't fully-resolved when specified as
   *     "optional", or if it was resolved but the subType doesn't match the specified subType.
   */
  @Override
  public PolarisResolvedPathWrapper getResolvedPath(Object key, PolarisEntitySubType subType) {
    return getResolvedPath(key, subType, false);
  }

  /**
   * @param key the key associated with the path to retrieve that was specified in addPath
   * @return null if the path resolved for {@code key} isn't fully-resolved when specified as
   *     "optional"
   */
  @Override
  public PolarisResolvedPathWrapper getPassthroughResolvedPath(Object key) {
    diagnostics.check(
        passthroughPaths.containsKey(key),
        "invalid_key_for_passthrough_resolved_path",
        "key={} passthroughPaths={}",
        key,
        passthroughPaths);
    ResolverPath requestedPath = passthroughPaths.get(key);

    // Run a single-use Resolver for this path.
    Resolver passthroughResolver =
        entityManager.prepareResolver(callContext, authenticatedPrincipal, catalogName);
    passthroughResolver.addPath(requestedPath);
    ResolverStatus status = passthroughResolver.resolveAll();

    if (status.getStatus() != ResolverStatus.StatusEnum.SUCCESS) {
      LOGGER.debug("Returning null for key {} due to resolver status {}", key, status.getStatus());
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
      Object key, PolarisEntitySubType subType) {
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
        .map(EntityCacheEntry::getEntity)
        .forEach(activatedRoles::add);
    if (primaryResolver.getResolvedCatalogRoles() != null) {
      primaryResolver.getResolvedCatalogRoles().values().stream()
          .map(EntityCacheEntry::getEntity)
          .forEach(activatedRoles::add);
    }
    return activatedRoles;
  }

  public AuthenticatedPolarisPrincipal getAuthenticatedPrincipal() {
    return authenticatedPrincipal;
  }

  @Nullable
  public PolarisEntity getResolvedPolarisPrincipal() {
    EntityCacheEntry entry = primaryResolver.getResolvedCallerPrincipal();
    return entry == null ? null : PolarisEntity.of(entry.getEntity());
  }

  @Nonnull
  public List<String> getResolvedPolarisPrincipalRoleNames() {
    return primaryResolver.getResolvedCallerPrincipalRoles().stream()
        .map(ce -> ce.getEntity().getName())
        .collect(Collectors.toList());
  }

  public Set<PolarisBaseEntity> getAllActivatedPrincipalRoleEntities() {
    Set<PolarisBaseEntity> activatedEntities = new HashSet<>();
    primaryResolver.getResolvedCallerPrincipalRoles().stream()
        .map(EntityCacheEntry::getEntity)
        .forEach(activatedEntities::add);
    return activatedEntities;
  }

  public void setSimulatedResolvedRootContainerEntity(
      ResolvedPolarisEntity simulatedResolvedRootContainerEntity) {
    this.simulatedResolvedRootContainerEntity = simulatedResolvedRootContainerEntity;
  }

  private ResolvedPolarisEntity getResolvedRootContainerEntity() {
    if (primaryResolverStatus.getStatus() != ResolverStatus.StatusEnum.SUCCESS) {
      return null;
    }
    EntityCacheEntry resolvedCacheEntry =
        primaryResolver.getResolvedEntity(
            PolarisEntityType.ROOT, PolarisEntityConstants.getRootContainerName());
    if (resolvedCacheEntry == null) {
      LOGGER.debug("Failed to find rootContainer, so using simulated rootContainer instead.");
      return simulatedResolvedRootContainerEntity;
    }
    return new ResolvedPolarisEntity(resolvedCacheEntry);
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
    EntityCacheEntry resolvedCachedCatalog = primaryResolver.getResolvedReferenceCatalog();
    if (resolvedCachedCatalog == null) {
      return null;
    }
    if (prependRootContainer) {
      // Operations directly on Catalogs also consider the root container to be a parent of its
      // authorization chain.
      // TODO: Throw appropriate Catalog NOT_FOUND exception before any call to
      // getResolvedReferenceCatalogEntity().
      return new PolarisResolvedPathWrapper(
          List.of(
              getResolvedRootContainerEntity(), new ResolvedPolarisEntity(resolvedCachedCatalog)));
    } else {
      return new PolarisResolvedPathWrapper(
          List.of(new ResolvedPolarisEntity(resolvedCachedCatalog)));
    }
  }

  public PolarisEntitySubType getLeafSubType(Object key) {
    diagnostics.check(
        pathLookup.containsKey(key),
        "never_registered_key_for_resolved_path",
        "key={} pathLookup={}",
        key,
        pathLookup);
    int index = pathLookup.get(key);
    List<EntityCacheEntry> resolved = primaryResolver.getResolvedPaths().get(index);
    if (resolved.isEmpty()) {
      return PolarisEntitySubType.NULL_SUBTYPE;
    }
    return resolved.get(resolved.size() - 1).getEntity().getSubType();
  }

  /**
   * @param key the key associated with the path to retrieve that was specified in addPath
   * @param prependRootContainer if true, also includes the rootContainer as the first element of
   *     the path; otherwise, the first element begins with the referenceCatalog.
   * @return null if the path resolved for {@code key} isn't fully-resolved when specified as
   *     "optional"
   */
  public PolarisResolvedPathWrapper getResolvedPath(Object key, boolean prependRootContainer) {
    diagnostics.check(
        pathLookup.containsKey(key),
        "never_registered_key_for_resolved_path",
        "key={} pathLookup={}",
        key,
        pathLookup);

    if (primaryResolverStatus.getStatus() != ResolverStatus.StatusEnum.SUCCESS) {
      return null;
    }
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

  /**
   * @return null if the path resolved for {@code key} isn't fully-resolved when specified as
   *     "optional", or if it was resolved but the subType doesn't match the specified subType.
   */
  public PolarisResolvedPathWrapper getResolvedPath(
      Object key, PolarisEntitySubType subType, boolean prependRootContainer) {
    PolarisResolvedPathWrapper resolvedPath = getResolvedPath(key, prependRootContainer);
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

    if (primaryResolverStatus.getStatus() != ResolverStatus.StatusEnum.SUCCESS) {
      return null;
    }

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

  public enum AuthorizationTargetType {
    PRIMARY(m -> m.primaryTargets),
    SECONDARY(m -> m.secondaryTargets),
    ;

    private final Function<PolarisResolutionManifest, List<Getter>> list;

    AuthorizationTargetType(Function<PolarisResolutionManifest, List<Getter>> list) {
      this.list = list;
    }

    private void addTo(PolarisResolutionManifest manifest, Getter getter) {
      list.apply(manifest).add(getter);
    }
  }

  private interface Getter {
    PolarisResolvedPathWrapper get(PolarisResolutionManifest manifest);

    static Getter rootContainer() {
      return PolarisResolutionManifest::getResolvedRootContainerEntityAsPath;
    }

    static Getter topLevelEntity(String entityName, PolarisEntityType type) {
      return m -> m.getResolvedTopLevelEntity(entityName, type);
    }

    static Getter path(Object key, PolarisEntitySubType subType) {
      return path(key, subType, () -> {});
    }

    static Getter path(Object key, PolarisEntitySubType subType, Runnable notFoundHandler) {
      return manifest -> {
        PolarisResolvedPathWrapper resolved = manifest.getResolvedPath(key, subType, true);
        if (resolved == null) {
          notFoundHandler.run();
        }
        return resolved;
      };
    }

    static Getter path(Object key) {
      return path(key, () -> {});
    }

    static Getter path(Object key, Runnable notFoundHandler) {
      return manifest -> {
        PolarisResolvedPathWrapper resolved = manifest.getResolvedPath(key, true);
        if (resolved == null) {
          notFoundHandler.run();
        }
        return resolved;
      };
    }
  }
}
