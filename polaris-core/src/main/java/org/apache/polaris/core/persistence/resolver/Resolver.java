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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.cache.EntityCacheByNameKey;
import org.apache.polaris.core.persistence.cache.EntityCacheEntry;
import org.apache.polaris.core.persistence.cache.EntityCacheLookupResult;
import org.apache.polaris.core.persistence.cache.PolarisRemoteCache;
import org.apache.polaris.core.persistence.cache.PolarisRemoteCache.ChangeTrackingResult;

/**
 * REST request resolver, allows to resolve all entities referenced directly or indirectly by in
 * incoming rest request, Once resolved, the request can be authorized.
 */
public class Resolver {

  // we stash the Polaris call context here
  private final @Nonnull PolarisCallContext polarisCallContext;

  // the diagnostic services
  private final @Nonnull PolarisDiagnostics diagnostics;

  // the polaris metastore manager
  private final @Nonnull PolarisRemoteCache polarisRemoteCache;

  // the cache of entities
  private final @Nonnull EntityCache cache;

  // the id of the principal making the call or 0 if unknown
  private final long callerPrincipalId;

  // the name of the principal making the call or null if unknown. If 0, the principal name will be
  // not null
  private final String callerPrincipalName;

  // reference catalog name for name resolution
  private final String referenceCatalogName;

  // if not null, subset of principal roles to activate
  private final @Nullable Set<String> callerPrincipalRoleNamesScope;

  // set of entities to resolve given their name. This does not include namespaces or table_like
  // entities which are
  // part of a path
  private final AbstractSet<ResolverEntityName> entitiesToResolve;

  // list of paths to resolve
  private final List<ResolverPath> pathsToResolve;

  // caller principal
  private EntityCacheEntry resolvedCallerPrincipal;

  // all principal roles which have been resolved
  private List<EntityCacheEntry> resolvedCallerPrincipalRoles;

  // catalog to use as the reference catalog for role activation
  private EntityCacheEntry resolvedReferenceCatalog;

  // all catalog roles which have been activated
  private final Map<Long, EntityCacheEntry> resolvedCatalogRoles;

  // all resolved paths
  private List<List<EntityCacheEntry>> resolvedPaths;

  // all entities which have been successfully resolved, by name
  private final Map<EntityCacheByNameKey, EntityCacheEntry> resolvedEntriesByName;

  // all entities which have been fully resolved, by id
  private final Map<Long, EntityCacheEntry> resolvedEntriesById;

  private ResolverStatus resolverStatus;

  /**
   * Constructor, effectively starts an entity resolver session
   *
   * @param polarisCallContext the polaris call context
   * @param polarisRemoteCache meta store manager
   * @param callerPrincipalId if not 0, the id of the principal calling the service
   * @param callerPrincipalName if callerPrincipalId is 0, the name of the principal calling the
   *     service
   * @param callerPrincipalRoleNamesScope if not null, scope principal roles
   * @param cache shared entity cache
   * @param referenceCatalogName if not null, specifies the name of the reference catalog. The
   *     reference catalog is the catalog used to resolve catalog roles and catalog path. Also, if a
   *     catalog reference is added, we will determine all catalog roles which are activated by the
   *     caller. Note that when a catalog name needs to be resolved because the principal creates or
   *     drop a catalog, it should not be specified here. Instead, it should be resolved by calling
   *     {@link #addEntityByName(PolarisEntityType, String)}. Generally, any DDL executed as a
   *     service admin should use null for that parameter.
   */
  public Resolver(
      @Nonnull PolarisCallContext polarisCallContext,
      @Nonnull PolarisRemoteCache polarisRemoteCache,
      long callerPrincipalId,
      @Nullable String callerPrincipalName,
      @Nullable Set<String> callerPrincipalRoleNamesScope,
      @Nonnull EntityCache cache,
      @Nullable String referenceCatalogName) {
    this.polarisCallContext = polarisCallContext;
    this.diagnostics = polarisCallContext.getDiagServices();
    this.polarisRemoteCache = polarisRemoteCache;
    this.cache = cache;
    this.callerPrincipalName = callerPrincipalName;
    this.callerPrincipalId = callerPrincipalId;
    this.referenceCatalogName = referenceCatalogName;

    // scoped principal role names
    this.callerPrincipalRoleNamesScope = callerPrincipalRoleNamesScope;

    // validate inputs
    this.diagnostics.checkNotNull(polarisRemoteCache, "unexpected_null_polarisRemoteCache");
    this.diagnostics.checkNotNull(cache, "unexpected_null_cache");

    // paths to resolve
    this.pathsToResolve = new ArrayList<>();
    this.resolvedPaths = new ArrayList<>();

    // all entities we need to resolve by name
    this.entitiesToResolve = new HashSet<>();

    // will contain all principal roles which we were able to resolve
    this.resolvedCallerPrincipalRoles = new ArrayList<>();

    // remember if a reference catalog name was specified
    if (referenceCatalogName != null) {
      this.resolvedCatalogRoles = new HashMap<>();
    } else {
      this.resolvedCatalogRoles = null;
    }

    // all resolved entities, by name and by if
    this.resolvedEntriesByName = new HashMap<>();
    resolvedEntriesById = new HashMap<>();

    // the resolver has not yet been called
    this.resolverStatus = null;
  }

  /**
   * Add a top-level entity to resolve. If the entity type is a catalog role, we also expect that a
   * reference catalog entity was specified at creation time, else we will assert. That catalog role
   * entity will be resolved from there. We will fail the entire resolution process if that entity
   * cannot be resolved. If this is not expected, use addOptionalEntityByName() instead.
   *
   * @param entityType the type of the entity, either a principal, a principal role, a catalog or a
   *     catalog role.
   * @param entityName the name of the entity
   */
  public void addEntityByName(@Nonnull PolarisEntityType entityType, @Nonnull String entityName) {
    diagnostics.checkNotNull(entityType, "entity_type_is_null");
    diagnostics.checkNotNull(entityName, "entity_name_is_null");
    // can only be called if the resolver has not yet been called
    this.diagnostics.check(resolverStatus == null, "resolver_called");
    this.addEntityByName(entityType, entityName, false);
  }

  /**
   * Add an optional top-level entity to resolve. If the entity type is a catalog role, we also
   * expect that a reference catalog entity was specified at creation time, else we will assert.
   * That catalog role entity will be resolved from there. If the entity cannot be resolved, we will
   * not fail the resolution process
   *
   * @param entityType the type of the entity, either a principal, a principal role, a catalog or a
   *     catalog role.
   * @param entityName the name of the entity
   */
  public void addOptionalEntityByName(
      @Nonnull PolarisEntityType entityType, @Nonnull String entityName) {
    diagnostics.checkNotNull(entityType, "entity_type_is_null");
    diagnostics.checkNotNull(entityName, "entity_name_is_null");
    // can only be called if the resolver has not yet been called
    this.diagnostics.check(resolverStatus == null, "resolver_called");
    this.addEntityByName(entityType, entityName, true);
  }

  /**
   * Add a path to resolve
   *
   * @param path path to resolve
   */
  public void addPath(@Nonnull ResolverPath path) {
    // can only be called if the resolver has not yet been called
    this.diagnostics.check(resolverStatus == null, "resolver_called");
    diagnostics.checkNotNull(path, "unexpected_null_entity_path");
    this.pathsToResolve.add(path);
  }

  /**
   * Run the resolution process and return the status, either an error or success
   *
   * <pre>
   * resolution might be working using multiple passes when using the cache since anything we find in the cache might
   * have changed in the backend store.
   * For each pass we will
   *    -  go over all entities and call EntityCache.getOrLoad...() on these entities, including all paths.
   *    -  split these entities into 3 groups:
   *          - dropped or purged. We will return an error for these.
   *          - to be validated entities, they were found in the cache. For those we need to ensure that the
   *            entity id, its name and parent id has not changed. If yes we need to perform another pass.
   *          - reloaded from backend, so the entity is validated. Validated entities will not be validated again
   * </pre>
   *
   * @return the status of the resolver. If success, all entities have been resolved and the
   *     getResolvedXYZ() method can be called.
   */
  public ResolverStatus resolveAll() {
    // can only be called if the resolver has not yet been called
    this.diagnostics.check(resolverStatus == null, "resolver_called");

    // retry until a pass terminates, or we reached the maximum iteration count. Note that we should
    // finish normally in no more than few passes so the 1000 limit is really to avoid spinning
    // forever if there is a bug.
    int count = 0;
    ResolverStatus status;
    do {
      status = runResolvePass();
      count++;
    } while (status == null && ++count < 1000);

    // assert if status is null
    this.diagnostics.checkNotNull(status, "cannot_resolve_all_entities");

    // remember the resolver status
    this.resolverStatus = status;

    // all has been resolved
    return status;
  }

  /**
   * @return the principal we resolved
   */
  public @Nullable EntityCacheEntry getResolvedCallerPrincipal() {
    // can only be called if the resolver has been called and was success
    this.diagnostics.checkNotNull(resolverStatus, "resolver_must_be_called_first");
    this.diagnostics.check(
        resolverStatus.getStatus() == ResolverStatus.StatusEnum.SUCCESS,
        "resolver_must_be_successful");

    return resolvedCallerPrincipal;
  }

  /**
   * @return all principal roles which were activated. The list can be empty
   */
  public @Nonnull List<EntityCacheEntry> getResolvedCallerPrincipalRoles() {
    // can only be called if the resolver has been called and was success
    this.diagnostics.checkNotNull(resolverStatus, "resolver_must_be_called_first");
    this.diagnostics.check(
        resolverStatus.getStatus() == ResolverStatus.StatusEnum.SUCCESS,
        "resolver_must_be_successful");

    return resolvedCallerPrincipalRoles;
  }

  /**
   * @return the reference catalog which has been resolved. Will be null if null was passed in for
   *     the parameter referenceCatalogName when the Resolver was constructed.
   */
  public @Nullable EntityCacheEntry getResolvedReferenceCatalog() {
    // can only be called if the resolver has been called and was success
    this.diagnostics.checkNotNull(resolverStatus, "resolver_must_be_called_first");
    this.diagnostics.check(
        resolverStatus.getStatus() == ResolverStatus.StatusEnum.SUCCESS,
        "resolver_must_be_successful");

    return resolvedReferenceCatalog;
  }

  /**
   * Empty map if no catalog was resolved. Else the list of catalog roles which are activated by the
   * caller
   *
   * @return map of activated catalog roles or null if no referenceCatalogName was specified
   */
  public @Nullable Map<Long, EntityCacheEntry> getResolvedCatalogRoles() {
    // can only be called if the resolver has been called and was success
    this.diagnostics.checkNotNull(resolverStatus, "resolver_must_be_called_first");
    this.diagnostics.check(
        resolverStatus.getStatus() == ResolverStatus.StatusEnum.SUCCESS,
        "resolver_must_be_successful");

    return resolvedCatalogRoles;
  }

  /**
   * Get path which has been resolved, should be used only when a single path was added to the
   * resolver. If the path to resolve was optional, only the prefix that was resolved will be
   * returned.
   *
   * @return single resolved path
   */
  public @Nonnull List<EntityCacheEntry> getResolvedPath() {
    // can only be called if the resolver has been called and was success
    this.diagnostics.checkNotNull(resolverStatus, "resolver_must_be_called_first");
    this.diagnostics.check(
        resolverStatus.getStatus() == ResolverStatus.StatusEnum.SUCCESS,
        "resolver_must_be_successful");
    this.diagnostics.check(this.resolvedPaths.size() == 1, "only_if_single");

    return resolvedPaths.get(0);
  }

  /**
   * One of more resolved path, in the order they were added to the resolver.
   *
   * @return list of resolved path
   */
  public @Nonnull List<List<EntityCacheEntry>> getResolvedPaths() {
    // can only be called if the resolver has been called and was success
    this.diagnostics.checkNotNull(resolverStatus, "resolver_must_be_called_first");
    this.diagnostics.check(
        resolverStatus.getStatus() == ResolverStatus.StatusEnum.SUCCESS,
        "resolver_must_be_successful");
    this.diagnostics.check(!this.resolvedPaths.isEmpty(), "no_path_resolved");

    return resolvedPaths;
  }

  /**
   * Get resolved entity associated to the specified type and name or null if not found
   *
   * @param entityType type of the entity, cannot be a NAMESPACE or a TABLE_LIKE entity. If it is a
   *     top-level catalog entity (i.e. CATALOG_ROLE), a reference catalog must have been specified
   *     at construction time.
   * @param entityName name of the entity.
   * @return the entity which has been resolved or null if that entity does not exist
   */
  public @Nullable EntityCacheEntry getResolvedEntity(
      @Nonnull PolarisEntityType entityType, @Nonnull String entityName) {
    // can only be called if the resolver has been called and was success
    this.diagnostics.checkNotNull(resolverStatus, "resolver_must_be_called_first");
    this.diagnostics.check(
        resolverStatus.getStatus() == ResolverStatus.StatusEnum.SUCCESS,
        "resolver_must_be_successful");

    // validate input
    diagnostics.check(
        entityType != PolarisEntityType.NAMESPACE && entityType != PolarisEntityType.TABLE_LIKE,
        "cannot_be_path");
    diagnostics.check(
        entityType.isTopLevel() || this.referenceCatalogName != null, "reference_catalog_expected");

    if (entityType.isTopLevel()) {
      return this.resolvedEntriesByName.get(new EntityCacheByNameKey(entityType, entityName));
    } else {
      long catalogId = this.resolvedReferenceCatalog.getEntity().getId();
      return this.resolvedEntriesByName.get(
          new EntityCacheByNameKey(catalogId, catalogId, entityType, entityName));
    }
  }

  /**
   * Execute one resolve pass on all entities
   *
   * @return status of the resolve pass
   */
  private ResolverStatus runResolvePass() {

    // we will resolve those again
    this.resolvedCallerPrincipal = null;
    this.resolvedReferenceCatalog = null;
    if (this.resolvedCatalogRoles != null) {
      this.resolvedCatalogRoles.clear();
    }
    this.resolvedCallerPrincipalRoles.clear();
    this.resolvedPaths.clear();

    // all entries we found in the cache but that we need to validate since they might be stale
    List<EntityCacheEntry> toValidate = new ArrayList<>();

    // first resolve the principal and determine the set of activated principal roles
    ResolverStatus status;
    if (this.callerPrincipalId > 0 || this.callerPrincipalName != null) {
      status =
          this.resolveCallerPrincipalAndPrincipalRoles(
              toValidate,
              this.callerPrincipalId,
              this.callerPrincipalName,
              this.callerPrincipalRoleNamesScope);
    } else {
      status = new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS);
    }

    // if success, continue resolving
    if (status.getStatus() == ResolverStatus.StatusEnum.SUCCESS) {
      // then resolve the reference catalog if one was specified
      if (this.referenceCatalogName != null) {
        status = this.resolveReferenceCatalog(toValidate, this.referenceCatalogName);
      }

      // if success, continue resolving
      if (status.getStatus() == ResolverStatus.StatusEnum.SUCCESS) {
        // then resolve all the additional entities we were asked to resolve
        status = this.resolveEntities(toValidate, this.entitiesToResolve);

        // if success, continue resolving
        if (status.getStatus() == ResolverStatus.StatusEnum.SUCCESS
            && this.referenceCatalogName != null) {
          // finally, resolve all paths we need to resolve
          status = this.resolvePaths(toValidate, this.pathsToResolve);
        }
      }
    }

    // all the above resolution was optimistic i.e. when we probe the cache and find an entity, we
    // don't validate if this entity has been changed in the backend. So validate now all these
    // entities in one single
    // go,
    boolean validationSuccess = this.bulkValidate(toValidate);

    if (validationSuccess) {
      this.updateResolved();
    }

    // if success, we are done, simply return the status.
    return validationSuccess ? status : null;
  }

  /**
   * Update all entities which have been resolved since after validation, some might have changed
   */
  private void updateResolved() {

    // if success, we need to get the validated entries
    // we will resolve those again
    this.resolvedCallerPrincipal = this.getResolved(this.resolvedCallerPrincipal);

    // update all principal roles with latest
    if (!this.resolvedCallerPrincipalRoles.isEmpty()) {
      List<EntityCacheEntry> refreshedResolvedCallerPrincipalRoles =
          new ArrayList<>(this.resolvedCallerPrincipalRoles.size());
      this.resolvedCallerPrincipalRoles.forEach(
          ce -> refreshedResolvedCallerPrincipalRoles.add(this.getResolved(ce)));
      this.resolvedCallerPrincipalRoles = refreshedResolvedCallerPrincipalRoles;
    }

    // update referenced catalog
    this.resolvedReferenceCatalog = this.getResolved(this.resolvedReferenceCatalog);

    // update all resolved catalog roles
    if (this.resolvedCatalogRoles != null) {
      for (EntityCacheEntry catalogCacheEntry : this.resolvedCatalogRoles.values()) {
        this.resolvedCatalogRoles.put(
            catalogCacheEntry.getEntity().getId(), this.getResolved(catalogCacheEntry));
      }
    }

    // update all resolved paths
    if (!this.resolvedPaths.isEmpty()) {
      List<List<EntityCacheEntry>> refreshedResolvedPaths =
          new ArrayList<>(this.resolvedPaths.size());
      this.resolvedPaths.forEach(
          rp -> {
            List<EntityCacheEntry> refreshedRp = new ArrayList<>(rp.size());
            rp.forEach(ce -> refreshedRp.add(this.getResolved(ce)));
            refreshedResolvedPaths.add(refreshedRp);
          });
      this.resolvedPaths = refreshedResolvedPaths;
    }
  }

  /**
   * Get the fully resolved cache entry for the specified cache entry
   *
   * @param cacheEntry input cache entry
   * @return the fully resolved cached entry which will often be the same
   */
  private EntityCacheEntry getResolved(EntityCacheEntry cacheEntry) {
    final EntityCacheEntry refreshedEntry;
    if (cacheEntry == null) {
      refreshedEntry = null;
    } else {
      // the latest refreshed entry
      refreshedEntry = this.resolvedEntriesById.get(cacheEntry.getEntity().getId());
      this.diagnostics.checkNotNull(
          refreshedEntry, "cache_entry_should_be_resolved", "entity={}", cacheEntry.getEntity());
    }
    return refreshedEntry;
  }

  /**
   * Bulk validate now the set of entities we didn't validate when we were accessing the entity
   * cache
   *
   * @param toValidate entities to validate
   * @return true if none of the entities in the cache has changed
   */
  private boolean bulkValidate(List<EntityCacheEntry> toValidate) {
    // assume everything is good
    boolean validationStatus = true;

    // bulk validate
    if (!toValidate.isEmpty()) {
      List<PolarisEntityId> entityIds =
          toValidate.stream()
              .map(
                  cacheEntry ->
                      new PolarisEntityId(
                          cacheEntry.getEntity().getCatalogId(), cacheEntry.getEntity().getId()))
              .collect(Collectors.toList());

      // now get the current backend versions of all these entities
      ChangeTrackingResult changeTrackingResult =
          this.polarisRemoteCache.loadEntitiesChangeTracking(this.polarisCallContext, entityIds);

      // refresh any entity which is not fresh. If an entity is missing, reload it
      Iterator<EntityCacheEntry> entityIterator = toValidate.iterator();
      Iterator<PolarisChangeTrackingVersions> versionIterator =
          changeTrackingResult.getChangeTrackingVersions().iterator();

      // determine the ones we need to reload or refresh and the ones which are up-to-date
      while (entityIterator.hasNext()) {
        // get cache entry and associated versions
        EntityCacheEntry cacheEntry = entityIterator.next();
        PolarisChangeTrackingVersions versions = versionIterator.next();

        // entity we found in the cache
        PolarisBaseEntity entity = cacheEntry.getEntity();

        // refresh cache entry if the entity or grant records version is different
        final EntityCacheEntry refreshedCacheEntry;
        if (versions == null
            || entity.getEntityVersion() != versions.getEntityVersion()
            || entity.getGrantRecordsVersion() != versions.getGrantRecordsVersion()) {
          // if null version we need to invalidate the cached entry since it has probably been
          // dropped
          if (versions == null) {
            this.cache.removeCacheEntry(cacheEntry);
            refreshedCacheEntry = null;
          } else {
            // refresh that entity. If versions is null, it has been dropped
            refreshedCacheEntry =
                this.cache.getAndRefreshIfNeeded(
                    this.polarisCallContext,
                    entity,
                    versions.getEntityVersion(),
                    versions.getGrantRecordsVersion());
          }

          // get the refreshed entity
          PolarisBaseEntity refreshedEntity =
              (refreshedCacheEntry == null) ? null : refreshedCacheEntry.getEntity();

          // if the entity has been removed, or its name has changed, or it was re-parented, or it
          // was dropped, we will have to perform another pass
          if (refreshedEntity == null
              || refreshedEntity.getParentId() != entity.getParentId()
              || refreshedEntity.isDropped() != entity.isDropped()
              || !refreshedEntity.getName().equals(entity.getName())) {
            validationStatus = false;
          }

          // special cases: the set of principal roles or catalog roles which have been
          // activated might change if usage grants to a principal or a principal role have
          // changed. Hence, force another pass if we are in that scenario
          if (entity.getTypeCode() == PolarisEntityType.PRINCIPAL.getCode()
              || entity.getTypeCode() == PolarisEntityType.PRINCIPAL_ROLE.getCode()) {
            validationStatus = false;
          }
        } else {
          // no need to refresh, it is up-to-date
          refreshedCacheEntry = cacheEntry;
        }

        // if it was found, it has been resolved, so if there is another pass, we will not have to
        // resolve it again
        if (refreshedCacheEntry != null) {
          this.addToResolved(refreshedCacheEntry);
        }
      }
    }

    // done, return final validation status
    return validationStatus;
  }

  /**
   * Resolve a set of top-level service or catalog entities
   *
   * @param toValidate all entities we have resolved from the cache, hence we will have to verify
   *     that these entities have not changed in the backend
   * @param entitiesToResolve the set of entities to resolve
   * @return the status of resolution
   */
  private ResolverStatus resolveEntities(
      List<EntityCacheEntry> toValidate, AbstractSet<ResolverEntityName> entitiesToResolve) {
    // resolve each
    for (ResolverEntityName entityName : entitiesToResolve) {
      // resolve that entity
      EntityCacheEntry resolvedEntity =
          this.resolveByName(toValidate, entityName.getEntityType(), entityName.getEntityName());

      // if not found, we can exit unless the entity is optional
      if (!entityName.isOptional()
          && (resolvedEntity == null || resolvedEntity.getEntity().isDropped())) {
        return new ResolverStatus(entityName.getEntityType(), entityName.getEntityName());
      }
    }

    // complete success
    return new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS);
  }

  /**
   * Resolve a set of path inside the referenced catalog
   *
   * @param toValidate all entities we have resolved from the cache, hence we will have to verify
   *     that these entities have not changed in the backend
   * @param pathsToResolve the set of paths to resolve
   * @return the status of resolution
   */
  private ResolverStatus resolvePaths(
      List<EntityCacheEntry> toValidate, List<ResolverPath> pathsToResolve) {

    // id of the catalog for all these paths
    final long catalogId = this.resolvedReferenceCatalog.getEntity().getId();

    // resolve each path
    for (ResolverPath path : pathsToResolve) {

      // path we are resolving
      List<EntityCacheEntry> resolvedPath = new ArrayList<>();

      // initial parent id is the catalog itself
      long parentId = catalogId;

      // resolve each segment
      Iterator<String> pathIt = path.getEntityNames().iterator();
      for (int segmentIndex = 0; segmentIndex < path.getEntityNames().size(); segmentIndex++) {
        // get segment name
        String segmentName = pathIt.next();

        // determine the segment type
        PolarisEntityType segmentType =
            pathIt.hasNext() ? PolarisEntityType.NAMESPACE : path.getLastEntityType();

        // resolve that entity
        EntityCacheEntry segment =
            this.resolveByName(toValidate, catalogId, segmentType, parentId, segmentName);

        // if not found, abort
        if (segment == null || segment.getEntity().isDropped()) {
          if (path.isOptional()) {
            // we have resolved as much as what we could have
            break;
          } else {
            return new ResolverStatus(path, segmentIndex);
          }
        }

        // this is the parent of the next segment
        parentId = segment.getEntity().getId();

        // add it to the path we are resolving
        resolvedPath.add(segment);
      }

      // one more path has been resolved
      this.resolvedPaths.add(resolvedPath);
    }

    // complete success
    return new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS);
  }

  /**
   * Resolve the principal and determine which principal roles are activated. Resolved those.
   *
   * @param toValidate all entities we have resolved from the cache, hence we will have to verify
   *     that these entities have not changed in the backend
   * @param callerPrincipalId the id of the principal which made the call
   * @param callerPrincipalRoleNamesScope if not null, subset of roles activated by this call
   * @return the status of resolution
   */
  private ResolverStatus resolveCallerPrincipalAndPrincipalRoles(
      List<EntityCacheEntry> toValidate,
      long callerPrincipalId,
      String callerPrincipalName,
      Set<String> callerPrincipalRoleNamesScope) {

    // resolve the principal, by name or id
    this.resolvedCallerPrincipal =
        (callerPrincipalId != PolarisEntityConstants.getNullId())
            ? this.resolveById(
                toValidate,
                PolarisEntityType.PRINCIPAL,
                PolarisEntityConstants.getNullId(),
                callerPrincipalId)
            : this.resolveByName(toValidate, PolarisEntityType.PRINCIPAL, callerPrincipalName);

    // if the principal was not found, we can end right there
    if (this.resolvedCallerPrincipal == null
        || this.resolvedCallerPrincipal.getEntity().isDropped()) {
      return new ResolverStatus(ResolverStatus.StatusEnum.CALLER_PRINCIPAL_DOES_NOT_EXIST);
    }

    // activate all principal roles which still exist
    for (PolarisGrantRecord grantRecord : this.resolvedCallerPrincipal.getGrantRecordsAsGrantee()) {
      if (grantRecord.getPrivilegeCode() == PolarisPrivilege.PRINCIPAL_ROLE_USAGE.getCode()) {

        // resolve principal role granted to that principal
        EntityCacheEntry principalRole =
            this.resolveById(
                toValidate,
                PolarisEntityType.PRINCIPAL_ROLE,
                PolarisEntityConstants.getNullId(),
                grantRecord.getSecurableId());

        // skip if purged or has been dropped
        if (principalRole != null && !principalRole.getEntity().isDropped()) {
          // add it to the activated list if no scoped principal role or this principal role is
          // activated
          if (callerPrincipalRoleNamesScope == null
              || callerPrincipalRoleNamesScope.contains(principalRole.getEntity().getName())) {
            // this principal role is activated
            this.resolvedCallerPrincipalRoles.add(principalRole);
          }
        }
      }
    }

    // total success
    return new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS);
  }

  /**
   * Resolve the reference catalog and determine all activated role. The principal and principal
   * roles should have already been resolved
   *
   * @param toValidate all entities we have resolved from the cache, hence we will have to verify
   *     that these entities have not changed in the backend
   * @param referenceCatalogName name of the reference catalog to resolve, along with all catalog
   *     roles which are activated
   * @return the status of resolution
   */
  private ResolverStatus resolveReferenceCatalog(
      @Nonnull List<EntityCacheEntry> toValidate, @Nonnull String referenceCatalogName) {
    // resolve the catalog
    this.resolvedReferenceCatalog =
        this.resolveByName(toValidate, PolarisEntityType.CATALOG, referenceCatalogName);

    // error out if we couldn't find it
    if (this.resolvedReferenceCatalog == null
        || this.resolvedReferenceCatalog.getEntity().isDropped()) {
      return new ResolverStatus(PolarisEntityType.CATALOG, this.referenceCatalogName);
    }

    // determine the set of catalog roles which have been activated
    long catalogId = this.resolvedReferenceCatalog.getEntity().getId();
    for (EntityCacheEntry principalRole : resolvedCallerPrincipalRoles) {
      for (PolarisGrantRecord grantRecord : principalRole.getGrantRecordsAsGrantee()) {
        // the securable is a catalog role belonging to
        if (grantRecord.getPrivilegeCode() == PolarisPrivilege.CATALOG_ROLE_USAGE.getCode()
            && grantRecord.getSecurableCatalogId() == catalogId) {
          // the id of the catalog role
          long catalogRoleId = grantRecord.getSecurableId();

          // skip if it has already been added
          if (!this.resolvedCatalogRoles.containsKey(catalogRoleId)) {
            // see if this catalog can be resolved
            EntityCacheEntry catalogRole =
                this.resolveById(
                    toValidate, PolarisEntityType.CATALOG_ROLE, catalogId, catalogRoleId);

            // if found and not dropped, add it to the list of activated catalog roles
            if (catalogRole != null && !catalogRole.getEntity().isDropped()) {
              this.resolvedCatalogRoles.put(catalogRoleId, catalogRole);
            }
          }
        }
      }
    }

    // all good
    return new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS);
  }

  /**
   * Add a cache entry to the set of resolved entities
   *
   * @param refreshedCacheEntry refreshed cache entry
   */
  private void addToResolved(EntityCacheEntry refreshedCacheEntry) {
    // underlying entity
    PolarisBaseEntity entity = refreshedCacheEntry.getEntity();

    // add it by ID
    this.resolvedEntriesById.put(entity.getId(), refreshedCacheEntry);

    // in the by name map, only add it if it has not been dropped
    if (!entity.isDropped()) {
      this.resolvedEntriesByName.put(
          new EntityCacheByNameKey(
              entity.getCatalogId(), entity.getParentId(), entity.getType(), entity.getName()),
          refreshedCacheEntry);
    }
  }

  /**
   * Add a top-level entity to resolve. If the entity type is a catalog role, we also expect that a
   * reference catalog entity was specified at creation time, else we will assert. That catalog role
   * entity will be resolved from there. We will fail the entire resolution process if that entity
   * cannot be resolved. If this is not expected, use addOptionalEntityByName() instead.
   *
   * @param entityType the type of the entity, either a principal, a principal role, a catalog or a
   *     catalog role.
   * @param entityName the name of the entity
   * @param optional if true, the entity is optional
   */
  private void addEntityByName(
      @Nonnull PolarisEntityType entityType, @Nonnull String entityName, boolean optional) {

    // can only be called if the resolver has not yet been called
    this.diagnostics.check(resolverStatus == null, "resolver_called");

    // ensure everything was specified
    diagnostics.checkNotNull(entityType, "unexpected_null_entity_type");
    diagnostics.checkNotNull(entityName, "unexpected_null_entity_name");

    // ensure that a reference catalog has been specified if this entity is a catalog role
    diagnostics.check(
        entityType != PolarisEntityType.CATALOG_ROLE || this.referenceCatalogName != null,
        "reference_catalog_must_be_specified");

    // one more to resolve
    this.entitiesToResolve.add(new ResolverEntityName(entityType, entityName, optional));
  }

  /**
   * Resolve a top-level entity by name
   *
   * @param toValidate set of entries we will have to validate
   * @param entityType entity type
   * @param entityName name of the entity to resolve
   * @return cache entry created for that entity
   */
  private EntityCacheEntry resolveByName(
      List<EntityCacheEntry> toValidate, PolarisEntityType entityType, String entityName) {
    if (entityType.isTopLevel()) {
      return this.resolveByName(
          toValidate,
          PolarisEntityConstants.getNullId(),
          entityType,
          PolarisEntityConstants.getNullId(),
          entityName);
    } else {
      // only top-level catalog entity
      long catalogId = this.resolvedReferenceCatalog.getEntity().getId();
      this.diagnostics.check(entityType == PolarisEntityType.CATALOG_ROLE, "catalog_role_expected");
      return this.resolveByName(toValidate, catalogId, entityType, catalogId, entityName);
    }
  }

  /**
   * Resolve a top-level entity by name
   *
   * @param toValidate (IN/OUT) list of entities we will have to validate
   * @param entityType entity type
   * @param entityName name of the entity to resolve
   * @return the resolve entity. Potentially update the toValidate list if we will have to validate
   *     that this entity is up-to-date
   */
  private EntityCacheEntry resolveByName(
      @Nonnull List<EntityCacheEntry> toValidate,
      long catalogId,
      @Nonnull PolarisEntityType entityType,
      long parentId,
      @Nonnull String entityName) {

    // key for that entity
    EntityCacheByNameKey nameKey =
        new EntityCacheByNameKey(catalogId, parentId, entityType, entityName);

    // first check if this entity has not yet been resolved
    EntityCacheEntry cacheEntry = this.resolvedEntriesByName.get(nameKey);
    if (cacheEntry != null) {
      return cacheEntry;
    }

    // then check if it does not exist in the toValidate list. The same entity might be resolved
    // several times with multi-path resolution
    for (EntityCacheEntry ce : toValidate) {
      PolarisBaseEntity entity = ce.getEntity();
      if (entity.getCatalogId() == catalogId
          && entity.getParentId() == parentId
          && entity.getType() == entityType
          && entity.getName().equals(entityName)) {
        return ce;
      }
    }

    // get or load by name
    EntityCacheLookupResult lookupResult =
        this.cache.getOrLoadEntityByName(
            this.polarisCallContext,
            new EntityCacheByNameKey(catalogId, parentId, entityType, entityName));

    // if not found
    if (lookupResult == null) {
      // not found
      return null;
    } else if (lookupResult.isCacheHit()) {
      // found in the cache, we will have to validate this entity
      toValidate.add(lookupResult.getCacheEntry());
    } else {
      // entry cannot be null
      this.diagnostics.checkNotNull(lookupResult.getCacheEntry(), "cache_entry_is_null");
      // if not found in cache, it was loaded from backend, hence it has been resolved
      this.addToResolved(lookupResult.getCacheEntry());
    }

    // return the cache entry
    return lookupResult.getCacheEntry();
  }

  /**
   * Resolve an entity by id
   *
   * @param toValidate (IN/OUT) list of entities we will have to validate
   * @param entityType type of the entity to resolve
   * @param catalogId entity catalog id
   * @param entityId entity id
   * @return the resolve entity. Potentially update the toValidate list if we will have to validate
   *     that this entity is up-to-date
   */
  private EntityCacheEntry resolveById(
      @Nonnull List<EntityCacheEntry> toValidate,
      @Nonnull PolarisEntityType entityType,
      long catalogId,
      long entityId) {
    // get or load by name
    EntityCacheLookupResult lookupResult =
        this.cache.getOrLoadEntityById(this.polarisCallContext, catalogId, entityId);

    // if not found, return null
    if (lookupResult == null) {
      return null;
    } else if (lookupResult.isCacheHit()) {
      // found in the cache, we will have to validate this entity
      toValidate.add(lookupResult.getCacheEntry());
    } else {
      // entry cannot be null
      this.diagnostics.checkNotNull(lookupResult.getCacheEntry(), "cache_entry_is_null");

      // if not found in cache, it was loaded from backend, hence it has been resolved
      this.addToResolved(lookupResult.getCacheEntry());
    }

    // return the cache entry
    return lookupResult.getCacheEntry();
  }
}
