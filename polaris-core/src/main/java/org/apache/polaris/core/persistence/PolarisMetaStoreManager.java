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
package org.apache.polaris.core.persistence;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.PolarisGrantManager;
import org.apache.polaris.core.auth.PolarisSecretsManager;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.ChangeTrackingResult;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingManager;
import org.apache.polaris.core.storage.PolarisCredentialVendor;

/**
 * Polaris Metastore Manager manages all Polaris entities and associated grant records metadata for
 * authorization. It uses the underlying persistent metastore to store and retrieve Polaris metadata
 */
public interface PolarisMetaStoreManager
    extends PolarisSecretsManager,
        PolarisGrantManager,
        PolarisCredentialVendor,
        PolarisPolicyMappingManager {

  /**
   * Bootstrap the Polaris service, creating the root catalog, root principal, and associated
   * service admin role. Will fail if the service has already been bootstrapped.
   *
   * @param callCtx call context
   * @return the result of the bootstrap attempt
   */
  @Nonnull
  BaseResult bootstrapPolarisService(@Nonnull PolarisCallContext callCtx);

  /**
   * Purge all metadata associated with the Polaris service, resetting the metastore to the state it
   * was in prior to bootstrapping.
   *
   * <p>*************************** WARNING ************************
   *
   * <p>This will destroy whatever Polaris metadata exists in the metastore
   *
   * @param callCtx call context
   * @return always success or unexpected error
   */
  @Nonnull
  BaseResult purge(@Nonnull PolarisCallContext callCtx);

  /**
   * Resolve an entity by name. Can be a top-level entity like a catalog or an entity inside a
   * catalog like a namespace, a role, a table like entity, or a principal. If the entity is inside
   * a catalog, the parameter catalogPath must be specified
   *
   * @param callCtx call context
   * @param catalogPath path inside a catalog to that entity, rooted by the catalog. If null, the
   *     entity being resolved is a top-level account entity like a catalog.
   * @param entityType entity type
   * @param entitySubType entity subtype. Can be the special value ANY_SUBTYPE to match any
   *     subtypes. Else exact match on the subtype will be required.
   * @param name name of the entity, cannot be null
   * @return the result of the lookup operation. ENTITY_NOT_FOUND is returned if the specified
   *     entity is not found in the specified path. CONCURRENT_MODIFICATION_DETECTED_NEED_RETRY is
   *     returned if the specified catalog path cannot be resolved.
   */
  @Nonnull
  EntityResult readEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name);

  /**
   * List all entities of the specified type under the specified catalogPath. If the catalogPath is
   * null, listed entities will be top-level entities like catalogs.
   *
   * @param callCtx call context
   * @param catalogPath path inside a catalog. If null or empty, the entities to list are top-level,
   *     like catalogs
   * @param entityType entity type
   * @param entitySubType entity subtype. Can be the special value ANY_SUBTYPE to match any subtype.
   *     Else exact match will be performed.
   * @return all entities name, ids and subtype under the specified namespace.
   */
  @Nonnull
  ListEntitiesResult listEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull PageToken pageToken);

  /**
   * Generate a new unique id that can be used by the Polaris client when it needs to create a new
   * entity
   *
   * @param callCtx call context
   * @return the newly created id, not expected to fail
   */
  @Nonnull
  GenerateEntityIdResult generateNewEntityId(@Nonnull PolarisCallContext callCtx);

  /**
   * Create a new principal. This not only creates the new principal entity but also generates a
   * client_id/secret pair for this new principal.
   *
   * @param callCtx call context
   * @param principal the principal entity to create
   * @return the client_id/secret for the new principal which was created. Will return
   *     ENTITY_ALREADY_EXISTS if the principal already exists
   */
  @Nonnull
  CreatePrincipalResult createPrincipal(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity principal);

  /**
   * Create a new catalog. This not only creates the new catalog entity but also the initial admin
   * role required to admin this catalog. If inline storage integration property is provided, create
   * a storage integration.
   *
   * @param callCtx call context
   * @param catalog the catalog entity to create
   * @param principalRoles once the catalog has been created, list of principal roles to grant its
   *     catalog_admin role to. If no principal role is specified, we will grant the catalog_admin
   *     role of the newly created catalog to the service admin role.
   * @return if success, the catalog which was created and its admin role.
   */
  @Nonnull
  CreateCatalogResult createCatalog(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity catalog,
      @Nonnull List<PolarisEntityCore> principalRoles);

  /**
   * Persist a newly created entity under the specified catalog path if specified, else this is a
   * top-level entity. We will re-resolve the specified path to ensure nothing has changed since the
   * Polaris app resolved the path. If the entity already exists with the same specified id, we will
   * simply return it. This can happen when the client retries. If a catalogPath is specified and
   * cannot be resolved, we will return null. And of course if another entity exists with the same
   * name, we will fail and also return null.
   *
   * @param callCtx call context
   * @param catalogPath path inside a catalog. If null, the entity to persist is assumed to be
   *     top-level.
   * @param entity entity to write
   * @return the newly created entity. If this entity was already created, we will simply return the
   *     already created entity. We will return null if a different entity with the same name exists
   *     or if the catalogPath couldn't be resolved. If null is returned, the client app should
   *     retry this operation.
   */
  @Nonnull
  EntityResult createEntityIfNotExists(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity);

  /**
   * Persist a batch of newly created entities under the specified catalog path if specified, else
   * these are top-level entities. We will re-resolve the specified path to ensure nothing has
   * changed since the Polaris app resolved the path. If any of the entities already exists with the
   * same specified id, we will simply return it. This can happen when the client retries. If a
   * catalogPath is specified and cannot be resolved, we will return null and none of the entities
   * will be persisted. And of course if any entity conflicts with an existing entity with the same
   * name, we will fail all entities and also return null.
   *
   * @param callCtx call context
   * @param catalogPath path inside a catalog. If null, the entity to persist is assumed to be
   *     top-level.
   * @param entities batch of entities to write
   * @return the newly created entities. If the entities were already created, we will simply return
   *     the already created entity. We will return null if a different entity with the same name
   *     exists or if the catalogPath couldn't be resolved. If null is returned, the client app
   *     should retry this operation.
   */
  @Nonnull
  EntitiesResult createEntitiesIfNotExist(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull List<? extends PolarisBaseEntity> entities);

  /**
   * Update some properties of this entity assuming it can still be resolved the same way and itself
   * has not changed. If this is not the case we will return false. Else we will update both the
   * internal and visible properties and return true
   *
   * @param callCtx call context
   * @param catalogPath path to that entity. Could be null if this entity is top-level
   * @param entity entity to update, cannot be null
   * @return the entity we updated or null if the client should retry
   */
  @Nonnull
  EntityResult updateEntityPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity);

  /**
   * This works exactly like {@link #updateEntityPropertiesIfNotChanged(PolarisCallContext, List,
   * PolarisBaseEntity)} but allows to operate on multiple entities at once. Just loop through the
   * list, calling each entity update and return null if any of those fail.
   *
   * @param callCtx call context
   * @param entities the set of entities to update
   * @return list of all entities we updated or null if the client should retry because one update
   *     failed
   */
  @Nonnull
  EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<EntityWithPath> entities);

  /**
   * Rename an entity, potentially re-parenting it.
   *
   * @param callCtx call context
   * @param catalogPath path to that entity. Could be an empty list of the entity is a catalog.
   * @param entityToRename entity to rename. This entity should have been resolved by the client
   * @param newCatalogPath if not null, new catalog path
   * @param renamedEntity the new renamed entity we need to persist. We will use this argument to
   *     also update the internal and external properties as part of the rename operation. This is
   *     required to update the namespace path of the entity if it has changed
   * @return the entity after renaming it or null if the rename operation has failed
   */
  @Nonnull
  EntityResult renameEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @Nonnull PolarisEntity renamedEntity);

  /**
   * Drop the specified entity assuming it exists
   *
   * @param callCtx call context
   * @param catalogPath path to that entity. Could be an empty list of the entity is a catalog.
   * @param entityToDrop entity to drop, must have been resolved by the client
   * @param cleanupProperties if not null, properties that will be persisted with the cleanup task
   * @param cleanup true if resources owned by this entity should be deleted as well
   * @return the result of the drop entity call, either success or error. If the error, it could be
   *     that the namespace or catalog to drop still has children, this should not be retried and
   *     should cause a failure
   */
  @Nonnull
  DropEntityResult dropEntityIfExists(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup);

  /**
   * Load the entity from backend store. Will return NULL if the entity does not exist, i.e. has
   * been purged. The entity being loaded might have been dropped
   *
   * @param callCtx call context
   * @param entityCatalogId id of the catalog for that entity
   * @param entityId the id of the entity to load
   */
  @Nonnull
  EntityResult loadEntity(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      @Nonnull PolarisEntityType entityType);

  /**
   * Fetch a list of tasks to be completed. Tasks
   *
   * @param callCtx call context
   * @param executorId executor id
   * @param pageToken page token to start after
   * @return list of tasks to be completed
   */
  @Nonnull
  EntitiesResult loadTasks(
      @Nonnull PolarisCallContext callCtx, String executorId, PageToken pageToken);

  /**
   * Load change tracking information for a set of entities in one single shot and return for each
   * the version for the entity itself and the version associated to its grant records.
   *
   * @param callCtx call context
   * @param entityIds list of catalog/entity pair ids for which we need to efficiently load the
   *     version information, both entity version and grant records version.
   * @return a list of version tracking information. Order in that returned list is the same as the
   *     input list. Some elements might be NULL if the entity has been purged. Not expected to fail
   */
  @Nonnull
  ChangeTrackingResult loadEntitiesChangeTracking(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<PolarisEntityId> entityIds);

  /**
   * Load a resolved entity, i.e. an entity definition and associated grant records, from the
   * backend store. The entity is identified by its id (entity catalog id and id).
   *
   * <p>For entities that can be grantees, the associated grant records will include both the grant
   * records for this entity as a grantee and for this entity as a securable.
   *
   * @param callCtx call context
   * @param entityCatalogId id of the catalog for that entity
   * @param entityId id of the entity
   * @return result with entity and grants. Status will be ENTITY_NOT_FOUND if the entity was not
   *     found
   */
  @Nonnull
  ResolvedEntityResult loadResolvedEntityById(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType);

  /**
   * Load a resolved entity, i.e. an entity definition and associated grant records, from the
   * backend store. The entity is identified by its name. Will return NULL if the entity does not
   * exist, i.e. has been purged or dropped.
   *
   * <p>For entities that can be grantees, the associated grant records will include both the grant
   * records for this entity as a grantee and for this entity as a securable.
   *
   * @param callCtx call context
   * @param entityCatalogId id of the catalog for that entity
   * @param parentId the id of the parent of that entity
   * @param entityType the type of this entity
   * @param entityName the name of this entity
   * @return result with entity and grants. Status will be ENTITY_NOT_FOUND if the entity was not
   *     found
   */
  @Nonnull
  ResolvedEntityResult loadResolvedEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull String entityName);

  /**
   * Refresh a resolved entity from the backend store. Will return NULL if the entity does not
   * exist, i.e. has been purged or dropped. Else, will determine what has changed based on the
   * version information sent by the caller and will return only what has changed.
   *
   * <p>For entities that can be grantees, the associated grant records will include both the grant
   * records for this entity as a grantee and for this entity as a securable.
   *
   * @param callCtx call context
   * @param entityType type of the entity whose entity and grants we are refreshing
   * @param entityCatalogId id of the catalog for that entity
   * @param entityId the id of the entity to load
   * @return result with entity and grants. Status will be ENTITY_NOT_FOUND if the entity was not
   *     found
   */
  @Nonnull
  ResolvedEntityResult refreshResolvedEntity(
      @Nonnull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      @Nonnull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId);

  /**
   * Check if the specified IcebergTableLikeEntity has any same-namespace siblings which share a
   * location
   *
   * @param callContext the polaris call context
   * @param entity the entity to check for overlapping siblings for
   * @return Optional.of(Optional.of ( location)) if the parent entity has children,
   *     Optional.of(Optional.empty()) if not, and Optional.empty() if the metastore doesn't support
   *     this operation
   */
  default <T extends PolarisEntity & LocationBasedEntity>
      Optional<Optional<String>> hasOverlappingSiblings(
          @Nonnull PolarisCallContext callContext, T entity) {
    return Optional.empty();
  }

  /**
   * Indicates whether this metastore manager implementation requires entities to be reloaded via
   * {@link #loadEntitiesChangeTracking} in order to ensure the most recent versions are obtained.
   *
   * <p>Generally this flag is {@code true} when entity caching is used.
   */
  default boolean requiresEntityReload() {
    return true;
  }
}
