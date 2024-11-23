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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.PolarisGrantManager;
import org.apache.polaris.core.auth.PolarisSecretsManager;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityActiveRecord;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.cache.PolarisRemoteCache;
import org.apache.polaris.core.storage.PolarisCredentialVendor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Polaris Metastore Manager manages all Polaris entities and associated grant records metadata for
 * authorization. It uses the underlying persistent metastore to store and retrieve Polaris metadata
 */
public interface PolarisMetaStoreManager
    extends PolarisSecretsManager,
        PolarisGrantManager,
        PolarisRemoteCache,
        PolarisCredentialVendor {

  /**
   * Bootstrap the Polaris service, creating the root catalog, root principal, and associated
   * service admin role. Will fail if the service has already been bootstrapped.
   *
   * @param callCtx call context
   * @return the result of the bootstrap attempt
   */
  @NotNull
  BaseResult bootstrapPolarisService(@NotNull PolarisCallContext callCtx);

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
  @NotNull
  BaseResult purge(@NotNull PolarisCallContext callCtx);

  /** the return for an entity lookup call */
  class EntityResult extends BaseResult {

    // null if not success
    private final PolarisBaseEntity entity;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information if error. Implementation specific
     */
    public EntityResult(
        @NotNull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.entity = null;
    }

    /**
     * Constructor for success
     *
     * @param entity the entity being looked-up
     */
    public EntityResult(@NotNull PolarisBaseEntity entity) {
      super(ReturnStatus.SUCCESS);
      this.entity = entity;
    }

    /**
     * Constructor for an object already exists error where the subtype of the existing entity is
     * returned
     *
     * @param errorStatus error status, cannot be SUCCESS
     * @param subTypeCode existing entity subtype code
     */
    public EntityResult(@NotNull BaseResult.ReturnStatus errorStatus, int subTypeCode) {
      super(errorStatus, Integer.toString(subTypeCode));
      this.entity = null;
    }

    /**
     * For object already exist error, we use the extra information to serialize the subtype code of
     * the existing object. Return the subtype
     *
     * @return object subtype or NULL (should not happen) if subtype code is missing or cannot be
     *     deserialized
     */
    @Nullable
    public PolarisEntitySubType getAlreadyExistsEntitySubType() {
      if (this.getExtraInformation() == null) {
        return null;
      } else {
        int subTypeCode;
        try {
          subTypeCode = Integer.parseInt(this.getExtraInformation());
        } catch (NumberFormatException e) {
          return null;
        }
        return PolarisEntitySubType.fromCode(subTypeCode);
      }
    }

    @JsonCreator
    private EntityResult(
        @JsonProperty("returnStatus") @NotNull ReturnStatus returnStatus,
        @JsonProperty("extraInformation") @Nullable String extraInformation,
        @JsonProperty("entity") @Nullable PolarisBaseEntity entity) {
      super(returnStatus, extraInformation);
      this.entity = entity;
    }

    public PolarisBaseEntity getEntity() {
      return entity;
    }
  }

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
  @NotNull
  PolarisMetaStoreManager.EntityResult readEntityByName(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityType entityType,
      @NotNull PolarisEntitySubType entitySubType,
      @NotNull String name);

  /** the return the result for a list entities call */
  class ListEntitiesResult extends BaseResult {

    // null if not success. Else the list of entities being returned
    private final List<PolarisEntityActiveRecord> entities;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public ListEntitiesResult(
        @NotNull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.entities = null;
    }

    /**
     * Constructor for success
     *
     * @param entities list of entities being returned, implies success
     */
    public ListEntitiesResult(@NotNull List<PolarisEntityActiveRecord> entities) {
      super(ReturnStatus.SUCCESS);
      this.entities = entities;
    }

    @JsonCreator
    private ListEntitiesResult(
        @JsonProperty("returnStatus") @NotNull ReturnStatus returnStatus,
        @JsonProperty("extraInformation") String extraInformation,
        @JsonProperty("entities") List<PolarisEntityActiveRecord> entities) {
      super(returnStatus, extraInformation);
      this.entities = entities;
    }

    public List<PolarisEntityActiveRecord> getEntities() {
      return entities;
    }
  }

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
  @NotNull
  ListEntitiesResult listEntities(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityType entityType,
      @NotNull PolarisEntitySubType entitySubType);

  /** the return for a generate new entity id */
  class GenerateEntityIdResult extends BaseResult {

    // null if not success
    private final Long id;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public GenerateEntityIdResult(
        @NotNull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.id = null;
    }

    /**
     * Constructor for success
     *
     * @param id the new id which was generated
     */
    public GenerateEntityIdResult(@NotNull Long id) {
      super(ReturnStatus.SUCCESS);
      this.id = id;
    }

    @JsonCreator
    private GenerateEntityIdResult(
        @JsonProperty("returnStatus") @NotNull ReturnStatus returnStatus,
        @JsonProperty("extraInformation") @Nullable String extraInformation,
        @JsonProperty("id") @Nullable Long id) {
      super(returnStatus, extraInformation);
      this.id = id;
    }

    public Long getId() {
      return id;
    }
  }

  /**
   * Generate a new unique id that can be used by the Polaris client when it needs to create a new
   * entity
   *
   * @param callCtx call context
   * @return the newly created id, not expected to fail
   */
  @NotNull
  GenerateEntityIdResult generateNewEntityId(@NotNull PolarisCallContext callCtx);

  /** the return the result of a create-principal method */
  class CreatePrincipalResult extends BaseResult {
    // the principal which has been created. Null if error
    private final PolarisBaseEntity principal;

    // principal client identifier and associated secrets. Null if error
    private final PolarisPrincipalSecrets principalSecrets;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public CreatePrincipalResult(
        @NotNull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.principal = null;
      this.principalSecrets = null;
    }

    /**
     * Constructor for success
     *
     * @param principal the principal
     * @param principalSecrets and associated secret information
     */
    public CreatePrincipalResult(
        @NotNull PolarisBaseEntity principal, @NotNull PolarisPrincipalSecrets principalSecrets) {
      super(ReturnStatus.SUCCESS);
      this.principal = principal;
      this.principalSecrets = principalSecrets;
    }

    @JsonCreator
    private CreatePrincipalResult(
        @JsonProperty("returnStatus") @NotNull ReturnStatus returnStatus,
        @JsonProperty("extraInformation") @Nullable String extraInformation,
        @JsonProperty("principal") @NotNull PolarisBaseEntity principal,
        @JsonProperty("principalSecrets") @NotNull PolarisPrincipalSecrets principalSecrets) {
      super(returnStatus, extraInformation);
      this.principal = principal;
      this.principalSecrets = principalSecrets;
    }

    public PolarisBaseEntity getPrincipal() {
      return principal;
    }

    public PolarisPrincipalSecrets getPrincipalSecrets() {
      return principalSecrets;
    }
  }

  /**
   * Create a new principal. This not only creates the new principal entity but also generates a
   * client_id/secret pair for this new principal.
   *
   * @param callCtx call context
   * @param principal the principal entity to create
   * @return the client_id/secret for the new principal which was created. Will return
   *     ENTITY_ALREADY_EXISTS if the principal already exists
   */
  @NotNull
  CreatePrincipalResult createPrincipal(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity principal);

  /** the return the result of a create-catalog method */
  class CreateCatalogResult extends BaseResult {

    // the catalog which has been created
    private final PolarisBaseEntity catalog;

    // its associated catalog admin role
    private final PolarisBaseEntity catalogAdminRole;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public CreateCatalogResult(
        @NotNull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.catalog = null;
      this.catalogAdminRole = null;
    }

    /**
     * Constructor for success
     *
     * @param catalog the catalog
     * @param catalogAdminRole and associated admin role
     */
    public CreateCatalogResult(
        @NotNull PolarisBaseEntity catalog, @NotNull PolarisBaseEntity catalogAdminRole) {
      super(ReturnStatus.SUCCESS);
      this.catalog = catalog;
      this.catalogAdminRole = catalogAdminRole;
    }

    @JsonCreator
    private CreateCatalogResult(
        @JsonProperty("returnStatus") @NotNull ReturnStatus returnStatus,
        @JsonProperty("extraInformation") @Nullable String extraInformation,
        @JsonProperty("catalog") @NotNull PolarisBaseEntity catalog,
        @JsonProperty("catalogAdminRole") @NotNull PolarisBaseEntity catalogAdminRole) {
      super(returnStatus, extraInformation);
      this.catalog = catalog;
      this.catalogAdminRole = catalogAdminRole;
    }

    public PolarisBaseEntity getCatalog() {
      return catalog;
    }

    public PolarisBaseEntity getCatalogAdminRole() {
      return catalogAdminRole;
    }
  }

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
  @NotNull
  CreateCatalogResult createCatalog(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisBaseEntity catalog,
      @NotNull List<PolarisEntityCore> principalRoles);

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
  @NotNull
  EntityResult createEntityIfNotExists(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisBaseEntity entity);

  /** a set of returned entities result */
  class EntitiesResult extends BaseResult {

    // null if not success. Else the list of entities being returned
    private final List<PolarisBaseEntity> entities;

    /**
     * Constructor for an error
     *
     * @param errorStatus error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public EntitiesResult(
        @NotNull BaseResult.ReturnStatus errorStatus, @Nullable String extraInformation) {
      super(errorStatus, extraInformation);
      this.entities = null;
    }

    /**
     * Constructor for success
     *
     * @param entities list of entities being returned, implies success
     */
    public EntitiesResult(@NotNull List<PolarisBaseEntity> entities) {
      super(ReturnStatus.SUCCESS);
      this.entities = entities;
    }

    @JsonCreator
    private EntitiesResult(
        @JsonProperty("returnStatus") @NotNull ReturnStatus returnStatus,
        @JsonProperty("extraInformation") String extraInformation,
        @JsonProperty("entities") List<PolarisBaseEntity> entities) {
      super(returnStatus, extraInformation);
      this.entities = entities;
    }

    public List<PolarisBaseEntity> getEntities() {
      return entities;
    }
  }

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
  @NotNull
  EntitiesResult createEntitiesIfNotExist(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull List<? extends PolarisBaseEntity> entities);

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
  @NotNull
  EntityResult updateEntityPropertiesIfNotChanged(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisBaseEntity entity);

  /** Class to represent an entity with its path */
  class EntityWithPath {
    // path to that entity. Could be null if this entity is top-level
    private final @NotNull List<PolarisEntityCore> catalogPath;

    // the base entity itself
    private final @NotNull PolarisBaseEntity entity;

    @JsonCreator
    public EntityWithPath(
        @JsonProperty("catalogPath") @NotNull List<PolarisEntityCore> catalogPath,
        @JsonProperty("entity") @NotNull PolarisBaseEntity entity) {
      this.catalogPath = catalogPath;
      this.entity = entity;
    }

    public @NotNull List<PolarisEntityCore> getCatalogPath() {
      return catalogPath;
    }

    public @NotNull PolarisBaseEntity getEntity() {
      return entity;
    }
  }

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
  @NotNull
  EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @NotNull PolarisCallContext callCtx, @NotNull List<EntityWithPath> entities);

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
  @NotNull
  EntityResult renameEntity(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @NotNull PolarisEntity renamedEntity);

  // the return the result of a drop entity
  class DropEntityResult extends BaseResult {

    /** If cleanup was requested and a task was successfully scheduled, */
    private final Long cleanupTaskId;

    /**
     * Constructor for an error
     *
     * @param errorStatus error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public DropEntityResult(
        @NotNull BaseResult.ReturnStatus errorStatus, @Nullable String extraInformation) {
      super(errorStatus, extraInformation);
      this.cleanupTaskId = null;
    }

    /** Constructor for success when no cleanup needs to be performed */
    public DropEntityResult() {
      super(ReturnStatus.SUCCESS);
      this.cleanupTaskId = null;
    }

    /**
     * Constructor for success when a cleanup task has been scheduled
     *
     * @param cleanupTaskId id of the task which was created to clean up the table drop
     */
    public DropEntityResult(long cleanupTaskId) {
      super(ReturnStatus.SUCCESS);
      this.cleanupTaskId = cleanupTaskId;
    }

    @JsonCreator
    private DropEntityResult(
        @JsonProperty("returnStatus") @NotNull ReturnStatus returnStatus,
        @JsonProperty("extraInformation") String extraInformation,
        @JsonProperty("cleanupTaskId") Long cleanupTaskId) {
      super(returnStatus, extraInformation);
      this.cleanupTaskId = cleanupTaskId;
    }

    public Long getCleanupTaskId() {
      return cleanupTaskId;
    }

    @JsonIgnore
    public boolean failedBecauseNotEmpty() {
      ReturnStatus status = this.getReturnStatus();
      return status == ReturnStatus.CATALOG_NOT_EMPTY || status == ReturnStatus.NAMESPACE_NOT_EMPTY;
    }

    public boolean isEntityUnDroppable() {
      return this.getReturnStatus() == ReturnStatus.ENTITY_UNDROPPABLE;
    }
  }

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
  @NotNull
  DropEntityResult dropEntityIfExists(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore entityToDrop,
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
  @NotNull
  EntityResult loadEntity(@NotNull PolarisCallContext callCtx, long entityCatalogId, long entityId);

  /**
   * Fetch a list of tasks to be completed. Tasks
   *
   * @param callCtx call context
   * @param executorId executor id
   * @param limit limit
   * @return list of tasks to be completed
   */
  @NotNull
  EntitiesResult loadTasks(@NotNull PolarisCallContext callCtx, String executorId, int limit);
}
