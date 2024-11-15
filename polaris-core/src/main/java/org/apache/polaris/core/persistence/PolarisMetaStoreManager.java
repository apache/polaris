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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityActiveRecord;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Polaris Metastore Manager manages all Polaris entities and associated grant records metadata for
 * authorization. It uses the underlying persistent metastore to store and retrieve Polaris metadata
 */
public interface PolarisMetaStoreManager {

  /** Possible return code for the various API calls. */
  enum ReturnStatus {
    // all good
    SUCCESS(1),

    // an unexpected error was thrown, should result in a 500 error to the client
    UNEXPECTED_ERROR_SIGNALED(2),

    // the specified catalog path cannot be resolved. There is a possibility that by the time a call
    // is made by the client to the persistent storage, something has changed due to concurrent
    // modification(s). The client should retry in that case.
    CATALOG_PATH_CANNOT_BE_RESOLVED(3),

    // the specified entity (and its path) cannot be resolved. There is a possibility that by the
    // time a call is made by the client to the persistent storage, something has changed due to
    // concurrent modification(s). The client should retry in that case.
    ENTITY_CANNOT_BE_RESOLVED(4),

    // entity not found
    ENTITY_NOT_FOUND(5),

    // grant not found
    GRANT_NOT_FOUND(6),

    // entity already exists
    ENTITY_ALREADY_EXISTS(7),

    // entity cannot be dropped, it is one of the bootstrap object like a catalog admin role or the
    // service admin principal role
    ENTITY_UNDROPPABLE(8),

    // Namespace is not empty and cannot be dropped
    NAMESPACE_NOT_EMPTY(9),

    // Catalog is not empty and cannot be dropped. All catalog roles (except the admin catalog
    // role) and all namespaces in the catalog must be dropped before the namespace can be dropped
    CATALOG_NOT_EMPTY(10),

    // The target entity was concurrently modified
    TARGET_ENTITY_CONCURRENTLY_MODIFIED(11),

    // entity cannot be renamed
    ENTITY_CANNOT_BE_RENAMED(12),

    // error caught while sub-scoping credentials. Error message will be returned
    SUBSCOPE_CREDS_ERROR(13),
    ;

    // code for the enum
    private final int code;

    /** constructor */
    ReturnStatus(int code) {
      this.code = code;
    }

    int getCode() {
      return this.code;
    }

    // to efficiently map a code to its corresponding return status
    private static final ReturnStatus[] REVERSE_MAPPING_ARRAY;

    static {
      // find max array size
      int maxCode = 0;
      for (ReturnStatus returnStatus : ReturnStatus.values()) {
        if (maxCode < returnStatus.code) {
          maxCode = returnStatus.code;
        }
      }

      // allocate mapping array
      REVERSE_MAPPING_ARRAY = new ReturnStatus[maxCode + 1];

      // populate mapping array
      for (ReturnStatus returnStatus : ReturnStatus.values()) {
        REVERSE_MAPPING_ARRAY[returnStatus.code] = returnStatus;
      }
    }

    static ReturnStatus getStatus(int code) {
      return code >= REVERSE_MAPPING_ARRAY.length ? null : REVERSE_MAPPING_ARRAY[code];
    }
  }

  /** Base result class for any call to the persistence layer */
  class BaseResult {
    // return code, indicates success or failure
    private final int returnStatusCode;

    // additional information for some error return code
    private final String extraInformation;

    public BaseResult() {
      this.returnStatusCode = ReturnStatus.SUCCESS.getCode();
      this.extraInformation = null;
    }

    public BaseResult(@NotNull PolarisMetaStoreManager.ReturnStatus returnStatus) {
      this.returnStatusCode = returnStatus.getCode();
      this.extraInformation = null;
    }

    @JsonCreator
    public BaseResult(
        @JsonProperty("returnStatus") @NotNull ReturnStatus returnStatus,
        @JsonProperty("extraInformation") @Nullable String extraInformation) {
      this.returnStatusCode = returnStatus.getCode();
      this.extraInformation = extraInformation;
    }

    public ReturnStatus getReturnStatus() {
      return ReturnStatus.getStatus(this.returnStatusCode);
    }

    public String getExtraInformation() {
      return extraInformation;
    }

    public boolean isSuccess() {
      return this.returnStatusCode == ReturnStatus.SUCCESS.getCode();
    }

    public boolean alreadyExists() {
      return this.returnStatusCode == ReturnStatus.ENTITY_ALREADY_EXISTS.getCode();
    }
  }

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
        @NotNull PolarisMetaStoreManager.ReturnStatus errorCode,
        @Nullable String extraInformation) {
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
    public EntityResult(
        @NotNull PolarisMetaStoreManager.ReturnStatus errorStatus, int subTypeCode) {
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
        @NotNull PolarisMetaStoreManager.ReturnStatus errorCode,
        @Nullable String extraInformation) {
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
        @NotNull PolarisMetaStoreManager.ReturnStatus errorCode,
        @Nullable String extraInformation) {
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
        @NotNull PolarisMetaStoreManager.ReturnStatus errorCode,
        @Nullable String extraInformation) {
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

  /** the result of load/rotate principal secrets */
  class PrincipalSecretsResult extends BaseResult {

    // principal client identifier and associated secrets. Null if error
    private final PolarisPrincipalSecrets principalSecrets;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public PrincipalSecretsResult(
        @NotNull PolarisMetaStoreManager.ReturnStatus errorCode,
        @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.principalSecrets = null;
    }

    /**
     * Constructor for success
     *
     * @param principalSecrets and associated secret information
     */
    public PrincipalSecretsResult(@NotNull PolarisPrincipalSecrets principalSecrets) {
      super(ReturnStatus.SUCCESS);
      this.principalSecrets = principalSecrets;
    }

    @JsonCreator
    private PrincipalSecretsResult(
        @JsonProperty("returnStatus") @NotNull ReturnStatus returnStatus,
        @JsonProperty("extraInformation") @Nullable String extraInformation,
        @JsonProperty("principalSecrets") @NotNull PolarisPrincipalSecrets principalSecrets) {
      super(returnStatus, extraInformation);
      this.principalSecrets = principalSecrets;
    }

    public PolarisPrincipalSecrets getPrincipalSecrets() {
      return principalSecrets;
    }
  }

  /**
   * Load the principal secrets given the client_id.
   *
   * @param callCtx call context
   * @param clientId principal client id
   * @return the secrets associated to that principal, including the entity id of the principal
   */
  @NotNull
  PrincipalSecretsResult loadPrincipalSecrets(
      @NotNull PolarisCallContext callCtx, @NotNull String clientId);

  /**
   * Rotate secrets
   *
   * @param callCtx call context
   * @param clientId principal client id
   * @param principalId id of the principal
   * @param reset true if the principal's secrets should be disabled and replaced with a one-time
   *     password. if the principal's secret is already a one-time password, this flag is
   *     automatically true
   * @param oldSecretHash main secret hash for the principal
   * @return the secrets associated to that principal amd the id of the principal
   */
  @NotNull
  PrincipalSecretsResult rotatePrincipalSecrets(
      @NotNull PolarisCallContext callCtx,
      @NotNull String clientId,
      long principalId,
      boolean reset,
      @NotNull String oldSecretHash);

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
        @NotNull PolarisMetaStoreManager.ReturnStatus errorCode,
        @Nullable String extraInformation) {
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
        @NotNull PolarisMetaStoreManager.ReturnStatus errorStatus,
        @Nullable String extraInformation) {
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
        @NotNull PolarisMetaStoreManager.ReturnStatus errorStatus,
        @Nullable String extraInformation) {
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

  /** Result of a grant/revoke privilege call */
  class PrivilegeResult extends BaseResult {

    // null if not success.
    private final PolarisGrantRecord grantRecord;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public PrivilegeResult(
        @NotNull PolarisMetaStoreManager.ReturnStatus errorCode,
        @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.grantRecord = null;
    }

    /**
     * Constructor for success
     *
     * @param grantRecord grant record being granted or revoked
     */
    public PrivilegeResult(@NotNull PolarisGrantRecord grantRecord) {
      super(ReturnStatus.SUCCESS);
      this.grantRecord = grantRecord;
    }

    @JsonCreator
    private PrivilegeResult(
        @JsonProperty("returnStatus") @NotNull ReturnStatus returnStatus,
        @JsonProperty("extraInformation") String extraInformation,
        @JsonProperty("grantRecord") PolarisGrantRecord grantRecord) {
      super(returnStatus, extraInformation);
      this.grantRecord = grantRecord;
    }

    public PolarisGrantRecord getGrantRecord() {
      return grantRecord;
    }
  }

  /**
   * Grant usage on a role to a grantee, for example granting usage on a catalog role to a principal
   * role or granting a principal role to a principal.
   *
   * @param callCtx call context
   * @param catalog if the role is a catalog role, the caller needs to pass-in the catalog entity
   *     which was used to resolve that granted. Else null.
   * @param role resolved catalog or principal role
   * @param grantee principal role or principal as resolved by the caller
   * @return the grant record we created for this grant. Will return ENTITY_NOT_FOUND if the
   *     specified role couldn't be found. Should be retried in that case
   */
  @NotNull
  PrivilegeResult grantUsageOnRoleToGrantee(
      @NotNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NotNull PolarisEntityCore role,
      @NotNull PolarisEntityCore grantee);

  /**
   * Revoke usage on a role (a catalog or a principal role) from a grantee (e.g. a principal role or
   * a principal).
   *
   * @param callCtx call context
   * @param catalog if the granted is a catalog role, the caller needs to pass-in the catalog entity
   *     which was used to resolve that role. Else null should be passed-in.
   * @param role a catalog/principal role as resolved by the caller
   * @param grantee resolved principal role or principal
   * @return the result. Will return ENTITY_NOT_FOUND if the * specified role couldn't be found.
   *     Should be retried in that case. Will return GRANT_NOT_FOUND if the grant to revoke cannot
   *     be found
   */
  @NotNull
  PrivilegeResult revokeUsageOnRoleFromGrantee(
      @NotNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NotNull PolarisEntityCore role,
      @NotNull PolarisEntityCore grantee);

  /**
   * Grant a privilege on a catalog securable to a grantee.
   *
   * @param callCtx call context
   * @param grantee resolved role, the grantee
   * @param catalogPath path to that entity, cannot be null or empty unless securable is top-level
   * @param securable securable entity, must have been resolved by the client. Can be the catalog
   *     itself
   * @param privilege privilege to grant
   * @return the grant record we created for this grant. Will return ENTITY_NOT_FOUND if the
   *     specified role couldn't be found. Should be retried in that case
   */
  @NotNull
  PrivilegeResult grantPrivilegeOnSecurableToRole(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore securable,
      @NotNull PolarisPrivilege privilege);

  /**
   * Revoke a privilege on a catalog securable from a grantee.
   *
   * @param callCtx call context
   * @param grantee resolved role, the grantee
   * @param catalogPath path to that entity, cannot be null or empty unless securable is top-level
   * @param securable securable entity, must have been resolved by the client. Can be the catalog
   *     itself.
   * @param privilege privilege to revoke
   * @return the result. Will return ENTITY_NOT_FOUND if the * specified role couldn't be found.
   *     Should be retried in that case. Will return GRANT_NOT_FOUND if the grant to revoke cannot
   *     be found
   */
  @NotNull
  PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore securable,
      @NotNull PolarisPrivilege privilege);

  /** Result of a load grants call */
  class LoadGrantsResult extends BaseResult {
    // true if success. If false, the caller should retry because of some concurrent change
    private final int grantsVersion;

    // null if not success. Else set of grants records on a securable or to a grantee
    private final List<PolarisGrantRecord> grantRecords;

    // null if not success. Else, for each grant record, list of securable or grantee entities
    private final List<PolarisBaseEntity> entities;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public LoadGrantsResult(
        @NotNull PolarisMetaStoreManager.ReturnStatus errorCode,
        @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.grantsVersion = 0;
      this.grantRecords = null;
      this.entities = null;
    }

    /**
     * Constructor for success
     *
     * @param grantsVersion version of the grants
     * @param grantRecords set of grant records
     */
    public LoadGrantsResult(
        int grantsVersion,
        @NotNull List<PolarisGrantRecord> grantRecords,
        List<PolarisBaseEntity> entities) {
      super(ReturnStatus.SUCCESS);
      this.grantsVersion = grantsVersion;
      this.grantRecords = grantRecords;
      this.entities = entities;
    }

    @JsonCreator
    private LoadGrantsResult(
        @JsonProperty("returnStatus") @NotNull ReturnStatus returnStatus,
        @JsonProperty("extraInformation") String extraInformation,
        @JsonProperty("grantsVersion") int grantsVersion,
        @JsonProperty("grantRecords") List<PolarisGrantRecord> grantRecords,
        @JsonProperty("entities") List<PolarisBaseEntity> entities) {
      super(returnStatus, extraInformation);
      this.grantsVersion = grantsVersion;
      this.grantRecords = grantRecords;
      // old GS code might not serialize this argument
      this.entities = entities;
    }

    public int getGrantsVersion() {
      return grantsVersion;
    }

    public List<PolarisGrantRecord> getGrantRecords() {
      return grantRecords;
    }

    public List<PolarisBaseEntity> getEntities() {
      return entities;
    }

    @JsonIgnore
    public Map<Long, PolarisBaseEntity> getEntitiesAsMap() {
      return (this.getEntities() == null)
          ? null
          : this.getEntities().stream()
              .collect(Collectors.toMap(PolarisBaseEntity::getId, entity -> entity));
    }

    @Override
    public String toString() {
      return "LoadGrantsResult{"
          + "grantsVersion="
          + grantsVersion
          + ", grantRecords="
          + grantRecords
          + ", entities="
          + entities
          + ", returnStatus="
          + getReturnStatus()
          + '}';
    }
  }

  /**
   * This method should be used by the Polaris app to cache all grant records on a securable.
   *
   * @param callCtx call context
   * @param securableCatalogId id of the catalog this securable belongs to
   * @param securableId id of the securable
   * @return the list of grants and the version of the grant records. We will return
   *     ENTITY_NOT_FOUND if the securable cannot be found
   */
  @NotNull
  LoadGrantsResult loadGrantsOnSecurable(
      @NotNull PolarisCallContext callCtx, long securableCatalogId, long securableId);

  /**
   * This method should be used by the Polaris app to load all grants made to a grantee, either a
   * role or a principal.
   *
   * @param callCtx call context
   * @param granteeCatalogId id of the catalog this grantee belongs to
   * @param granteeId id of the grantee
   * @return the list of grants and the version of the grant records. We will return NULL if the
   *     grantee does not exist
   */
  @NotNull
  LoadGrantsResult loadGrantsToGrantee(
      PolarisCallContext callCtx, long granteeCatalogId, long granteeId);

  /** Result of a loadEntitiesChangeTracking call */
  class ChangeTrackingResult extends BaseResult {

    // null if not success. Else, will be null if the grant to revoke was not found
    private final List<PolarisChangeTrackingVersions> changeTrackingVersions;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public ChangeTrackingResult(
        @NotNull PolarisMetaStoreManager.ReturnStatus errorCode,
        @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.changeTrackingVersions = null;
    }

    /**
     * Constructor for success
     *
     * @param changeTrackingVersions change tracking versions
     */
    public ChangeTrackingResult(
        @NotNull List<PolarisChangeTrackingVersions> changeTrackingVersions) {
      super(ReturnStatus.SUCCESS);
      this.changeTrackingVersions = changeTrackingVersions;
    }

    @JsonCreator
    private ChangeTrackingResult(
        @JsonProperty("returnStatus") @NotNull ReturnStatus returnStatus,
        @JsonProperty("extraInformation") String extraInformation,
        @JsonProperty("changeTrackingVersions")
            List<PolarisChangeTrackingVersions> changeTrackingVersions) {
      super(returnStatus, extraInformation);
      this.changeTrackingVersions = changeTrackingVersions;
    }

    public List<PolarisChangeTrackingVersions> getChangeTrackingVersions() {
      return changeTrackingVersions;
    }
  }

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
  @NotNull
  ChangeTrackingResult loadEntitiesChangeTracking(
      @NotNull PolarisCallContext callCtx, @NotNull List<PolarisEntityId> entityIds);

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

  /** Result of a getSubscopedCredsForEntity() call */
  class ScopedCredentialsResult extends BaseResult {

    // null if not success. Else, set of name/value pairs for the credentials
    private final EnumMap<PolarisCredentialProperty, String> credentials;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public ScopedCredentialsResult(
        @NotNull PolarisMetaStoreManager.ReturnStatus errorCode,
        @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.credentials = null;
    }

    /**
     * Constructor for success
     *
     * @param credentials credentials
     */
    public ScopedCredentialsResult(
        @NotNull EnumMap<PolarisCredentialProperty, String> credentials) {
      super(ReturnStatus.SUCCESS);
      this.credentials = credentials;
    }

    @JsonCreator
    private ScopedCredentialsResult(
        @JsonProperty("returnStatus") @NotNull ReturnStatus returnStatus,
        @JsonProperty("extraInformation") String extraInformation,
        @JsonProperty("credentials") Map<String, String> credentials) {
      super(returnStatus, extraInformation);
      this.credentials = new EnumMap<>(PolarisCredentialProperty.class);
      if (credentials != null) {
        credentials.forEach(
            (k, v) -> this.credentials.put(PolarisCredentialProperty.valueOf(k), v));
      }
    }

    public EnumMap<PolarisCredentialProperty, String> getCredentials() {
      return credentials;
    }
  }

  /**
   * Get a sub-scoped credentials for an entity against the provided allowed read and write
   * locations.
   *
   * @param callCtx the polaris call context
   * @param catalogId the catalog id
   * @param entityId the entity id
   * @param allowListOperation whether to allow LIST operation on the allowedReadLocations and
   *     allowedWriteLocations
   * @param allowedReadLocations a set of allowed to read locations
   * @param allowedWriteLocations a set of allowed to write locations
   * @return an enum map containing the scoped credentials
   */
  @NotNull
  ScopedCredentialsResult getSubscopedCredsForEntity(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      boolean allowListOperation,
      @NotNull Set<String> allowedReadLocations,
      @NotNull Set<String> allowedWriteLocations);

  /** Result of a validateAccessToLocations() call */
  class ValidateAccessResult extends BaseResult {

    // null if not success. Else, set of location/validationResult pairs for each location in the
    // set
    private final Map<String, String> validateResult;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public ValidateAccessResult(
        @NotNull PolarisMetaStoreManager.ReturnStatus errorCode,
        @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.validateResult = null;
    }

    /**
     * Constructor for success
     *
     * @param validateResult validate result
     */
    public ValidateAccessResult(@NotNull Map<String, String> validateResult) {
      super(ReturnStatus.SUCCESS);
      this.validateResult = validateResult;
    }

    @JsonCreator
    private ValidateAccessResult(
        @JsonProperty("returnStatus") @NotNull ReturnStatus returnStatus,
        @JsonProperty("extraInformation") String extraInformation,
        @JsonProperty("validateResult") Map<String, String> validateResult) {
      super(returnStatus, extraInformation);
      this.validateResult = validateResult;
    }

    public Map<String, String> getValidateResult() {
      return this.validateResult;
    }
  }

  /**
   * Validate whether the entity has access to the locations with the provided target operations
   *
   * @param callCtx the polaris call context
   * @param catalogId the catalog id
   * @param entityId the entity id
   * @param actions a set of operation actions: READ/WRITE/LIST/DELETE/ALL
   * @param locations a set of locations to verify
   * @return a Map of {@code <location, validate result>}, a validate result value looks like this
   *     <pre>
   * {
   *   "status" : "failure",
   *   "actions" : {
   *     "READ" : {
   *       "message" : "The specified file was not found",
   *       "status" : "failure"
   *     },
   *     "DELETE" : {
   *       "message" : "One or more objects could not be deleted (Status Code: 200; Error Code: null)",
   *       "status" : "failure"
   *     },
   *     "LIST" : {
   *       "status" : "success"
   *     },
   *     "WRITE" : {
   *       "message" : "Access Denied (Status Code: 403; Error Code: AccessDenied)",
   *       "status" : "failure"
   *     }
   *   },
   *   "message" : "Some of the integration checks failed. Check the Polaris documentation for more information."
   * }
   * </pre>
   */
  @NotNull
  ValidateAccessResult validateAccessToLocations(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      @NotNull Set<PolarisStorageActions> actions,
      @NotNull Set<String> locations);

  /**
   * Represents an entry in the cache. If we refresh a cached entry, we will only refresh the
   * information which have changed, based on the version of the entity
   */
  class CachedEntryResult extends BaseResult {

    // the entity itself if it was loaded
    private final @Nullable PolarisBaseEntity entity;

    // version for the grant records, in case the entity was not loaded
    private final int grantRecordsVersion;

    private final @Nullable List<PolarisGrantRecord> entityGrantRecords;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public CachedEntryResult(
        @NotNull PolarisMetaStoreManager.ReturnStatus errorCode,
        @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.entity = null;
      this.entityGrantRecords = null;
      this.grantRecordsVersion = 0;
    }

    /**
     * Constructor with success
     *
     * @param entity the entity for that cached entry
     * @param grantRecordsVersion the version of the grant records
     * @param entityGrantRecords the list of grant records
     */
    public CachedEntryResult(
        @Nullable PolarisBaseEntity entity,
        int grantRecordsVersion,
        @Nullable List<PolarisGrantRecord> entityGrantRecords) {
      super(ReturnStatus.SUCCESS);
      this.entity = entity;
      this.entityGrantRecords = entityGrantRecords;
      this.grantRecordsVersion = grantRecordsVersion;
    }

    @JsonCreator
    public CachedEntryResult(
        @JsonProperty("returnStatus") @NotNull ReturnStatus returnStatus,
        @JsonProperty("extraInformation") String extraInformation,
        @Nullable @JsonProperty("entity") PolarisBaseEntity entity,
        @JsonProperty("grantRecordsVersion") int grantRecordsVersion,
        @Nullable @JsonProperty("entityGrantRecords") List<PolarisGrantRecord> entityGrantRecords) {
      super(returnStatus, extraInformation);
      this.entity = entity;
      this.entityGrantRecords = entityGrantRecords;
      this.grantRecordsVersion = grantRecordsVersion;
    }

    public @Nullable PolarisBaseEntity getEntity() {
      return entity;
    }

    public int getGrantRecordsVersion() {
      return grantRecordsVersion;
    }

    public @Nullable List<PolarisGrantRecord> getEntityGrantRecords() {
      return entityGrantRecords;
    }
  }

  /**
   * Load a cached entry, i.e. an entity definition and associated grant records, from the backend
   * store. The entity is identified by its id (entity catalog id and id).
   *
   * <p>For entities that can be grantees, the associated grant records will include both the grant
   * records for this entity as a grantee and for this entity as a securable.
   *
   * @param callCtx call context
   * @param entityCatalogId id of the catalog for that entity
   * @param entityId id of the entity
   * @return cached entry for this entity. Status will be ENTITY_NOT_FOUND if the entity was not
   *     found
   */
  @NotNull
  PolarisMetaStoreManager.CachedEntryResult loadCachedEntryById(
      @NotNull PolarisCallContext callCtx, long entityCatalogId, long entityId);

  /**
   * Load a cached entry, i.e. an entity definition and associated grant records, from the backend
   * store. The entity is identified by its name. Will return NULL if the entity does not exist,
   * i.e. has been purged or dropped.
   *
   * <p>For entities that can be grantees, the associated grant records will include both the grant
   * records for this entity as a grantee and for this entity as a securable.
   *
   * @param callCtx call context
   * @param entityCatalogId id of the catalog for that entity
   * @param parentId the id of the parent of that entity
   * @param entityType the type of this entity
   * @param entityName the name of this entity
   * @return cached entry for this entity. Status will be ENTITY_NOT_FOUND if the entity was not
   *     found
   */
  @NotNull
  PolarisMetaStoreManager.CachedEntryResult loadCachedEntryByName(
      @NotNull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @NotNull PolarisEntityType entityType,
      @NotNull String entityName);

  /**
   * Refresh a cached entity from the backend store. Will return NULL if the entity does not exist,
   * i.e. has been purged or dropped. Else, will determine what has changed based on the version
   * information sent by the caller and will return only what has changed.
   *
   * <p>For entities that can be grantees, the associated grant records will include both the grant
   * records for this entity as a grantee and for this entity as a securable.
   *
   * @param callCtx call context
   * @param entityType type of the entity whose cached entry we are refreshing
   * @param entityCatalogId id of the catalog for that entity
   * @param entityId the id of the entity to load
   * @return cached entry for this entity. Status will be ENTITY_NOT_FOUND if the entity was not *
   *     found
   */
  @NotNull
  PolarisMetaStoreManager.CachedEntryResult refreshCachedEntity(
      @NotNull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      @NotNull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId);
}
