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
package org.apache.polaris.core.persistence.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.BaseResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Interface to the remote entity cache. This allows the local cache to detect remote entity changes
 * and refresh the local copies where necessary.
 */
public interface PolarisRemoteCache {
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
  CachedEntryResult loadCachedEntryById(
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
  CachedEntryResult loadCachedEntryByName(
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
  CachedEntryResult refreshCachedEntity(
      @NotNull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      @NotNull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId);

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
        @NotNull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
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
      super(BaseResult.ReturnStatus.SUCCESS);
      this.changeTrackingVersions = changeTrackingVersions;
    }

    @JsonCreator
    private ChangeTrackingResult(
        @JsonProperty("returnStatus") @NotNull BaseResult.ReturnStatus returnStatus,
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
        @NotNull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
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
      super(BaseResult.ReturnStatus.SUCCESS);
      this.entity = entity;
      this.entityGrantRecords = entityGrantRecords;
      this.grantRecordsVersion = grantRecordsVersion;
    }

    @JsonCreator
    public CachedEntryResult(
        @JsonProperty("returnStatus") @NotNull BaseResult.ReturnStatus returnStatus,
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
}
