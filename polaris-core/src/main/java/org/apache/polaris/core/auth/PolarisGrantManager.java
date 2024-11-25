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
package org.apache.polaris.core.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.BaseResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Manage grants for Polaris entities. */
public interface PolarisGrantManager {
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
        @NotNull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.grantRecord = null;
    }

    /**
     * Constructor for success
     *
     * @param grantRecord grant record being granted or revoked
     */
    public PrivilegeResult(@NotNull PolarisGrantRecord grantRecord) {
      super(BaseResult.ReturnStatus.SUCCESS);
      this.grantRecord = grantRecord;
    }

    @JsonCreator
    private PrivilegeResult(
        @JsonProperty("returnStatus") @NotNull BaseResult.ReturnStatus returnStatus,
        @JsonProperty("extraInformation") String extraInformation,
        @JsonProperty("grantRecord") PolarisGrantRecord grantRecord) {
      super(returnStatus, extraInformation);
      this.grantRecord = grantRecord;
    }

    public PolarisGrantRecord getGrantRecord() {
      return grantRecord;
    }
  }

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
        @NotNull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
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
      super(BaseResult.ReturnStatus.SUCCESS);
      this.grantsVersion = grantsVersion;
      this.grantRecords = grantRecords;
      this.entities = entities;
    }

    @JsonCreator
    private LoadGrantsResult(
        @JsonProperty("returnStatus") @NotNull BaseResult.ReturnStatus returnStatus,
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
}
