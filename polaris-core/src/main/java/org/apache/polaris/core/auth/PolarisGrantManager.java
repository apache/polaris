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

import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

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
  @NonNull PrivilegeResult grantUsageOnRoleToGrantee(
      @NonNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NonNull PolarisEntityCore role,
      @NonNull PolarisEntityCore grantee);

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
  @NonNull PrivilegeResult revokeUsageOnRoleFromGrantee(
      @NonNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NonNull PolarisEntityCore role,
      @NonNull PolarisEntityCore grantee);

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
  @NonNull PrivilegeResult grantPrivilegeOnSecurableToRole(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityCore securable,
      @NonNull PolarisPrivilege privilege);

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
  @NonNull PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityCore securable,
      @NonNull PolarisPrivilege privilege);

  /**
   * This method should be used by the Polaris app to cache all grant records on a securable.
   *
   * @param callCtx call context
   * @param securable the securable entity
   * @return the list of grants and the version of the grant records. We will return
   *     ENTITY_NOT_FOUND if the securable cannot be found
   */
  @NonNull LoadGrantsResult loadGrantsOnSecurable(
      @NonNull PolarisCallContext callCtx, PolarisEntityCore securable);

  /**
   * This method should be used by the Polaris app to load all grants made to a grantee, either a
   * role or a principal.
   *
   * @param callCtx call context
   * @param grantee the grantee entity
   * @return the list of grants and the version of the grant records. We will return NULL if the
   *     grantee does not exist
   */
  @NonNull LoadGrantsResult loadGrantsToGrantee(
      @NonNull PolarisCallContext callCtx, PolarisEntityCore grantee);
}
