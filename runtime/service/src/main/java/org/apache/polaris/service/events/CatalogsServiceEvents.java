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

package org.apache.polaris.service.events;

import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.entity.PolarisPrivilege;

public class CatalogsServiceEvents {
  public record BeforeCreateCatalogEvent(String catalogName) implements PolarisEvent {}

  public record AfterCreateCatalogEvent(Catalog catalog) implements PolarisEvent {}

  public record BeforeDeleteCatalogEvent(String catalogName) implements PolarisEvent {}

  public record AfterDeleteCatalogEvent(String catalogName) implements PolarisEvent {}

  public record BeforeGetCatalogEvent(String catalogName) implements PolarisEvent {}

  public record AfterGetCatalogEvent(Catalog catalog) implements PolarisEvent {}

  public record BeforeUpdateCatalogEvent(String catalogName, UpdateCatalogRequest updateRequest)
      implements PolarisEvent {}

  public record AfterUpdateCatalogEvent(Catalog catalog) implements PolarisEvent {}

  public record BeforeListCatalogEvent() implements PolarisEvent {}

  public record AfterListCatalogEvent() implements PolarisEvent {}

  public record BeforeCreateCatalogRoleEvent(String catalogName, String catalogRoleName)
      implements PolarisEvent {}

  public record AfterCreateCatalogRoleEvent(String catalogName, CatalogRole catalogRole)
      implements PolarisEvent {}

  public record BeforeDeleteCatalogRoleEvent(String catalogName, String catalogRoleName)
      implements PolarisEvent {}

  public record AfterDeleteCatalogRoleEvent(String catalogName, String catalogRoleName)
      implements PolarisEvent {}

  public record BeforeGetCatalogRoleEvent(String catalogName, String catalogRoleName)
      implements PolarisEvent {}

  public record AfterGetCatalogRoleEvent(String catalogName, CatalogRole catalogRole)
      implements PolarisEvent {}

  public record BeforeUpdateCatalogRoleEvent(
      String catalogName, String catalogRoleName, UpdateCatalogRoleRequest updateRequest)
      implements PolarisEvent {}

  public record AfterUpdateCatalogRoleEvent(String catalogName, CatalogRole updatedCatalogRole)
      implements PolarisEvent {}

  public record BeforeListCatalogRolesEvent(String catalogName) implements PolarisEvent {}

  public record AfterListCatalogRolesEvent(String catalogName) implements PolarisEvent {}

  /**
   * Event fired before a grant is added to a catalog role in Polaris.
   *
   * @param catalogName the name of the catalog
   * @param catalogRoleName the name of the catalog role
   * @param grantRequest the grant request
   */
  public record BeforeAddGrantToCatalogRoleEvent(
      String catalogName, String catalogRoleName, AddGrantRequest grantRequest)
      implements PolarisEvent {}

  /**
   * Event fired after a grant is added to a catalog role in Polaris.
   *
   * @param catalogName the name of the catalog
   * @param catalogRoleName the name of the catalog role
   * @param privilege the privilege granted
   * @param grantResource the grant resource
   */
  public record AfterAddGrantToCatalogRoleEvent(
      String catalogName,
      String catalogRoleName,
      PolarisPrivilege privilege,
      GrantResource grantResource)
      implements PolarisEvent {}

  /**
   * Event fired before a grant is revoked from a catalog role in Polaris.
   *
   * @param catalogName the name of the catalog
   * @param catalogRoleName the name of the catalog role
   * @param grantRequest the revoke grant request
   * @param cascade whether the revoke is cascading
   */
  public record BeforeRevokeGrantFromCatalogRoleEvent(
      String catalogName, String catalogRoleName, RevokeGrantRequest grantRequest, Boolean cascade)
      implements PolarisEvent {}

  /**
   * Event fired after a grant is revoked from a catalog role in Polaris.
   *
   * @param catalogName the name of the catalog
   * @param catalogRoleName the name of the catalog role
   * @param privilege the privilege revoked
   * @param grantResource the revoke grant resource
   * @param cascade whether to cascade the revocation
   */
  public record AfterRevokeGrantFromCatalogRoleEvent(
      String catalogName,
      String catalogRoleName,
      PolarisPrivilege privilege,
      GrantResource grantResource,
      Boolean cascade)
      implements PolarisEvent {}

  public record BeforeListAssigneePrincipalRolesForCatalogRoleEvent(
      String catalogName, String catalogRoleName) implements PolarisEvent {}

  public record AfterListAssigneePrincipalRolesForCatalogRoleEvent(
      String catalogName, String catalogRoleName) implements PolarisEvent {}

  public record BeforeListGrantsForCatalogRoleEvent(String catalogName, String catalogRoleName)
      implements PolarisEvent {}

  public record AfterListGrantsForCatalogRoleEvent(String catalogName, String catalogRoleName)
      implements PolarisEvent {}
}
