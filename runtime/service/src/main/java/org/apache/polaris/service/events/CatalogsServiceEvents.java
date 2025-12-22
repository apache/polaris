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

  public record BeforeCreateCatalogEvent(PolarisEventMetadata metadata, String catalogName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CREATE_CATALOG;
    }
  }

  public record AfterCreateCatalogEvent(PolarisEventMetadata metadata, Catalog catalog)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CREATE_CATALOG;
    }
  }

  public record BeforeDeleteCatalogEvent(PolarisEventMetadata metadata, String catalogName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_DELETE_CATALOG;
    }
  }

  public record AfterDeleteCatalogEvent(PolarisEventMetadata metadata, String catalogName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_DELETE_CATALOG;
    }
  }

  public record BeforeGetCatalogEvent(PolarisEventMetadata metadata, String catalogName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_GET_CATALOG;
    }
  }

  public record AfterGetCatalogEvent(PolarisEventMetadata metadata, Catalog catalog)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_GET_CATALOG;
    }
  }

  public record BeforeUpdateCatalogEvent(
      PolarisEventMetadata metadata, String catalogName, UpdateCatalogRequest updateRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_UPDATE_CATALOG;
    }
  }

  public record AfterUpdateCatalogEvent(PolarisEventMetadata metadata, Catalog catalog)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_UPDATE_CATALOG;
    }
  }

  public record BeforeListCatalogsEvent(PolarisEventMetadata metadata) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_CATALOGS;
    }
  }

  public record AfterListCatalogsEvent(PolarisEventMetadata metadata) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_CATALOGS;
    }
  }

  public record BeforeCreateCatalogRoleEvent(
      PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CREATE_CATALOG_ROLE;
    }
  }

  public record AfterCreateCatalogRoleEvent(
      PolarisEventMetadata metadata, String catalogName, CatalogRole catalogRole)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CREATE_CATALOG_ROLE;
    }
  }

  public record BeforeDeleteCatalogRoleEvent(
      PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_DELETE_CATALOG_ROLE;
    }
  }

  public record AfterDeleteCatalogRoleEvent(
      PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_DELETE_CATALOG_ROLE;
    }
  }

  public record BeforeGetCatalogRoleEvent(
      PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_GET_CATALOG_ROLE;
    }
  }

  public record AfterGetCatalogRoleEvent(
      PolarisEventMetadata metadata, String catalogName, CatalogRole catalogRole)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_GET_CATALOG_ROLE;
    }
  }

  public record BeforeUpdateCatalogRoleEvent(
      PolarisEventMetadata metadata,
      String catalogName,
      String catalogRoleName,
      UpdateCatalogRoleRequest updateRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_UPDATE_CATALOG_ROLE;
    }
  }

  public record AfterUpdateCatalogRoleEvent(
      PolarisEventMetadata metadata, String catalogName, CatalogRole updatedCatalogRole)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_UPDATE_CATALOG_ROLE;
    }
  }

  public record BeforeListCatalogRolesEvent(PolarisEventMetadata metadata, String catalogName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_CATALOG_ROLES;
    }
  }

  public record AfterListCatalogRolesEvent(PolarisEventMetadata metadata, String catalogName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_CATALOG_ROLES;
    }
  }

  /**
   * Event fired before a grant is added to a catalog role in Polaris.
   *
   * @param catalogName the name of the catalog
   * @param catalogRoleName the name of the catalog role
   * @param grantRequest the grant request
   */
  public record BeforeAddGrantToCatalogRoleEvent(
      PolarisEventMetadata metadata,
      String catalogName,
      String catalogRoleName,
      AddGrantRequest grantRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_ADD_GRANT_TO_CATALOG_ROLE;
    }
  }

  /**
   * Event fired after a grant is added to a catalog role in Polaris.
   *
   * @param catalogName the name of the catalog
   * @param catalogRoleName the name of the catalog role
   * @param privilege the privilege granted
   * @param grantResource the grant resource
   */
  public record AfterAddGrantToCatalogRoleEvent(
      PolarisEventMetadata metadata,
      String catalogName,
      String catalogRoleName,
      PolarisPrivilege privilege,
      GrantResource grantResource)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_ADD_GRANT_TO_CATALOG_ROLE;
    }
  }

  /**
   * Event fired before a grant is revoked from a catalog role in Polaris.
   *
   * @param catalogName the name of the catalog
   * @param catalogRoleName the name of the catalog role
   * @param grantRequest the revoke grant request
   * @param cascade whether the revoke is cascading
   */
  public record BeforeRevokeGrantFromCatalogRoleEvent(
      PolarisEventMetadata metadata,
      String catalogName,
      String catalogRoleName,
      RevokeGrantRequest grantRequest,
      Boolean cascade)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_REVOKE_GRANT_FROM_CATALOG_ROLE;
    }
  }

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
      PolarisEventMetadata metadata,
      String catalogName,
      String catalogRoleName,
      PolarisPrivilege privilege,
      GrantResource grantResource,
      Boolean cascade)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_REVOKE_GRANT_FROM_CATALOG_ROLE;
    }
  }

  public record BeforeListAssigneePrincipalRolesForCatalogRoleEvent(
      PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_ASSIGNEE_PRINCIPAL_ROLES_FOR_CATALOG_ROLE;
    }
  }

  public record AfterListAssigneePrincipalRolesForCatalogRoleEvent(
      PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_ASSIGNEE_PRINCIPAL_ROLES_FOR_CATALOG_ROLE;
    }
  }

  public record BeforeListGrantsForCatalogRoleEvent(
      PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_GRANTS_FOR_CATALOG_ROLE;
    }
  }

  public record AfterListGrantsForCatalogRoleEvent(
      PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_GRANTS_FOR_CATALOG_ROLE;
    }
  }
}
