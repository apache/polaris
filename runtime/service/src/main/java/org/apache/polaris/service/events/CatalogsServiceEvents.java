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

import java.util.UUID;
import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.entity.PolarisPrivilege;

public class CatalogsServiceEvents {

  public record BeforeCreateCatalogEvent(UUID id, PolarisEventMetadata metadata, String catalogName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CREATE_CATALOG;
    }
  }

  public record AfterCreateCatalogEvent(UUID id, PolarisEventMetadata metadata, Catalog catalog)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CREATE_CATALOG;
    }
  }

  public record BeforeDeleteCatalogEvent(UUID id, PolarisEventMetadata metadata, String catalogName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_DELETE_CATALOG;
    }
  }

  public record AfterDeleteCatalogEvent(UUID id, PolarisEventMetadata metadata, String catalogName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_DELETE_CATALOG;
    }
  }

  public record BeforeGetCatalogEvent(UUID id, PolarisEventMetadata metadata, String catalogName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_GET_CATALOG;
    }
  }

  public record AfterGetCatalogEvent(UUID id, PolarisEventMetadata metadata, Catalog catalog)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_GET_CATALOG;
    }
  }

  public record BeforeUpdateCatalogEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      UpdateCatalogRequest updateRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_UPDATE_CATALOG;
    }
  }

  public record AfterUpdateCatalogEvent(UUID id, PolarisEventMetadata metadata, Catalog catalog)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_UPDATE_CATALOG;
    }
  }

  public record BeforeListCatalogsEvent(UUID id, PolarisEventMetadata metadata)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_CATALOGS;
    }
  }

  public record AfterListCatalogsEvent(UUID id, PolarisEventMetadata metadata)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_CATALOGS;
    }
  }

  public record BeforeCreateCatalogRoleEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CREATE_CATALOG_ROLE;
    }
  }

  public record AfterCreateCatalogRoleEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, CatalogRole catalogRole)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CREATE_CATALOG_ROLE;
    }
  }

  public record BeforeDeleteCatalogRoleEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_DELETE_CATALOG_ROLE;
    }
  }

  public record AfterDeleteCatalogRoleEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_DELETE_CATALOG_ROLE;
    }
  }

  public record BeforeGetCatalogRoleEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_GET_CATALOG_ROLE;
    }
  }

  public record AfterGetCatalogRoleEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, CatalogRole catalogRole)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_GET_CATALOG_ROLE;
    }
  }

  public record BeforeUpdateCatalogRoleEvent(
      UUID id,
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
      UUID id, PolarisEventMetadata metadata, String catalogName, CatalogRole updatedCatalogRole)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_UPDATE_CATALOG_ROLE;
    }
  }

  public record BeforeListCatalogRolesEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_CATALOG_ROLES;
    }
  }

  public record AfterListCatalogRolesEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName) implements PolarisEvent {
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
      UUID id,
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
      UUID id,
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
      UUID id,
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
      UUID id,
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
      UUID id, PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_ASSIGNEE_PRINCIPAL_ROLES_FOR_CATALOG_ROLE;
    }
  }

  public record AfterListAssigneePrincipalRolesForCatalogRoleEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_ASSIGNEE_PRINCIPAL_ROLES_FOR_CATALOG_ROLE;
    }
  }

  public record BeforeListGrantsForCatalogRoleEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_GRANTS_FOR_CATALOG_ROLE;
    }
  }

  public record AfterListGrantsForCatalogRoleEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_GRANTS_FOR_CATALOG_ROLE;
    }
  }
}
