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
import org.apache.polaris.core.admin.model.CreatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;

public class PrincipalRolesServiceEvents {
  public record AfterCreatePrincipalRoleEvent(
      UUID id, PolarisEventMetadata metadata, PrincipalRole principalRole) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CREATE_PRINCIPAL_ROLE;
    }
  }

  public record BeforeCreatePrincipalRoleEvent(
      UUID id, PolarisEventMetadata metadata, CreatePrincipalRoleRequest createPrincipalRoleRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CREATE_PRINCIPAL_ROLE;
    }
  }

  public record AfterDeletePrincipalRoleEvent(
      UUID id, PolarisEventMetadata metadata, String principalRoleName) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_DELETE_PRINCIPAL_ROLE;
    }
  }

  public record BeforeDeletePrincipalRoleEvent(
      UUID id, PolarisEventMetadata metadata, String principalRoleName) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_DELETE_PRINCIPAL_ROLE;
    }
  }

  public record AfterGetPrincipalRoleEvent(
      UUID id, PolarisEventMetadata metadata, PrincipalRole principalRole) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_GET_PRINCIPAL_ROLE;
    }
  }

  public record BeforeGetPrincipalRoleEvent(
      UUID id, PolarisEventMetadata metadata, String principalRoleName) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_GET_PRINCIPAL_ROLE;
    }
  }

  public record AfterUpdatePrincipalRoleEvent(
      UUID id, PolarisEventMetadata metadata, PrincipalRole updatedPrincipalRole)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_UPDATE_PRINCIPAL_ROLE;
    }
  }

  public record BeforeUpdatePrincipalRoleEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String principalRoleName,
      UpdatePrincipalRoleRequest updateRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_UPDATE_PRINCIPAL_ROLE;
    }
  }

  public record AfterListPrincipalRolesEvent(UUID id, PolarisEventMetadata metadata)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_PRINCIPAL_ROLES;
    }
  }

  public record BeforeListPrincipalRolesEvent(UUID id, PolarisEventMetadata metadata)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_PRINCIPAL_ROLES;
    }
  }

  public record AfterAssignCatalogRoleToPrincipalRoleEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String principalRoleName,
      String catalogName,
      String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE;
    }
  }

  public record BeforeAssignCatalogRoleToPrincipalRoleEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String principalRoleName,
      String catalogName,
      String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE;
    }
  }

  public record AfterRevokeCatalogRoleFromPrincipalRoleEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String principalRoleName,
      String catalogName,
      String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_REVOKE_CATALOG_ROLE_FROM_PRINCIPAL_ROLE;
    }
  }

  public record BeforeRevokeCatalogRoleFromPrincipalRoleEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String principalRoleName,
      String catalogName,
      String catalogRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_REVOKE_CATALOG_ROLE_FROM_PRINCIPAL_ROLE;
    }
  }

  public record AfterListAssigneePrincipalsForPrincipalRoleEvent(
      UUID id, PolarisEventMetadata metadata, String principalRoleName) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_ASSIGNEE_PRINCIPALS_FOR_PRINCIPAL_ROLE;
    }
  }

  public record BeforeListAssigneePrincipalsForPrincipalRoleEvent(
      UUID id, PolarisEventMetadata metadata, String principalRoleName) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_ASSIGNEE_PRINCIPALS_FOR_PRINCIPAL_ROLE;
    }
  }

  public record AfterListCatalogRolesForPrincipalRoleEvent(
      UUID id, PolarisEventMetadata metadata, String principalRoleName, String catalogName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_CATALOG_ROLES_FOR_PRINCIPAL_ROLE;
    }
  }

  public record BeforeListCatalogRolesForPrincipalRoleEvent(
      UUID id, PolarisEventMetadata metadata, String principalRoleName, String catalogName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_CATALOG_ROLES_FOR_PRINCIPAL_ROLE;
    }
  }
}
