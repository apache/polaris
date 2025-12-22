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

import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;

public class PrincipalsServiceEvents {
  public record AfterCreatePrincipalEvent(PolarisEventMetadata metadata, Principal principal)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CREATE_PRINCIPAL;
    }
  }

  public record BeforeCreatePrincipalEvent(PolarisEventMetadata metadata, String principalName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CREATE_PRINCIPAL;
    }
  }

  public record AfterDeletePrincipalEvent(PolarisEventMetadata metadata, String principalName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_DELETE_PRINCIPAL;
    }
  }

  public record BeforeDeletePrincipalEvent(PolarisEventMetadata metadata, String principalName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_DELETE_PRINCIPAL;
    }
  }

  public record AfterGetPrincipalEvent(PolarisEventMetadata metadata, Principal principal)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_GET_PRINCIPAL;
    }
  }

  public record BeforeGetPrincipalEvent(PolarisEventMetadata metadata, String principalName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_GET_PRINCIPAL;
    }
  }

  public record AfterUpdatePrincipalEvent(PolarisEventMetadata metadata, Principal principal)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_UPDATE_PRINCIPAL;
    }
  }

  public record BeforeUpdatePrincipalEvent(
      PolarisEventMetadata metadata,
      String principalName,
      UpdatePrincipalRequest updatePrincipalRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_UPDATE_PRINCIPAL;
    }
  }

  public record AfterRotateCredentialsEvent(
      PolarisEventMetadata metadata, Principal rotatedPrincipal) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_ROTATE_CREDENTIALS;
    }
  }

  public record BeforeRotateCredentialsEvent(PolarisEventMetadata metadata, String principalName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_ROTATE_CREDENTIALS;
    }
  }

  public record AfterListPrincipalsEvent(PolarisEventMetadata metadata) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_PRINCIPALS;
    }
  }

  public record BeforeListPrincipalsEvent(PolarisEventMetadata metadata) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_PRINCIPALS;
    }
  }

  public record AfterAssignPrincipalRoleEvent(
      PolarisEventMetadata metadata, String principalName, PrincipalRole principalRole)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_ASSIGN_PRINCIPAL_ROLE;
    }
  }

  public record BeforeAssignPrincipalRoleEvent(
      PolarisEventMetadata metadata, String principalName, PrincipalRole principalRole)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_ASSIGN_PRINCIPAL_ROLE;
    }
  }

  public record AfterRevokePrincipalRoleEvent(
      PolarisEventMetadata metadata, String principalName, String principalRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_REVOKE_PRINCIPAL_ROLE;
    }
  }

  public record BeforeRevokePrincipalRoleEvent(
      PolarisEventMetadata metadata, String principalName, String principalRoleName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_REVOKE_PRINCIPAL_ROLE;
    }
  }

  public record AfterListAssignedPrincipalRolesEvent(
      PolarisEventMetadata metadata, String principalName) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_ASSIGNED_PRINCIPAL_ROLES;
    }
  }

  public record BeforeListAssignedPrincipalRolesEvent(
      PolarisEventMetadata metadata, String principalName) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_ASSIGNED_PRINCIPAL_ROLES;
    }
  }

  public record BeforeResetCredentialsEvent(PolarisEventMetadata metadata, String principalName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_RESET_CREDENTIALS;
    }
  }

  public record AfterResetCredentialsEvent(PolarisEventMetadata metadata, Principal principal)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_RESET_CREDENTIALS;
    }
  }
}
