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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;

/**
 * Authorization request inputs for pre-authorization and core authorization.
 *
 * <p>This wrapper keeps authorization inputs together while preserving legacy semantics.
 */
public final class AuthorizationRequest {
  private final PolarisPrincipal principal;
  private final PolarisAuthorizableOperation operation;
  private final List<PolarisSecurable> targets;
  private final List<PolarisSecurable> secondaries;

  public AuthorizationRequest(
      @Nonnull PolarisPrincipal principal,
      @Nonnull PolarisAuthorizableOperation operation,
      @Nullable List<PolarisSecurable> targets,
      @Nullable List<PolarisSecurable> secondaries) {
    this.principal = principal;
    this.operation = operation;
    this.targets = targets;
    this.secondaries = secondaries;
  }

  public @Nonnull PolarisPrincipal getPrincipal() {
    return principal;
  }

  public @Nonnull PolarisAuthorizableOperation getOperation() {
    return operation;
  }

  public @Nullable List<PolarisSecurable> getTargets() {
    return targets;
  }

  public @Nullable List<PolarisSecurable> getSecondaries() {
    return secondaries;
  }

  public boolean isInternalPrincipalScope() {
    return hasSecurableType(PolarisEntityType.PRINCIPAL)
        || hasPrivilegeSecurableType(PolarisEntityType.PRINCIPAL)
        || hasPrivilegeNameForPrincipalScope();
  }

  public boolean isInternalPrincipalRoleScope() {
    return hasSecurableType(PolarisEntityType.PRINCIPAL_ROLE)
        || hasPrivilegeSecurableType(PolarisEntityType.PRINCIPAL_ROLE)
        || hasPrivilegeNameForPrincipalRoleScope();
  }

  public boolean isInternalCatalogRoleScope() {
    return hasSecurableType(PolarisEntityType.CATALOG_ROLE)
        || hasPrivilegeSecurableType(PolarisEntityType.CATALOG_ROLE)
        || hasPrivilegeNameForCatalogRoleScope();
  }

  private boolean hasSecurableType(PolarisEntityType... types) {
    if (targets != null && containsType(targets, types)) {
      return true;
    }
    return secondaries != null && containsType(secondaries, types);
  }

  private boolean hasPrivilegeSecurableType(PolarisEntityType... types) {
    return containsPrivilegeType(operation.getPrivilegesOnTarget(), types)
        || containsPrivilegeType(operation.getPrivilegesOnSecondary(), types);
  }

  private boolean hasPrivilegeNameForPrincipalScope() {
    return containsPrivilegeNameForPrincipalScope(operation.getPrivilegesOnTarget())
        || containsPrivilegeNameForPrincipalScope(operation.getPrivilegesOnSecondary());
  }

  private boolean hasPrivilegeNameForPrincipalRoleScope() {
    return containsPrivilegeNameForPrincipalRoleScope(operation.getPrivilegesOnTarget())
        || containsPrivilegeNameForPrincipalRoleScope(operation.getPrivilegesOnSecondary());
  }

  private boolean hasPrivilegeNameForCatalogRoleScope() {
    return containsPrivilegeNameForCatalogRoleScope(operation.getPrivilegesOnTarget())
        || containsPrivilegeNameForCatalogRoleScope(operation.getPrivilegesOnSecondary());
  }

  private static boolean containsPrivilegeType(
      Set<PolarisPrivilege> privileges, PolarisEntityType... types) {
    if (privileges == null || privileges.isEmpty()) {
      return false;
    }
    for (PolarisPrivilege privilege : privileges) {
      PolarisEntityType privilegeType = privilege.getSecurableType();
      for (PolarisEntityType type : types) {
        if (privilegeType == type) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean containsType(
      List<PolarisSecurable> securables, PolarisEntityType... types) {
    for (PolarisSecurable securable : securables) {
      PolarisEntityType entityType = securable.getEntityType();
      for (PolarisEntityType type : types) {
        if (entityType == type) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean containsPrivilegeNameForPrincipalScope(Set<PolarisPrivilege> privileges) {
    if (privileges == null || privileges.isEmpty()) {
      return false;
    }
    for (PolarisPrivilege privilege : privileges) {
      String name = privilege.name();
      if (name.startsWith("PRINCIPAL_ROLE_") || name.startsWith("CATALOG_ROLE_")) {
        continue;
      }
      if (name.startsWith("PRINCIPAL_")) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsPrivilegeNameForPrincipalRoleScope(
      Set<PolarisPrivilege> privileges) {
    if (privileges == null || privileges.isEmpty()) {
      return false;
    }
    for (PolarisPrivilege privilege : privileges) {
      String name = privilege.name();
      if (name.startsWith("PRINCIPAL_ROLE_")) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsPrivilegeNameForCatalogRoleScope(
      Set<PolarisPrivilege> privileges) {
    if (privileges == null || privileges.isEmpty()) {
      return false;
    }
    for (PolarisPrivilege privilege : privileges) {
      String name = privilege.name();
      if (name.startsWith("CATALOG_ROLE_")) {
        return true;
      }
    }
    return false;
  }
}
