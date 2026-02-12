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
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Authorization request inputs for pre-authorization and core authorization.
 *
 * <p>This wrapper keeps authorization inputs together while preserving legacy semantics.
 */
@PolarisImmutable
public interface AuthorizationRequest {
  static AuthorizationRequest of(
      @Nonnull PolarisPrincipal principal,
      @Nonnull PolarisAuthorizableOperation operation,
      @Nullable List<PolarisSecurable> targets,
      @Nullable List<PolarisSecurable> secondaries) {
    return ImmutableAuthorizationRequest.builder()
        .principal(principal)
        .operation(operation)
        .targets(targets)
        .secondaries(secondaries)
        .build();
  }

  @Nonnull
  PolarisPrincipal getPrincipal();

  @Nonnull
  PolarisAuthorizableOperation getOperation();

  @Nullable
  List<PolarisSecurable> getTargets();

  @Nullable
  List<PolarisSecurable> getSecondaries();

  default boolean isInternalPrincipalScope() {
    return hasSecurableType(PolarisEntityType.PRINCIPAL)
        || hasPrivilegeSecurableType(PolarisEntityType.PRINCIPAL)
        || hasPrivilegeNameForPrincipalScope();
  }

  default boolean isInternalPrincipalRoleScope() {
    return hasSecurableType(PolarisEntityType.PRINCIPAL_ROLE)
        || hasPrivilegeSecurableType(PolarisEntityType.PRINCIPAL_ROLE)
        || hasPrivilegeNameForPrincipalRoleScope();
  }

  default boolean isInternalCatalogRoleScope() {
    return hasSecurableType(PolarisEntityType.CATALOG_ROLE)
        || hasPrivilegeSecurableType(PolarisEntityType.CATALOG_ROLE)
        || hasPrivilegeNameForCatalogRoleScope();
  }

  private boolean hasSecurableType(PolarisEntityType... types) {
    if (getTargets() != null && containsType(getTargets(), types)) {
      return true;
    }
    return getSecondaries() != null && containsType(getSecondaries(), types);
  }

  private boolean hasPrivilegeSecurableType(PolarisEntityType... types) {
    return containsPrivilegeType(getOperation().getPrivilegesOnTarget(), types)
        || containsPrivilegeType(getOperation().getPrivilegesOnSecondary(), types);
  }

  private boolean hasPrivilegeNameForPrincipalScope() {
    return containsPrivilegeNameForPrincipalScope(getOperation().getPrivilegesOnTarget())
        || containsPrivilegeNameForPrincipalScope(getOperation().getPrivilegesOnSecondary());
  }

  private boolean hasPrivilegeNameForPrincipalRoleScope() {
    return containsPrivilegeNameForPrincipalRoleScope(getOperation().getPrivilegesOnTarget())
        || containsPrivilegeNameForPrincipalRoleScope(getOperation().getPrivilegesOnSecondary());
  }

  private boolean hasPrivilegeNameForCatalogRoleScope() {
    return containsPrivilegeNameForCatalogRoleScope(getOperation().getPrivilegesOnTarget())
        || containsPrivilegeNameForCatalogRoleScope(getOperation().getPrivilegesOnSecondary());
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
    return containsPrivilegeName(privileges, "PRINCIPAL_MANAGE_GRANTS")
        || containsPrivilegeNameForPrefix(privileges, "PRINCIPAL_");
  }

  private static boolean containsPrivilegeNameForPrincipalRoleScope(
      Set<PolarisPrivilege> privileges) {
    return containsPrivilegeName(privileges, "PRINCIPAL_ROLE_MANAGE_GRANTS")
        || containsPrivilegeNameForPrefix(privileges, "PRINCIPAL_ROLE_");
  }

  private static boolean containsPrivilegeNameForCatalogRoleScope(
      Set<PolarisPrivilege> privileges) {
    return containsPrivilegeName(privileges, "CATALOG_ROLE_MANAGE_GRANTS")
        || containsPrivilegeNameForPrefix(privileges, "CATALOG_ROLE_");
  }

  private static boolean containsPrivilegeNameForPrefix(
      Set<PolarisPrivilege> privileges, String prefix) {
    if (privileges == null || privileges.isEmpty()) {
      return false;
    }
    for (PolarisPrivilege privilege : privileges) {
      if (privilege.name().startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsPrivilegeName(Set<PolarisPrivilege> privileges, String name) {
    if (privileges == null || privileges.isEmpty()) {
      return false;
    }
    for (PolarisPrivilege privilege : privileges) {
      if (privilege.name().equals(name)) {
        return true;
      }
    }
    return false;
  }
}
