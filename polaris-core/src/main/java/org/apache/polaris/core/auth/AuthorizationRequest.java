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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Authorization request inputs for pre-authorization and core authorization.
 *
 * <p>This request contains the principal, operation, and two lists of resources:
 * <ul>
 *   <li><b>Targets:</b> Resources where target privileges will be checked
 *       (as defined by {@code RbacOperationSemantics.targetPrivileges()})
 *   <li><b>Secondaries:</b> Resources where secondary privileges will be checked
 *       (as defined by {@code RbacOperationSemantics.secondaryPrivileges()})
 * </ul>
 *
 * <p>The operation type determines which privileges are checked on each list.
 *
 * <p><b>Examples:</b>
 * <ul>
 *   <li>{@code ATTACH_POLICY_TO_TABLE}: targets=[policy], secondaries=[table1, table2, ...]
 *       <br>Checks POLICY_ATTACH on policy, TABLE_ATTACH_POLICY on each table
 *   <li>{@code RENAME_TABLE}: targets=[sourceTable], secondaries=[destNamespace]
 *       <br>Checks TABLE_DROP on source, TABLE_LIST+TABLE_CREATE on destination
 *   <li>{@code DROP_TABLE}: targets=[table1, table2, ...], secondaries=[]
 *       <br>Checks TABLE_DROP on each table
 * </ul>
 */
@PolarisImmutable
public interface AuthorizationRequest {
  /**
   * Creates an authorization request with target and secondary resource lists.
   *
   * <p>This is the preferred factory method. Resource lists are interpreted based on the
   * operation's RBAC semantics (see {@code RbacOperationSemantics}).
   *
   * @param principal the principal requesting authorization
   * @param operation the operation to authorize
   * @param targets resources where target privileges will be checked (may be empty)
   * @param secondaries resources where secondary privileges will be checked (may be empty)
   */
  static AuthorizationRequest of(
      @Nonnull PolarisPrincipal principal,
      @Nonnull PolarisAuthorizableOperation operation,
      @Nonnull List<PolarisSecurable> targets,
      @Nonnull List<PolarisSecurable> secondaries) {
    return ImmutableAuthorizationRequest.builder()
        .principal(principal)
        .operation(operation)
        .targets(targets)
        .secondaries(secondaries)
        .build();
  }

  /** Returns the principal requesting authorization. */
  @Nonnull
  PolarisPrincipal getPrincipal();

  /** Returns the operation being authorized. */
  @Nonnull
  PolarisAuthorizableOperation getOperation();

  /**
   * Returns resources where target privileges will be checked.
   *
   * <p>The specific privileges checked are defined by
   * {@code RbacOperationSemantics.forOperation(getOperation()).targetPrivileges()}.
   */
  @Nonnull
  List<PolarisSecurable> getTargets();

  /**
   * Returns resources where secondary privileges will be checked.
   *
   * <p>The specific privileges checked are defined by
   * {@code RbacOperationSemantics.forOperation(getOperation()).secondaryPrivileges()}.
   */
  @Nonnull
  List<PolarisSecurable> getSecondaries();

  /**
   * Returns a stable debug string for authorization messages.
   *
   * <p>Includes the operation, principal name, formatted targets, and formatted secondaries.
   */
  @Nonnull
  default String formatForAuthorizationMessage() {
    return String.format(
        "operation=%s principal=%s targets=%s secondaries=%s",
        getOperation(),
        getPrincipal().getName(),
        formatSecurables(getTargets()),
        formatSecurables(getSecondaries()));
  }

  private static String formatSecurables(List<PolarisSecurable> securables) {
    return securables.stream()
        .map(PolarisSecurable::formatForAuthorizationMessage)
        .collect(Collectors.joining(", ", "[", "]"));
  }

  default boolean hasSecurableType(PolarisEntityType... types) {
    for (PolarisSecurable target : getTargets()) {
      if (containsType(target, types)) {
        return true;
      }
    }
    for (PolarisSecurable secondary : getSecondaries()) {
      if (containsType(secondary, types)) {
        return true;
      }
    }
    return false;
  }

  static boolean containsType(PolarisSecurable securable, PolarisEntityType... types) {
    PolarisEntityType entityType = securable.getLeaf().entityType();
    for (PolarisEntityType type : types) {
      if (entityType == type) {
        return true;
      }
    }
    return false;
  }
}
