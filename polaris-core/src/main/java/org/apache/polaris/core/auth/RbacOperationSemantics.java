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

import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ATTACH_POLICY;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_DETACH_POLICY;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_WRITE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_ATTACH_POLICY;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_DETACH_POLICY;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.POLICY_ATTACH;
import static org.apache.polaris.core.entity.PolarisPrivilege.POLICY_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.POLICY_DETACH;
import static org.apache.polaris.core.entity.PolarisPrivilege.POLICY_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.POLICY_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.POLICY_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.POLICY_READ;
import static org.apache.polaris.core.entity.PolarisPrivilege.POLICY_WRITE;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_LIST_GRANTS;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_RESET_CREDENTIALS;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_WRITE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROTATE_CREDENTIALS;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_WRITE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.SERVICE_MANAGE_ACCESS;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_ADD_PARTITION_SPEC;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_ADD_SCHEMA;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_ADD_SNAPSHOT;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_ADD_SORT_ORDER;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_ASSIGN_UUID;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_ATTACH_POLICY;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_DETACH_POLICY;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_READ_DATA;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_REMOVE_PARTITION_SPECS;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_REMOVE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_REMOVE_SNAPSHOTS;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_REMOVE_SNAPSHOT_REF;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_REMOVE_STATISTICS;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_SET_CURRENT_SCHEMA;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_SET_DEFAULT_SORT_ORDER;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_SET_LOCATION;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_SET_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_SET_SNAPSHOT_REF;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_SET_STATISTICS;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_UPGRADE_FORMAT_VERSION;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_WRITE_DATA;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_WRITE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_WRITE_PROPERTIES;

import com.google.common.base.Preconditions;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisPrivilege;

/** Polaris RBAC-specific interpretation of an authorizable operation. */
record RbacOperationSemantics(
    Set<PolarisPrivilege> targetPrivileges,
    Set<PolarisPrivilege> secondaryPrivileges,
    ResolvedPathRooting rooting) {

  public RbacOperationSemantics {
    Preconditions.checkNotNull(targetPrivileges, "targetPrivileges must be non-null");
    Preconditions.checkNotNull(rooting, "rooting must be non-null");
    targetPrivileges = Set.copyOf(targetPrivileges);
    secondaryPrivileges = secondaryPrivileges == null ? Set.of() : Set.copyOf(secondaryPrivileges);
    Preconditions.checkArgument(!targetPrivileges.isEmpty(), "targetPrivileges must be non-empty");
  }

  /**
   * Determines whether RBAC should prepend the root container to the resolved path before
   * evaluating privileges.
   *
   * <p>{@code ROOT} means include the root container in the resolved path. {@code CATALOG} means do
   * not prepend the root container.
   */
  enum ResolvedPathRooting {
    ROOT,
    CATALOG
  }

  private static final EnumMap<PolarisAuthorizableOperation, RbacOperationSemantics>
      RBAC_SEMANTICS_BY_OPERATION = new EnumMap<>(PolarisAuthorizableOperation.class);

  /** Helper method to register an operation with a single privilege using ROOT rooting. */
  private static void register(PolarisAuthorizableOperation operation, PolarisPrivilege privilege) {
    register(operation, EnumSet.of(privilege), null, ResolvedPathRooting.ROOT);
  }

  /** Helper method to register an operation with multiple privileges using ROOT rooting. */
  private static void register(
      PolarisAuthorizableOperation operation,
      PolarisPrivilege privilege1,
      PolarisPrivilege privilege2) {
    register(operation, EnumSet.of(privilege1, privilege2), null, ResolvedPathRooting.ROOT);
  }

  /** Helper method to register an operation with multiple privileges using ROOT rooting. */
  private static void register(
      PolarisAuthorizableOperation operation,
      PolarisPrivilege privilege1,
      PolarisPrivilege privilege2,
      PolarisPrivilege privilege3,
      PolarisPrivilege privilege4,
      PolarisPrivilege privilege5) {
    register(
        operation,
        EnumSet.of(privilege1, privilege2, privilege3, privilege4, privilege5),
        null,
        ResolvedPathRooting.ROOT);
  }

  /** Helper method to register an operation with target and secondary privileges. */
  private static void register(
      PolarisAuthorizableOperation operation,
      Set<PolarisPrivilege> targetPrivileges,
      Set<PolarisPrivilege> secondaryPrivileges,
      ResolvedPathRooting rooting) {
    RBAC_SEMANTICS_BY_OPERATION.put(
        operation, new RbacOperationSemantics(targetPrivileges, secondaryPrivileges, rooting));
  }

  static {
    // Namespace operations
    register(PolarisAuthorizableOperation.LIST_NAMESPACES, NAMESPACE_LIST);
    register(PolarisAuthorizableOperation.CREATE_NAMESPACE, NAMESPACE_CREATE);
    register(PolarisAuthorizableOperation.LOAD_NAMESPACE_METADATA, NAMESPACE_READ_PROPERTIES);
    register(PolarisAuthorizableOperation.NAMESPACE_EXISTS, NAMESPACE_LIST);
    register(PolarisAuthorizableOperation.DROP_NAMESPACE, NAMESPACE_DROP);
    register(PolarisAuthorizableOperation.UPDATE_NAMESPACE_PROPERTIES, NAMESPACE_WRITE_PROPERTIES);

    // Table operations
    register(PolarisAuthorizableOperation.LIST_TABLES, TABLE_LIST);
    register(PolarisAuthorizableOperation.CREATE_TABLE_DIRECT, TABLE_CREATE);
    register(
        PolarisAuthorizableOperation.CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION,
        TABLE_CREATE,
        TABLE_WRITE_DATA);
    register(PolarisAuthorizableOperation.CREATE_TABLE_STAGED, TABLE_CREATE);
    register(
        PolarisAuthorizableOperation.CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION,
        TABLE_CREATE,
        TABLE_WRITE_DATA);
    register(PolarisAuthorizableOperation.REGISTER_TABLE, TABLE_CREATE);
    register(PolarisAuthorizableOperation.LOAD_TABLE, TABLE_READ_PROPERTIES);
    register(PolarisAuthorizableOperation.LOAD_TABLE_WITH_READ_DELEGATION, TABLE_READ_DATA);
    register(PolarisAuthorizableOperation.LOAD_TABLE_WITH_WRITE_DELEGATION, TABLE_WRITE_DATA);
    register(PolarisAuthorizableOperation.UPDATE_TABLE, TABLE_WRITE_PROPERTIES);
    register(PolarisAuthorizableOperation.UPDATE_TABLE_FOR_STAGED_CREATE, TABLE_CREATE);
    register(PolarisAuthorizableOperation.DROP_TABLE_WITHOUT_PURGE, TABLE_DROP);
    register(PolarisAuthorizableOperation.DROP_TABLE_WITH_PURGE, TABLE_DROP, TABLE_WRITE_DATA);
    register(PolarisAuthorizableOperation.TABLE_EXISTS, TABLE_LIST);
    register(
        PolarisAuthorizableOperation.RENAME_TABLE,
        EnumSet.of(TABLE_DROP),
        EnumSet.of(TABLE_LIST, TABLE_CREATE),
        ResolvedPathRooting.ROOT);
    register(
        PolarisAuthorizableOperation.COMMIT_TRANSACTION,
        EnumSet.of(TABLE_WRITE_PROPERTIES, TABLE_CREATE),
        null,
        ResolvedPathRooting.ROOT);

    // View operations
    register(PolarisAuthorizableOperation.LIST_VIEWS, VIEW_LIST);
    register(PolarisAuthorizableOperation.CREATE_VIEW, VIEW_CREATE);
    register(PolarisAuthorizableOperation.LOAD_VIEW, VIEW_READ_PROPERTIES);
    register(PolarisAuthorizableOperation.REPLACE_VIEW, VIEW_WRITE_PROPERTIES);
    register(PolarisAuthorizableOperation.DROP_VIEW, VIEW_DROP);
    register(PolarisAuthorizableOperation.VIEW_EXISTS, VIEW_LIST);
    register(
        PolarisAuthorizableOperation.RENAME_VIEW,
        EnumSet.of(VIEW_DROP),
        EnumSet.of(VIEW_LIST, VIEW_CREATE),
        ResolvedPathRooting.ROOT);

    // Metrics and notifications
    register(PolarisAuthorizableOperation.REPORT_READ_METRICS, TABLE_READ_DATA);
    register(PolarisAuthorizableOperation.REPORT_WRITE_METRICS, TABLE_WRITE_DATA);
    register(
        PolarisAuthorizableOperation.SEND_NOTIFICATIONS,
        TABLE_CREATE,
        TABLE_WRITE_PROPERTIES,
        TABLE_DROP,
        NAMESPACE_CREATE,
        NAMESPACE_DROP);

    // Catalog operations
    register(PolarisAuthorizableOperation.LIST_CATALOGS, CATALOG_LIST);
    register(PolarisAuthorizableOperation.CREATE_CATALOG, CATALOG_CREATE);
    register(PolarisAuthorizableOperation.GET_CATALOG, CATALOG_READ_PROPERTIES);
    register(PolarisAuthorizableOperation.UPDATE_CATALOG, CATALOG_WRITE_PROPERTIES);
    register(PolarisAuthorizableOperation.DELETE_CATALOG, CATALOG_DROP);

    // Principal operations
    register(PolarisAuthorizableOperation.LIST_PRINCIPALS, PRINCIPAL_LIST);
    register(PolarisAuthorizableOperation.CREATE_PRINCIPAL, PRINCIPAL_CREATE);
    register(PolarisAuthorizableOperation.GET_PRINCIPAL, PRINCIPAL_READ_PROPERTIES);
    register(PolarisAuthorizableOperation.UPDATE_PRINCIPAL, PRINCIPAL_WRITE_PROPERTIES);
    register(PolarisAuthorizableOperation.DELETE_PRINCIPAL, PRINCIPAL_DROP);
    register(PolarisAuthorizableOperation.ROTATE_CREDENTIALS, PRINCIPAL_ROTATE_CREDENTIALS);
    register(PolarisAuthorizableOperation.RESET_CREDENTIALS, PRINCIPAL_RESET_CREDENTIALS);
    register(PolarisAuthorizableOperation.LIST_PRINCIPAL_ROLES_ASSIGNED, PRINCIPAL_LIST_GRANTS);
    register(
        PolarisAuthorizableOperation.ASSIGN_PRINCIPAL_ROLE,
        PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE);
    register(
        PolarisAuthorizableOperation.REVOKE_PRINCIPAL_ROLE,
        EnumSet.of(PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE),
        EnumSet.of(PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE),
        ResolvedPathRooting.ROOT);

    // Principal role operations
    register(PolarisAuthorizableOperation.LIST_PRINCIPAL_ROLES, PRINCIPAL_ROLE_LIST);
    register(PolarisAuthorizableOperation.CREATE_PRINCIPAL_ROLE, PRINCIPAL_ROLE_CREATE);
    register(PolarisAuthorizableOperation.GET_PRINCIPAL_ROLE, PRINCIPAL_ROLE_READ_PROPERTIES);
    register(PolarisAuthorizableOperation.UPDATE_PRINCIPAL_ROLE, PRINCIPAL_ROLE_WRITE_PROPERTIES);
    register(PolarisAuthorizableOperation.DELETE_PRINCIPAL_ROLE, PRINCIPAL_ROLE_DROP);
    register(
        PolarisAuthorizableOperation.LIST_ASSIGNEE_PRINCIPALS_FOR_PRINCIPAL_ROLE,
        PRINCIPAL_ROLE_LIST_GRANTS);
    register(
        PolarisAuthorizableOperation.LIST_CATALOG_ROLES_FOR_PRINCIPAL_ROLE,
        PRINCIPAL_ROLE_LIST_GRANTS);
    register(
        PolarisAuthorizableOperation.ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE,
        CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE);
    register(
        PolarisAuthorizableOperation.REVOKE_CATALOG_ROLE_FROM_PRINCIPAL_ROLE,
        CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE);

    // Catalog role operations
    register(PolarisAuthorizableOperation.LIST_CATALOG_ROLES, CATALOG_ROLE_LIST);
    register(PolarisAuthorizableOperation.CREATE_CATALOG_ROLE, CATALOG_ROLE_CREATE);
    register(PolarisAuthorizableOperation.GET_CATALOG_ROLE, CATALOG_ROLE_READ_PROPERTIES);
    register(PolarisAuthorizableOperation.UPDATE_CATALOG_ROLE, CATALOG_ROLE_WRITE_PROPERTIES);
    register(PolarisAuthorizableOperation.DELETE_CATALOG_ROLE, CATALOG_ROLE_DROP);
    register(
        PolarisAuthorizableOperation.LIST_ASSIGNEE_PRINCIPAL_ROLES_FOR_CATALOG_ROLE,
        CATALOG_ROLE_LIST_GRANTS);
    register(PolarisAuthorizableOperation.LIST_GRANTS_FOR_CATALOG_ROLE, CATALOG_ROLE_LIST_GRANTS);

    // Grant operations
    register(PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE, SERVICE_MANAGE_ACCESS);
    register(
        PolarisAuthorizableOperation.REVOKE_ROOT_GRANT_FROM_PRINCIPAL_ROLE,
        EnumSet.of(SERVICE_MANAGE_ACCESS),
        EnumSet.of(PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
        ResolvedPathRooting.ROOT);
    register(
        PolarisAuthorizableOperation.ADD_CATALOG_GRANT_TO_CATALOG_ROLE,
        CATALOG_MANAGE_GRANTS_ON_SECURABLE);
    register(
        PolarisAuthorizableOperation.REVOKE_CATALOG_GRANT_FROM_CATALOG_ROLE,
        EnumSet.of(CATALOG_MANAGE_GRANTS_ON_SECURABLE),
        EnumSet.of(CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
        ResolvedPathRooting.ROOT);
    register(
        PolarisAuthorizableOperation.ADD_NAMESPACE_GRANT_TO_CATALOG_ROLE,
        NAMESPACE_MANAGE_GRANTS_ON_SECURABLE);
    register(
        PolarisAuthorizableOperation.REVOKE_NAMESPACE_GRANT_FROM_CATALOG_ROLE,
        EnumSet.of(NAMESPACE_MANAGE_GRANTS_ON_SECURABLE),
        EnumSet.of(CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
        ResolvedPathRooting.ROOT);
    register(
        PolarisAuthorizableOperation.ADD_TABLE_GRANT_TO_CATALOG_ROLE,
        TABLE_MANAGE_GRANTS_ON_SECURABLE);
    register(
        PolarisAuthorizableOperation.REVOKE_TABLE_GRANT_FROM_CATALOG_ROLE,
        EnumSet.of(TABLE_MANAGE_GRANTS_ON_SECURABLE),
        EnumSet.of(CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
        ResolvedPathRooting.ROOT);
    register(
        PolarisAuthorizableOperation.ADD_VIEW_GRANT_TO_CATALOG_ROLE,
        VIEW_MANAGE_GRANTS_ON_SECURABLE);
    register(
        PolarisAuthorizableOperation.REVOKE_VIEW_GRANT_FROM_CATALOG_ROLE,
        EnumSet.of(VIEW_MANAGE_GRANTS_ON_SECURABLE),
        EnumSet.of(CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
        ResolvedPathRooting.ROOT);

    // Policy operations
    register(PolarisAuthorizableOperation.CREATE_POLICY, POLICY_CREATE);
    register(PolarisAuthorizableOperation.LOAD_POLICY, POLICY_READ);
    register(PolarisAuthorizableOperation.DROP_POLICY, POLICY_DROP);
    register(PolarisAuthorizableOperation.UPDATE_POLICY, POLICY_WRITE);
    register(PolarisAuthorizableOperation.LIST_POLICY, POLICY_LIST);

    // Policy attachment operations (use CATALOG rooting)
    register(
        PolarisAuthorizableOperation.ATTACH_POLICY_TO_CATALOG,
        EnumSet.of(POLICY_ATTACH),
        EnumSet.of(CATALOG_ATTACH_POLICY),
        ResolvedPathRooting.CATALOG);
    register(
        PolarisAuthorizableOperation.ATTACH_POLICY_TO_NAMESPACE,
        EnumSet.of(POLICY_ATTACH),
        EnumSet.of(NAMESPACE_ATTACH_POLICY),
        ResolvedPathRooting.CATALOG);
    register(
        PolarisAuthorizableOperation.ATTACH_POLICY_TO_TABLE,
        EnumSet.of(POLICY_ATTACH),
        EnumSet.of(TABLE_ATTACH_POLICY),
        ResolvedPathRooting.CATALOG);
    register(
        PolarisAuthorizableOperation.DETACH_POLICY_FROM_CATALOG,
        EnumSet.of(POLICY_DETACH),
        EnumSet.of(CATALOG_DETACH_POLICY),
        ResolvedPathRooting.CATALOG);
    register(
        PolarisAuthorizableOperation.DETACH_POLICY_FROM_NAMESPACE,
        EnumSet.of(POLICY_DETACH),
        EnumSet.of(NAMESPACE_DETACH_POLICY),
        ResolvedPathRooting.CATALOG);
    register(
        PolarisAuthorizableOperation.DETACH_POLICY_FROM_TABLE,
        EnumSet.of(POLICY_DETACH),
        EnumSet.of(TABLE_DETACH_POLICY),
        ResolvedPathRooting.CATALOG);

    // Get applicable policies operations
    register(
        PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_CATALOG,
        EnumSet.of(CATALOG_READ_PROPERTIES),
        null,
        ResolvedPathRooting.CATALOG);
    register(
        PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_NAMESPACE,
        NAMESPACE_READ_PROPERTIES);
    register(PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_TABLE, TABLE_READ_PROPERTIES);

    // Policy grant operations
    register(
        PolarisAuthorizableOperation.ADD_POLICY_GRANT_TO_CATALOG_ROLE,
        POLICY_MANAGE_GRANTS_ON_SECURABLE);
    register(
        PolarisAuthorizableOperation.REVOKE_POLICY_GRANT_FROM_CATALOG_ROLE,
        EnumSet.of(POLICY_MANAGE_GRANTS_ON_SECURABLE),
        EnumSet.of(CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
        ResolvedPathRooting.ROOT);

    // Table metadata operations
    register(PolarisAuthorizableOperation.ASSIGN_TABLE_UUID, TABLE_ASSIGN_UUID);
    register(
        PolarisAuthorizableOperation.UPGRADE_TABLE_FORMAT_VERSION, TABLE_UPGRADE_FORMAT_VERSION);
    register(PolarisAuthorizableOperation.ADD_TABLE_SCHEMA, TABLE_ADD_SCHEMA);
    register(PolarisAuthorizableOperation.SET_TABLE_CURRENT_SCHEMA, TABLE_SET_CURRENT_SCHEMA);
    register(PolarisAuthorizableOperation.ADD_TABLE_PARTITION_SPEC, TABLE_ADD_PARTITION_SPEC);
    register(PolarisAuthorizableOperation.ADD_TABLE_SORT_ORDER, TABLE_ADD_SORT_ORDER);
    register(
        PolarisAuthorizableOperation.SET_TABLE_DEFAULT_SORT_ORDER, TABLE_SET_DEFAULT_SORT_ORDER);
    register(PolarisAuthorizableOperation.ADD_TABLE_SNAPSHOT, TABLE_ADD_SNAPSHOT);
    register(PolarisAuthorizableOperation.SET_TABLE_SNAPSHOT_REF, TABLE_SET_SNAPSHOT_REF);
    register(PolarisAuthorizableOperation.REMOVE_TABLE_SNAPSHOTS, TABLE_REMOVE_SNAPSHOTS);
    register(PolarisAuthorizableOperation.REMOVE_TABLE_SNAPSHOT_REF, TABLE_REMOVE_SNAPSHOT_REF);
    register(PolarisAuthorizableOperation.SET_TABLE_LOCATION, TABLE_SET_LOCATION);
    register(PolarisAuthorizableOperation.SET_TABLE_PROPERTIES, TABLE_SET_PROPERTIES);
    register(PolarisAuthorizableOperation.REMOVE_TABLE_PROPERTIES, TABLE_REMOVE_PROPERTIES);
    register(PolarisAuthorizableOperation.SET_TABLE_STATISTICS, TABLE_SET_STATISTICS);
    register(PolarisAuthorizableOperation.REMOVE_TABLE_STATISTICS, TABLE_REMOVE_STATISTICS);
    register(
        PolarisAuthorizableOperation.REMOVE_TABLE_PARTITION_SPECS, TABLE_REMOVE_PARTITION_SPECS);

    EnumSet<PolarisAuthorizableOperation> missing =
        EnumSet.allOf(PolarisAuthorizableOperation.class);
    missing.removeAll(RBAC_SEMANTICS_BY_OPERATION.keySet());
    if (!missing.isEmpty()) {
      throw new IllegalStateException("Missing RBAC semantics for operations: " + missing);
    }
  }

  static RbacOperationSemantics forOperation(PolarisAuthorizableOperation operation) {
    RbacOperationSemantics semantics = RBAC_SEMANTICS_BY_OPERATION.get(operation);
    if (semantics == null) {
      throw new IllegalStateException("Missing RBAC semantics for operation: " + operation);
    }
    return semantics;
  }

  boolean hasSecondaryPrivileges() {
    return !secondaryPrivileges.isEmpty();
  }
}
