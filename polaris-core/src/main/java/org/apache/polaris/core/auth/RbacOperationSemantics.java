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
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_LIST_GRANTS;
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
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_LIST_GRANTS;
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
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE;
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
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_LIST_GRANTS;
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
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_LIST_GRANTS;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_WRITE_PROPERTIES;

import com.google.common.base.Preconditions;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import org.apache.polaris.core.entity.PolarisPrivilege;

/** Polaris RBAC-specific interpretation of an authorizable operation. */
record RbacOperationSemantics(
    EnumSet<PolarisPrivilege> targetPrivileges,
    EnumSet<PolarisPrivilege> secondaryPrivileges,
    ResolvedPathRooting rooting) {

  private RbacOperationSemantics(
      List<PolarisPrivilege> targetPrivileges,
      List<PolarisPrivilege> secondaryPrivileges,
      ResolvedPathRooting rooting) {
    this(toEnumSet(targetPrivileges), toEnumSet(secondaryPrivileges), rooting);
  }

  public RbacOperationSemantics {
    Preconditions.checkNotNull(targetPrivileges, "targetPrivileges must be non-null");
    Preconditions.checkNotNull(rooting, "rooting must be non-null");
    secondaryPrivileges =
        secondaryPrivileges == null ? EnumSet.noneOf(PolarisPrivilege.class) : secondaryPrivileges;
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

  static {
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_NAMESPACES,
        new RbacOperationSemantics(List.of(NAMESPACE_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_NAMESPACE,
        new RbacOperationSemantics(List.of(NAMESPACE_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LOAD_NAMESPACE_METADATA,
        new RbacOperationSemantics(
            List.of(NAMESPACE_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.NAMESPACE_EXISTS,
        new RbacOperationSemantics(List.of(NAMESPACE_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DROP_NAMESPACE,
        new RbacOperationSemantics(List.of(NAMESPACE_DROP), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPDATE_NAMESPACE_PROPERTIES,
        new RbacOperationSemantics(
            List.of(NAMESPACE_WRITE_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_TABLES,
        new RbacOperationSemantics(List.of(TABLE_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_TABLE_DIRECT,
        new RbacOperationSemantics(List.of(TABLE_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION,
        new RbacOperationSemantics(
            List.of(TABLE_CREATE, TABLE_WRITE_DATA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_TABLE_STAGED,
        new RbacOperationSemantics(List.of(TABLE_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION,
        new RbacOperationSemantics(
            List.of(TABLE_CREATE, TABLE_WRITE_DATA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REGISTER_TABLE,
        new RbacOperationSemantics(List.of(TABLE_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LOAD_TABLE,
        new RbacOperationSemantics(List.of(TABLE_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LOAD_TABLE_WITH_READ_DELEGATION,
        new RbacOperationSemantics(List.of(TABLE_READ_DATA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LOAD_TABLE_WITH_WRITE_DELEGATION,
        new RbacOperationSemantics(List.of(TABLE_WRITE_DATA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPDATE_TABLE,
        new RbacOperationSemantics(
            List.of(TABLE_WRITE_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPDATE_TABLE_FOR_STAGED_CREATE,
        new RbacOperationSemantics(List.of(TABLE_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DROP_TABLE_WITHOUT_PURGE,
        new RbacOperationSemantics(List.of(TABLE_DROP), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DROP_TABLE_WITH_PURGE,
        new RbacOperationSemantics(
            List.of(TABLE_DROP, TABLE_WRITE_DATA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.TABLE_EXISTS,
        new RbacOperationSemantics(List.of(TABLE_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.RENAME_TABLE,
        new RbacOperationSemantics(
            List.of(TABLE_DROP), List.of(TABLE_LIST, TABLE_CREATE), ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.COMMIT_TRANSACTION,
        new RbacOperationSemantics(
            List.of(TABLE_WRITE_PROPERTIES, TABLE_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_VIEWS,
        new RbacOperationSemantics(List.of(VIEW_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_VIEW,
        new RbacOperationSemantics(List.of(VIEW_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LOAD_VIEW,
        new RbacOperationSemantics(List.of(VIEW_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REPLACE_VIEW,
        new RbacOperationSemantics(List.of(VIEW_WRITE_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DROP_VIEW,
        new RbacOperationSemantics(List.of(VIEW_DROP), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.VIEW_EXISTS,
        new RbacOperationSemantics(List.of(VIEW_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.RENAME_VIEW,
        new RbacOperationSemantics(
            List.of(VIEW_DROP), List.of(VIEW_LIST, VIEW_CREATE), ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REPORT_READ_METRICS,
        new RbacOperationSemantics(List.of(TABLE_READ_DATA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REPORT_WRITE_METRICS,
        new RbacOperationSemantics(List.of(TABLE_WRITE_DATA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.SEND_NOTIFICATIONS,
        new RbacOperationSemantics(
            List.of(
                TABLE_CREATE, TABLE_WRITE_PROPERTIES, TABLE_DROP, NAMESPACE_CREATE, NAMESPACE_DROP),
            null,
            ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_CATALOGS,
        new RbacOperationSemantics(List.of(CATALOG_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_CATALOG,
        new RbacOperationSemantics(List.of(CATALOG_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.GET_CATALOG,
        new RbacOperationSemantics(
            List.of(CATALOG_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPDATE_CATALOG,
        new RbacOperationSemantics(
            List.of(CATALOG_WRITE_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DELETE_CATALOG,
        new RbacOperationSemantics(List.of(CATALOG_DROP), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_PRINCIPALS,
        new RbacOperationSemantics(List.of(PRINCIPAL_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_PRINCIPAL,
        new RbacOperationSemantics(List.of(PRINCIPAL_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.GET_PRINCIPAL,
        new RbacOperationSemantics(
            List.of(PRINCIPAL_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPDATE_PRINCIPAL,
        new RbacOperationSemantics(
            List.of(PRINCIPAL_WRITE_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DELETE_PRINCIPAL,
        new RbacOperationSemantics(List.of(PRINCIPAL_DROP), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ROTATE_CREDENTIALS,
        new RbacOperationSemantics(
            List.of(PRINCIPAL_ROTATE_CREDENTIALS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.RESET_CREDENTIALS,
        new RbacOperationSemantics(
            List.of(PRINCIPAL_RESET_CREDENTIALS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_PRINCIPAL_ROLES_ASSIGNED,
        new RbacOperationSemantics(List.of(PRINCIPAL_LIST_GRANTS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ASSIGN_PRINCIPAL_ROLE,
        new RbacOperationSemantics(
            List.of(PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REVOKE_PRINCIPAL_ROLE,
        new RbacOperationSemantics(
            List.of(PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE),
            List.of(PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE),
            ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_PRINCIPAL_ROLES,
        new RbacOperationSemantics(List.of(PRINCIPAL_ROLE_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_PRINCIPAL_ROLE,
        new RbacOperationSemantics(List.of(PRINCIPAL_ROLE_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.GET_PRINCIPAL_ROLE,
        new RbacOperationSemantics(
            List.of(PRINCIPAL_ROLE_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPDATE_PRINCIPAL_ROLE,
        new RbacOperationSemantics(
            List.of(PRINCIPAL_ROLE_WRITE_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DELETE_PRINCIPAL_ROLE,
        new RbacOperationSemantics(List.of(PRINCIPAL_ROLE_DROP), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_ASSIGNEE_PRINCIPALS_FOR_PRINCIPAL_ROLE,
        new RbacOperationSemantics(
            List.of(PRINCIPAL_ROLE_LIST_GRANTS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_CATALOG_ROLES_FOR_PRINCIPAL_ROLE,
        new RbacOperationSemantics(
            List.of(PRINCIPAL_ROLE_LIST_GRANTS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE,
        new RbacOperationSemantics(
            List.of(CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REVOKE_CATALOG_ROLE_FROM_PRINCIPAL_ROLE,
        new RbacOperationSemantics(
            List.of(CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_CATALOG_ROLES,
        new RbacOperationSemantics(List.of(CATALOG_ROLE_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_CATALOG_ROLE,
        new RbacOperationSemantics(List.of(CATALOG_ROLE_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.GET_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(CATALOG_ROLE_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPDATE_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(CATALOG_ROLE_WRITE_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DELETE_CATALOG_ROLE,
        new RbacOperationSemantics(List.of(CATALOG_ROLE_DROP), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_ASSIGNEE_PRINCIPAL_ROLES_FOR_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(CATALOG_ROLE_LIST_GRANTS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_GRANTS_FOR_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(CATALOG_ROLE_LIST_GRANTS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE,
        new RbacOperationSemantics(List.of(SERVICE_MANAGE_ACCESS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REVOKE_ROOT_GRANT_FROM_PRINCIPAL_ROLE,
        new RbacOperationSemantics(
            List.of(SERVICE_MANAGE_ACCESS),
            List.of(PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
            ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_GRANTS_ON_ROOT,
        new RbacOperationSemantics(List.of(SERVICE_MANAGE_ACCESS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_PRINCIPAL_GRANT_TO_PRINCIPAL_ROLE,
        new RbacOperationSemantics(
            List.of(PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REVOKE_PRINCIPAL_GRANT_FROM_PRINCIPAL_ROLE,
        new RbacOperationSemantics(
            List.of(PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE),
            List.of(PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
            ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_GRANTS_ON_PRINCIPAL,
        new RbacOperationSemantics(List.of(PRINCIPAL_LIST_GRANTS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_PRINCIPAL_ROLE_GRANT_TO_PRINCIPAL_ROLE,
        new RbacOperationSemantics(
            List.of(PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REVOKE_PRINCIPAL_ROLE_GRANT_FROM_PRINCIPAL_ROLE,
        new RbacOperationSemantics(
            List.of(PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE),
            List.of(PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
            ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_GRANTS_ON_PRINCIPAL_ROLE,
        new RbacOperationSemantics(
            List.of(PRINCIPAL_ROLE_LIST_GRANTS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_CATALOG_ROLE_GRANT_TO_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REVOKE_CATALOG_ROLE_GRANT_FROM_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE),
            List.of(CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
            ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_GRANTS_ON_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(CATALOG_ROLE_LIST_GRANTS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_CATALOG_GRANT_TO_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(CATALOG_MANAGE_GRANTS_ON_SECURABLE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REVOKE_CATALOG_GRANT_FROM_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(CATALOG_MANAGE_GRANTS_ON_SECURABLE),
            List.of(CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
            ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_GRANTS_ON_CATALOG,
        new RbacOperationSemantics(List.of(CATALOG_LIST_GRANTS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_NAMESPACE_GRANT_TO_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(NAMESPACE_MANAGE_GRANTS_ON_SECURABLE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REVOKE_NAMESPACE_GRANT_FROM_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(NAMESPACE_MANAGE_GRANTS_ON_SECURABLE),
            List.of(CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
            ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_GRANTS_ON_NAMESPACE,
        new RbacOperationSemantics(List.of(NAMESPACE_LIST_GRANTS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_TABLE_GRANT_TO_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(TABLE_MANAGE_GRANTS_ON_SECURABLE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REVOKE_TABLE_GRANT_FROM_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(TABLE_MANAGE_GRANTS_ON_SECURABLE),
            List.of(CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
            ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_GRANTS_ON_TABLE,
        new RbacOperationSemantics(List.of(TABLE_LIST_GRANTS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_VIEW_GRANT_TO_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(VIEW_MANAGE_GRANTS_ON_SECURABLE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REVOKE_VIEW_GRANT_FROM_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(VIEW_MANAGE_GRANTS_ON_SECURABLE),
            List.of(CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
            ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_GRANTS_ON_VIEW,
        new RbacOperationSemantics(List.of(VIEW_LIST_GRANTS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_POLICY,
        new RbacOperationSemantics(List.of(POLICY_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LOAD_POLICY,
        new RbacOperationSemantics(List.of(POLICY_READ), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DROP_POLICY,
        new RbacOperationSemantics(List.of(POLICY_DROP), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPDATE_POLICY,
        new RbacOperationSemantics(List.of(POLICY_WRITE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_POLICY,
        new RbacOperationSemantics(List.of(POLICY_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ATTACH_POLICY_TO_CATALOG,
        new RbacOperationSemantics(
            List.of(POLICY_ATTACH), List.of(CATALOG_ATTACH_POLICY), ResolvedPathRooting.CATALOG));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ATTACH_POLICY_TO_NAMESPACE,
        new RbacOperationSemantics(
            List.of(POLICY_ATTACH), List.of(NAMESPACE_ATTACH_POLICY), ResolvedPathRooting.CATALOG));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ATTACH_POLICY_TO_TABLE,
        new RbacOperationSemantics(
            List.of(POLICY_ATTACH), List.of(TABLE_ATTACH_POLICY), ResolvedPathRooting.CATALOG));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DETACH_POLICY_FROM_CATALOG,
        new RbacOperationSemantics(
            List.of(POLICY_DETACH), List.of(CATALOG_DETACH_POLICY), ResolvedPathRooting.CATALOG));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DETACH_POLICY_FROM_NAMESPACE,
        new RbacOperationSemantics(
            List.of(POLICY_DETACH), List.of(NAMESPACE_DETACH_POLICY), ResolvedPathRooting.CATALOG));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DETACH_POLICY_FROM_TABLE,
        new RbacOperationSemantics(
            List.of(POLICY_DETACH), List.of(TABLE_DETACH_POLICY), ResolvedPathRooting.CATALOG));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_CATALOG,
        new RbacOperationSemantics(
            List.of(CATALOG_READ_PROPERTIES), null, ResolvedPathRooting.CATALOG));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_NAMESPACE,
        new RbacOperationSemantics(
            List.of(NAMESPACE_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_TABLE,
        new RbacOperationSemantics(List.of(TABLE_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_POLICY_GRANT_TO_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(POLICY_MANAGE_GRANTS_ON_SECURABLE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REVOKE_POLICY_GRANT_FROM_CATALOG_ROLE,
        new RbacOperationSemantics(
            List.of(POLICY_MANAGE_GRANTS_ON_SECURABLE),
            List.of(CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
            ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ASSIGN_TABLE_UUID,
        new RbacOperationSemantics(List.of(TABLE_ASSIGN_UUID), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPGRADE_TABLE_FORMAT_VERSION,
        new RbacOperationSemantics(
            List.of(TABLE_UPGRADE_FORMAT_VERSION), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_TABLE_SCHEMA,
        new RbacOperationSemantics(List.of(TABLE_ADD_SCHEMA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.SET_TABLE_CURRENT_SCHEMA,
        new RbacOperationSemantics(
            List.of(TABLE_SET_CURRENT_SCHEMA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_TABLE_PARTITION_SPEC,
        new RbacOperationSemantics(
            List.of(TABLE_ADD_PARTITION_SPEC), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_TABLE_SORT_ORDER,
        new RbacOperationSemantics(List.of(TABLE_ADD_SORT_ORDER), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.SET_TABLE_DEFAULT_SORT_ORDER,
        new RbacOperationSemantics(
            List.of(TABLE_SET_DEFAULT_SORT_ORDER), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_TABLE_SNAPSHOT,
        new RbacOperationSemantics(List.of(TABLE_ADD_SNAPSHOT), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.SET_TABLE_SNAPSHOT_REF,
        new RbacOperationSemantics(
            List.of(TABLE_SET_SNAPSHOT_REF), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REMOVE_TABLE_SNAPSHOTS,
        new RbacOperationSemantics(
            List.of(TABLE_REMOVE_SNAPSHOTS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REMOVE_TABLE_SNAPSHOT_REF,
        new RbacOperationSemantics(
            List.of(TABLE_REMOVE_SNAPSHOT_REF), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.SET_TABLE_LOCATION,
        new RbacOperationSemantics(List.of(TABLE_SET_LOCATION), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.SET_TABLE_PROPERTIES,
        new RbacOperationSemantics(List.of(TABLE_SET_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REMOVE_TABLE_PROPERTIES,
        new RbacOperationSemantics(
            List.of(TABLE_REMOVE_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.SET_TABLE_STATISTICS,
        new RbacOperationSemantics(List.of(TABLE_SET_STATISTICS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REMOVE_TABLE_STATISTICS,
        new RbacOperationSemantics(
            List.of(TABLE_REMOVE_STATISTICS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REMOVE_TABLE_PARTITION_SPECS,
        new RbacOperationSemantics(
            List.of(TABLE_REMOVE_PARTITION_SPECS), null, ResolvedPathRooting.ROOT));

    EnumSet<PolarisAuthorizableOperation> missing =
        EnumSet.allOf(PolarisAuthorizableOperation.class);
    missing.removeAll(RBAC_SEMANTICS_BY_OPERATION.keySet());
    if (!missing.isEmpty()) {
      throw new IllegalStateException("Missing RBAC semantics for operations: " + missing);
    }
  }

  private static EnumSet<PolarisPrivilege> toEnumSet(List<PolarisPrivilege> privileges) {
    if (privileges == null || privileges.isEmpty()) {
      return EnumSet.noneOf(PolarisPrivilege.class);
    }
    return EnumSet.copyOf(privileges);
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
