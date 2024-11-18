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

import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_CREATE;
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
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_WRITE_MAINTENANCE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_WRITE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_LIST_GRANTS;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES;
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
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_LIST_GRANTS;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_READ_DATA;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_WRITE_DATA;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_WRITE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_LIST_GRANTS;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_WRITE_PROPERTIES;

import java.util.EnumSet;
import org.apache.polaris.core.entity.PolarisPrivilege;

/**
 * Denotes the fine-grained expansion of all Polaris operations that are associated with some set of
 * authorization requirements to enact.
 */
public enum PolarisAuthorizableOperation {
  LIST_NAMESPACES(NAMESPACE_LIST),
  CREATE_NAMESPACE(NAMESPACE_CREATE),
  LOAD_NAMESPACE_METADATA(NAMESPACE_READ_PROPERTIES),
  NAMESPACE_EXISTS(NAMESPACE_LIST),
  DROP_NAMESPACE(NAMESPACE_DROP),
  UPDATE_NAMESPACE_PROPERTIES(NAMESPACE_WRITE_PROPERTIES),
  LIST_TABLES(TABLE_LIST),
  CREATE_TABLE_DIRECT(TABLE_CREATE),
  CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION(EnumSet.of(TABLE_CREATE, TABLE_WRITE_DATA)),
  CREATE_TABLE_STAGED(TABLE_CREATE),
  CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION(EnumSet.of(TABLE_CREATE, TABLE_WRITE_DATA)),
  REGISTER_TABLE(TABLE_CREATE),
  LOAD_TABLE(TABLE_READ_PROPERTIES),
  LOAD_TABLE_WITH_READ_DELEGATION(TABLE_READ_DATA),
  LOAD_TABLE_WITH_WRITE_DELEGATION(TABLE_WRITE_DATA),
  UPDATE_TABLE(TABLE_WRITE_PROPERTIES),
  UPDATE_TABLE_FOR_STAGED_CREATE(TABLE_CREATE),
  DROP_TABLE_WITHOUT_PURGE(TABLE_DROP),
  DROP_TABLE_WITH_PURGE(EnumSet.of(TABLE_DROP, TABLE_WRITE_DATA)),
  TABLE_EXISTS(TABLE_LIST),
  RENAME_TABLE(TABLE_DROP, EnumSet.of(TABLE_LIST, TABLE_CREATE)),
  COMMIT_TRANSACTION(EnumSet.of(TABLE_WRITE_PROPERTIES, TABLE_CREATE)),
  LIST_VIEWS(VIEW_LIST),
  CREATE_VIEW(VIEW_CREATE),
  LOAD_VIEW(VIEW_READ_PROPERTIES),
  REPLACE_VIEW(VIEW_WRITE_PROPERTIES),
  DROP_VIEW(VIEW_DROP),
  VIEW_EXISTS(VIEW_LIST),
  RENAME_VIEW(VIEW_DROP, EnumSet.of(VIEW_LIST, VIEW_CREATE)),
  REPORT_METRICS(EnumSet.noneOf(PolarisPrivilege.class)),
  SEND_NOTIFICATIONS(
      EnumSet.of(
          TABLE_CREATE, TABLE_WRITE_PROPERTIES, TABLE_DROP, NAMESPACE_CREATE, NAMESPACE_DROP)),
  LIST_CATALOGS(CATALOG_LIST),
  CREATE_CATALOG(CATALOG_CREATE),
  GET_CATALOG(CATALOG_READ_PROPERTIES),
  UPDATE_CATALOG(CATALOG_WRITE_PROPERTIES),
  UPDATE_CATALOG_MAINTENANCE_PROPERTIES(CATALOG_WRITE_MAINTENANCE_PROPERTIES),
  DELETE_CATALOG(CATALOG_DROP),
  LIST_PRINCIPALS(PRINCIPAL_LIST),
  CREATE_PRINCIPAL(PRINCIPAL_CREATE),
  GET_PRINCIPAL(PRINCIPAL_READ_PROPERTIES),
  UPDATE_PRINCIPAL(PRINCIPAL_WRITE_PROPERTIES),
  DELETE_PRINCIPAL(PRINCIPAL_DROP),
  ROTATE_CREDENTIALS(PRINCIPAL_ROTATE_CREDENTIALS),
  RESET_CREDENTIALS(PRINCIPAL_RESET_CREDENTIALS),
  LIST_PRINCIPAL_ROLES_ASSIGNED(PRINCIPAL_LIST_GRANTS),
  ASSIGN_PRINCIPAL_ROLE(PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE),
  REVOKE_PRINCIPAL_ROLE(
      PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE, PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE),
  LIST_PRINCIPAL_ROLES(PRINCIPAL_ROLE_LIST),
  CREATE_PRINCIPAL_ROLE(PRINCIPAL_ROLE_CREATE),
  GET_PRINCIPAL_ROLE(PRINCIPAL_ROLE_READ_PROPERTIES),
  UPDATE_PRINCIPAL_ROLE(PRINCIPAL_ROLE_WRITE_PROPERTIES),
  DELETE_PRINCIPAL_ROLE(PRINCIPAL_ROLE_DROP),
  LIST_ASSIGNEE_PRINCIPALS_FOR_PRINCIPAL_ROLE(PRINCIPAL_ROLE_LIST_GRANTS),
  LIST_CATALOG_ROLES_FOR_PRINCIPAL_ROLE(PRINCIPAL_ROLE_LIST_GRANTS),
  ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE(CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE),
  REVOKE_CATALOG_ROLE_FROM_PRINCIPAL_ROLE(
      CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE, PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
  LIST_CATALOG_ROLES(CATALOG_ROLE_LIST),
  CREATE_CATALOG_ROLE(CATALOG_ROLE_CREATE),
  GET_CATALOG_ROLE(CATALOG_ROLE_READ_PROPERTIES),
  UPDATE_CATALOG_ROLE(CATALOG_ROLE_WRITE_PROPERTIES),
  DELETE_CATALOG_ROLE(CATALOG_ROLE_DROP),
  LIST_ASSIGNEE_PRINCIPAL_ROLES_FOR_CATALOG_ROLE(CATALOG_ROLE_LIST_GRANTS),
  LIST_GRANTS_FOR_CATALOG_ROLE(CATALOG_ROLE_LIST_GRANTS),
  ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE(SERVICE_MANAGE_ACCESS),
  REVOKE_ROOT_GRANT_FROM_PRINCIPAL_ROLE(
      SERVICE_MANAGE_ACCESS, PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
  LIST_GRANTS_ON_ROOT(SERVICE_MANAGE_ACCESS),
  ADD_PRINCIPAL_GRANT_TO_PRINCIPAL_ROLE(PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE),
  REVOKE_PRINCIPAL_GRANT_FROM_PRINCIPAL_ROLE(
      PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE, PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
  LIST_GRANTS_ON_PRINCIPAL(PRINCIPAL_LIST_GRANTS),
  ADD_PRINCIPAL_ROLE_GRANT_TO_PRINCIPAL_ROLE(PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE),
  REVOKE_PRINCIPAL_ROLE_GRANT_FROM_PRINCIPAL_ROLE(
      PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE, PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
  LIST_GRANTS_ON_PRINCIPAL_ROLE(PRINCIPAL_ROLE_LIST_GRANTS),
  ADD_CATALOG_ROLE_GRANT_TO_CATALOG_ROLE(CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE),
  REVOKE_CATALOG_ROLE_GRANT_FROM_CATALOG_ROLE(
      CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE, CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
  LIST_GRANTS_ON_CATALOG_ROLE(CATALOG_ROLE_LIST_GRANTS),
  ADD_CATALOG_GRANT_TO_CATALOG_ROLE(CATALOG_MANAGE_GRANTS_ON_SECURABLE),
  REVOKE_CATALOG_GRANT_FROM_CATALOG_ROLE(
      CATALOG_MANAGE_GRANTS_ON_SECURABLE, CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
  LIST_GRANTS_ON_CATALOG(CATALOG_LIST_GRANTS),
  ADD_NAMESPACE_GRANT_TO_CATALOG_ROLE(NAMESPACE_MANAGE_GRANTS_ON_SECURABLE),
  REVOKE_NAMESPACE_GRANT_FROM_CATALOG_ROLE(
      NAMESPACE_MANAGE_GRANTS_ON_SECURABLE, CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
  LIST_GRANTS_ON_NAMESPACE(NAMESPACE_LIST_GRANTS),
  ADD_TABLE_GRANT_TO_CATALOG_ROLE(TABLE_MANAGE_GRANTS_ON_SECURABLE),
  REVOKE_TABLE_GRANT_FROM_CATALOG_ROLE(
      TABLE_MANAGE_GRANTS_ON_SECURABLE, CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
  LIST_GRANTS_ON_TABLE(TABLE_LIST_GRANTS),
  ADD_VIEW_GRANT_TO_CATALOG_ROLE(VIEW_MANAGE_GRANTS_ON_SECURABLE),
  REVOKE_VIEW_GRANT_FROM_CATALOG_ROLE(
      VIEW_MANAGE_GRANTS_ON_SECURABLE, CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
  LIST_GRANTS_ON_VIEW(VIEW_LIST_GRANTS),
  ;

  private final EnumSet<PolarisPrivilege> privilegesOnTarget;
  private final EnumSet<PolarisPrivilege> privilegesOnSecondary;

  /** Most common case -- single privilege on target entities. */
  PolarisAuthorizableOperation(PolarisPrivilege targetPrivilege) {
    this(targetPrivilege == null ? null : EnumSet.of(targetPrivilege), null);
  }

  /** Require multiple simultaneous privileges on target entities. */
  PolarisAuthorizableOperation(EnumSet<PolarisPrivilege> privilegesOnTarget) {
    this(privilegesOnTarget, null);
  }

  /** Single privilege on target entities, multiple privileges on secondary. */
  PolarisAuthorizableOperation(
      PolarisPrivilege targetPrivilege, EnumSet<PolarisPrivilege> privilegesOnSecondary) {
    this(targetPrivilege == null ? null : EnumSet.of(targetPrivilege), privilegesOnSecondary);
  }

  /** Single privilege on target, single privilege on targetParent. */
  PolarisAuthorizableOperation(
      PolarisPrivilege targetPrivilege, PolarisPrivilege secondaryPrivilege) {
    this(
        targetPrivilege == null ? null : EnumSet.of(targetPrivilege),
        secondaryPrivilege == null ? null : EnumSet.of(secondaryPrivilege));
  }

  /** EnumSets on target, targetParent */
  PolarisAuthorizableOperation(
      EnumSet<PolarisPrivilege> privilegesOnTarget,
      EnumSet<PolarisPrivilege> privilegesOnSecondary) {
    this.privilegesOnTarget =
        privilegesOnTarget == null ? EnumSet.noneOf(PolarisPrivilege.class) : privilegesOnTarget;
    this.privilegesOnSecondary =
        privilegesOnSecondary == null
            ? EnumSet.noneOf(PolarisPrivilege.class)
            : privilegesOnSecondary;
  }

  public EnumSet<PolarisPrivilege> getPrivilegesOnTarget() {
    return privilegesOnTarget;
  }

  public EnumSet<PolarisPrivilege> getPrivilegesOnSecondary() {
    return privilegesOnSecondary;
  }
}
