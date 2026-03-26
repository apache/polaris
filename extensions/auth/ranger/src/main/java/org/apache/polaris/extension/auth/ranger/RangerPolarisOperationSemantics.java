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

package org.apache.polaris.extension.auth.ranger;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Polaris RBAC-specific interpretation of an authorizable operation. */
public record RangerPolarisOperationSemantics(
    Set<String> targetPrivileges, Set<String> secondaryPrivileges, ResolvedPathRooting rooting) {
  private static final Logger LOG = LoggerFactory.getLogger(RangerPolarisOperationSemantics.class);

  public RangerPolarisOperationSemantics {
    Preconditions.checkNotNull(targetPrivileges, "targetPrivileges must be non-null");
    Preconditions.checkArgument(!targetPrivileges.isEmpty(), "targetPrivileges must be non-empty");
    Preconditions.checkNotNull(rooting, "rooting must be non-null");
    targetPrivileges = Set.copyOf(targetPrivileges);
    secondaryPrivileges = secondaryPrivileges == null ? Set.of() : Set.copyOf(secondaryPrivileges);
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

  private static final String PRINCIPAL_CREATE = "principal-create";
  private static final String PRINCIPAL_DROP = "principal-drop";
  private static final String PRINCIPAL_LIST = "principal-list";
  private static final String PRINCIPAL_READ_PROPERTIES = "principal-properties-read";
  private static final String PRINCIPAL_WRITE_PROPERTIES = "principal-properties-write";
  private static final String PRINCIPAL_ROTATE_CREDENTIALS = "principal-credentials-rotate";
  private static final String PRINCIPAL_RESET_CREDENTIALS = "principal-credentials-reset";

  private static final String CATALOG_CREATE = "catalog-create";
  private static final String CATALOG_DROP = "catalog-drop";
  private static final String CATALOG_LIST = "catalog-list";
  private static final String CATALOG_READ_PROPERTIES = "catalog-properties-read";
  private static final String CATALOG_WRITE_PROPERTIES = "catalog-properties-write";
  private static final String CATALOG_ATTACH_POLICY = "catalog-policy-attach";
  private static final String CATALOG_DETACH_POLICY = "catalog-policy-detach";

  private static final String NAMESPACE_CREATE = "namespace-create";
  private static final String NAMESPACE_DROP = "namespace-drop";
  private static final String NAMESPACE_LIST = "namespace-list";
  private static final String NAMESPACE_READ_PROPERTIES = "namespace-properties-read";
  private static final String NAMESPACE_WRITE_PROPERTIES = "namespace-properties-write";
  private static final String NAMESPACE_ATTACH_POLICY = "namespace-policy-attach";
  private static final String NAMESPACE_DETACH_POLICY = "namespace-policy-detach";

  private static final String TABLE_CREATE = "table-create";
  private static final String TABLE_DROP = "table-drop";
  private static final String TABLE_LIST = "table-list";
  private static final String TABLE_READ_PROPERTIES = "table-properties-read";
  private static final String TABLE_WRITE_PROPERTIES = "table-properties-write";
  private static final String TABLE_READ_DATA = "table-data-read";
  private static final String TABLE_WRITE_DATA = "table-data-write";
  private static final String TABLE_ATTACH_POLICY = "table-policy-attach";
  private static final String TABLE_DETACH_POLICY = "table-policy-detach";
  private static final String TABLE_ASSIGN_UUID = "table-uuid-assign";
  private static final String TABLE_UPGRADE_FORMAT_VERSION = "table-format-version-upgrade";
  private static final String TABLE_ADD_SCHEMA = "table-schema-add";
  private static final String TABLE_SET_CURRENT_SCHEMA = "table-schema-set-current";
  private static final String TABLE_ADD_PARTITION_SPEC = "table-partition-spec-add";
  private static final String TABLE_ADD_SORT_ORDER = "table-sort-order-add";
  private static final String TABLE_SET_DEFAULT_SORT_ORDER = "table-sort-order-set-default";
  private static final String TABLE_ADD_SNAPSHOT = "table-snapshot-add";
  private static final String TABLE_SET_SNAPSHOT_REF = "table-snapshot-ref-set";
  private static final String TABLE_REMOVE_SNAPSHOTS = "table-snapshots-remove";
  private static final String TABLE_REMOVE_SNAPSHOT_REF = "table-snapshot-ref-remove";
  private static final String TABLE_SET_LOCATION = "table-location-set";
  private static final String TABLE_SET_PROPERTIES = "table-properties-set";
  private static final String TABLE_REMOVE_PROPERTIES = "table-properties-remove";
  private static final String TABLE_SET_STATISTICS = "table-statistics-set";
  private static final String TABLE_REMOVE_STATISTICS = "table-statistics-remove";
  private static final String TABLE_REMOVE_PARTITION_SPECS = "table-partition-specs-remove";

  private static final String VIEW_CREATE = "view-create";
  private static final String VIEW_DROP = "view-drop";
  private static final String VIEW_LIST = "view-list";
  private static final String VIEW_READ_PROPERTIES = "view-properties-read";
  private static final String VIEW_WRITE_PROPERTIES = "view-properties-write";

  private static final String POLICY_CREATE = "policy-create";
  private static final String POLICY_READ = "policy-read";
  private static final String POLICY_DROP = "policy-drop";
  private static final String POLICY_WRITE = "policy-write";
  private static final String POLICY_LIST = "policy-list";
  private static final String POLICY_ATTACH = "policy-attach";
  private static final String POLICY_DETACH = "policy-detach";

  private static final EnumMap<PolarisAuthorizableOperation, RangerPolarisOperationSemantics>
      RBAC_SEMANTICS_BY_OPERATION = new EnumMap<>(PolarisAuthorizableOperation.class);

  static {
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_NAMESPACES,
        new RangerPolarisOperationSemantics(toSet(NAMESPACE_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_NAMESPACE,
        new RangerPolarisOperationSemantics(
            toSet(NAMESPACE_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LOAD_NAMESPACE_METADATA,
        new RangerPolarisOperationSemantics(
            toSet(NAMESPACE_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.NAMESPACE_EXISTS,
        new RangerPolarisOperationSemantics(toSet(NAMESPACE_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DROP_NAMESPACE,
        new RangerPolarisOperationSemantics(toSet(NAMESPACE_DROP), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPDATE_NAMESPACE_PROPERTIES,
        new RangerPolarisOperationSemantics(
            toSet(NAMESPACE_WRITE_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_TABLES,
        new RangerPolarisOperationSemantics(toSet(TABLE_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_TABLE_DIRECT,
        new RangerPolarisOperationSemantics(toSet(TABLE_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_CREATE, TABLE_WRITE_DATA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_TABLE_STAGED,
        new RangerPolarisOperationSemantics(toSet(TABLE_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_CREATE, TABLE_WRITE_DATA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REGISTER_TABLE,
        new RangerPolarisOperationSemantics(toSet(TABLE_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LOAD_TABLE,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LOAD_TABLE_WITH_READ_DELEGATION,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_READ_DATA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LOAD_TABLE_WITH_WRITE_DELEGATION,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_WRITE_DATA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPDATE_TABLE,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_WRITE_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPDATE_TABLE_FOR_STAGED_CREATE,
        new RangerPolarisOperationSemantics(toSet(TABLE_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DROP_TABLE_WITHOUT_PURGE,
        new RangerPolarisOperationSemantics(toSet(TABLE_DROP), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DROP_TABLE_WITH_PURGE,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_DROP, TABLE_WRITE_DATA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.TABLE_EXISTS,
        new RangerPolarisOperationSemantics(toSet(TABLE_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.RENAME_TABLE,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_DROP), toSet(TABLE_LIST, TABLE_CREATE), ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.COMMIT_TRANSACTION,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_WRITE_PROPERTIES, TABLE_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_VIEWS,
        new RangerPolarisOperationSemantics(toSet(VIEW_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_VIEW,
        new RangerPolarisOperationSemantics(toSet(VIEW_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LOAD_VIEW,
        new RangerPolarisOperationSemantics(
            toSet(VIEW_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REPLACE_VIEW,
        new RangerPolarisOperationSemantics(
            toSet(VIEW_WRITE_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DROP_VIEW,
        new RangerPolarisOperationSemantics(toSet(VIEW_DROP), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.VIEW_EXISTS,
        new RangerPolarisOperationSemantics(toSet(VIEW_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.RENAME_VIEW,
        new RangerPolarisOperationSemantics(
            toSet(VIEW_DROP), toSet(VIEW_LIST, VIEW_CREATE), ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REPORT_READ_METRICS,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_READ_DATA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REPORT_WRITE_METRICS,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_WRITE_DATA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.SEND_NOTIFICATIONS,
        new RangerPolarisOperationSemantics(
            new HashSet<>(
                Arrays.asList(
                    TABLE_CREATE,
                    TABLE_WRITE_PROPERTIES,
                    TABLE_DROP,
                    NAMESPACE_CREATE,
                    NAMESPACE_DROP)),
            null,
            ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_CATALOGS,
        new RangerPolarisOperationSemantics(toSet(CATALOG_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_CATALOG,
        new RangerPolarisOperationSemantics(toSet(CATALOG_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.GET_CATALOG,
        new RangerPolarisOperationSemantics(
            toSet(CATALOG_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPDATE_CATALOG,
        new RangerPolarisOperationSemantics(
            toSet(CATALOG_WRITE_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DELETE_CATALOG,
        new RangerPolarisOperationSemantics(toSet(CATALOG_DROP), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_PRINCIPALS,
        new RangerPolarisOperationSemantics(toSet(PRINCIPAL_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_PRINCIPAL,
        new RangerPolarisOperationSemantics(
            toSet(PRINCIPAL_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.GET_PRINCIPAL,
        new RangerPolarisOperationSemantics(
            toSet(PRINCIPAL_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPDATE_PRINCIPAL,
        new RangerPolarisOperationSemantics(
            toSet(PRINCIPAL_WRITE_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DELETE_PRINCIPAL,
        new RangerPolarisOperationSemantics(toSet(PRINCIPAL_DROP), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ROTATE_CREDENTIALS,
        new RangerPolarisOperationSemantics(
            toSet(PRINCIPAL_ROTATE_CREDENTIALS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.RESET_CREDENTIALS,
        new RangerPolarisOperationSemantics(
            toSet(PRINCIPAL_RESET_CREDENTIALS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.CREATE_POLICY,
        new RangerPolarisOperationSemantics(toSet(POLICY_CREATE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LOAD_POLICY,
        new RangerPolarisOperationSemantics(toSet(POLICY_READ), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DROP_POLICY,
        new RangerPolarisOperationSemantics(toSet(POLICY_DROP), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPDATE_POLICY,
        new RangerPolarisOperationSemantics(toSet(POLICY_WRITE), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.LIST_POLICY,
        new RangerPolarisOperationSemantics(toSet(POLICY_LIST), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ATTACH_POLICY_TO_CATALOG,
        new RangerPolarisOperationSemantics(
            toSet(POLICY_ATTACH), toSet(CATALOG_ATTACH_POLICY), ResolvedPathRooting.CATALOG));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ATTACH_POLICY_TO_NAMESPACE,
        new RangerPolarisOperationSemantics(
            toSet(POLICY_ATTACH), toSet(NAMESPACE_ATTACH_POLICY), ResolvedPathRooting.CATALOG));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ATTACH_POLICY_TO_TABLE,
        new RangerPolarisOperationSemantics(
            toSet(POLICY_ATTACH), toSet(TABLE_ATTACH_POLICY), ResolvedPathRooting.CATALOG));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DETACH_POLICY_FROM_CATALOG,
        new RangerPolarisOperationSemantics(
            toSet(POLICY_DETACH), toSet(CATALOG_DETACH_POLICY), ResolvedPathRooting.CATALOG));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DETACH_POLICY_FROM_NAMESPACE,
        new RangerPolarisOperationSemantics(
            toSet(POLICY_DETACH), toSet(NAMESPACE_DETACH_POLICY), ResolvedPathRooting.CATALOG));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.DETACH_POLICY_FROM_TABLE,
        new RangerPolarisOperationSemantics(
            toSet(POLICY_DETACH), toSet(TABLE_DETACH_POLICY), ResolvedPathRooting.CATALOG));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_CATALOG,
        new RangerPolarisOperationSemantics(
            toSet(CATALOG_READ_PROPERTIES), null, ResolvedPathRooting.CATALOG));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_NAMESPACE,
        new RangerPolarisOperationSemantics(
            toSet(NAMESPACE_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_TABLE,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_READ_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ASSIGN_TABLE_UUID,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_ASSIGN_UUID), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.UPGRADE_TABLE_FORMAT_VERSION,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_UPGRADE_FORMAT_VERSION), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_TABLE_SCHEMA,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_ADD_SCHEMA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.SET_TABLE_CURRENT_SCHEMA,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_SET_CURRENT_SCHEMA), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_TABLE_PARTITION_SPEC,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_ADD_PARTITION_SPEC), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_TABLE_SORT_ORDER,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_ADD_SORT_ORDER), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.SET_TABLE_DEFAULT_SORT_ORDER,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_SET_DEFAULT_SORT_ORDER), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.ADD_TABLE_SNAPSHOT,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_ADD_SNAPSHOT), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.SET_TABLE_SNAPSHOT_REF,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_SET_SNAPSHOT_REF), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REMOVE_TABLE_SNAPSHOTS,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_REMOVE_SNAPSHOTS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REMOVE_TABLE_SNAPSHOT_REF,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_REMOVE_SNAPSHOT_REF), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.SET_TABLE_LOCATION,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_SET_LOCATION), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.SET_TABLE_PROPERTIES,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_SET_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REMOVE_TABLE_PROPERTIES,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_REMOVE_PROPERTIES), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.SET_TABLE_STATISTICS,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_SET_STATISTICS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REMOVE_TABLE_STATISTICS,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_REMOVE_STATISTICS), null, ResolvedPathRooting.ROOT));
    RBAC_SEMANTICS_BY_OPERATION.put(
        PolarisAuthorizableOperation.REMOVE_TABLE_PARTITION_SPECS,
        new RangerPolarisOperationSemantics(
            toSet(TABLE_REMOVE_PARTITION_SPECS), null, ResolvedPathRooting.ROOT));

    if (LOG.isDebugEnabled()) {
      EnumSet<PolarisAuthorizableOperation> missing =
          EnumSet.allOf(PolarisAuthorizableOperation.class);

      missing.removeAll(RBAC_SEMANTICS_BY_OPERATION.keySet());

      if (!missing.isEmpty()) {
        LOG.debug("Unhandled operations: {}", missing);
      }
    }
  }

  static Set<String> toSet(String str) {
    return Collections.singleton(str);
  }

  static Set<String> toSet(String str1, String str2) {
    Set<String> ret = new HashSet<>();

    ret.add(str1);
    ret.add(str2);

    return Collections.unmodifiableSet(ret);
  }

  static RangerPolarisOperationSemantics forOperation(PolarisAuthorizableOperation operation) {
    return RBAC_SEMANTICS_BY_OPERATION.get(operation);
  }
}
