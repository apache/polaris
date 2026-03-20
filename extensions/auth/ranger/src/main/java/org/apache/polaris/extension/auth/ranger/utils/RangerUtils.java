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

package org.apache.polaris.extension.auth.ranger.utils;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.ranger.authz.model.RangerAccessInfo;
import org.apache.ranger.authz.model.RangerResourceInfo;
import org.apache.ranger.authz.model.RangerUserInfo;
import org.apache.ranger.authz.util.RangerResourceNameParser;

public class RangerUtils {
  public static String toResourceType(PolarisEntityType entityType) {
    return switch (entityType) {
      case ROOT -> "root";
      case PRINCIPAL -> "principal";
      case CATALOG -> "catalog";
      case NAMESPACE -> "namespace";
      case TABLE_LIKE -> "table";
      case POLICY -> "policy";
      default ->
          throw new UnsupportedOperationException(
              entityType + ": unsupported entity type in Ranger authorizer");
    };
  }

  public static String toAccessType(PolarisPrivilege privilege) {
    return switch (privilege) {
      case SERVICE_MANAGE_ACCESS -> "service-access-manage";

      case PRINCIPAL_CREATE -> "principal-create";
      case PRINCIPAL_DROP -> "principal-drop";
      case PRINCIPAL_LIST -> "principal-list";
      case PRINCIPAL_READ_PROPERTIES -> "principal-properties-read";
      case PRINCIPAL_WRITE_PROPERTIES -> "principal-properties-write";
      case PRINCIPAL_FULL_METADATA -> "principal-metadata-full";
      case PRINCIPAL_ROTATE_CREDENTIALS -> "principal-credentials-rotate";
      case PRINCIPAL_RESET_CREDENTIALS -> "principal-credentials-reset";

      case CATALOG_CREATE -> "catalog-create";
      case CATALOG_DROP -> "catalog-drop";
      case CATALOG_LIST -> "catalog-list";
      case CATALOG_READ_PROPERTIES -> "catalog-properties-read";
      case CATALOG_WRITE_PROPERTIES -> "catalog-properties-write";
      case CATALOG_FULL_METADATA -> "catalog-metadata-full";
      case CATALOG_MANAGE_METADATA -> "catalog-metadata-manage";
      case CATALOG_MANAGE_CONTENT -> "catalog-content-manage";
      case CATALOG_ATTACH_POLICY -> "catalog-policy-attach";
      case CATALOG_DETACH_POLICY -> "catalog-policy-detach";

      case NAMESPACE_CREATE -> "namespace-create";
      case NAMESPACE_DROP -> "namespace-drop";
      case NAMESPACE_LIST -> "namespace-list";
      case NAMESPACE_READ_PROPERTIES -> "namespace-properties-read";
      case NAMESPACE_WRITE_PROPERTIES -> "namespace-properties-write";
      case NAMESPACE_FULL_METADATA -> "namespace-metadata-full";
      case NAMESPACE_ATTACH_POLICY -> "namespace-policy-attach";
      case NAMESPACE_DETACH_POLICY -> "namespace-policy-detach";

      case TABLE_CREATE -> "table-create";
      case TABLE_DROP -> "table-drop";
      case TABLE_LIST -> "table-list";
      case TABLE_READ_PROPERTIES -> "table-properties-read";
      case TABLE_WRITE_PROPERTIES -> "table-properties-write";
      case TABLE_READ_DATA -> "table-data-read";
      case TABLE_WRITE_DATA -> "table-data-write";
      case TABLE_FULL_METADATA -> "table-metadata-full";
      case TABLE_ATTACH_POLICY -> "table-policy-attach";
      case TABLE_DETACH_POLICY -> "table-policy-detach";
      case TABLE_ASSIGN_UUID -> "table-uuid-assign";
      case TABLE_UPGRADE_FORMAT_VERSION -> "table-format-version-upgrade";
      case TABLE_ADD_SCHEMA -> "table-schema-add";
      case TABLE_SET_CURRENT_SCHEMA -> "table-schema-set-current";
      case TABLE_ADD_PARTITION_SPEC -> "table-partition-spec-add";
      case TABLE_ADD_SORT_ORDER -> "table-sort-order-add";
      case TABLE_SET_DEFAULT_SORT_ORDER -> "table-sort-order-set-default";
      case TABLE_ADD_SNAPSHOT -> "table-snapshot-add";
      case TABLE_SET_SNAPSHOT_REF -> "table-snapshot-ref-set";
      case TABLE_REMOVE_SNAPSHOTS -> "table-snapshots-remove";
      case TABLE_REMOVE_SNAPSHOT_REF -> "table-snapshot-ref-remove";
      case TABLE_SET_LOCATION -> "table-location-set";
      case TABLE_SET_PROPERTIES -> "table-properties-set";
      case TABLE_REMOVE_PROPERTIES -> "table-properties-remove";
      case TABLE_SET_STATISTICS -> "table-statistics-set";
      case TABLE_REMOVE_STATISTICS -> "table-statistics-remove";
      case TABLE_REMOVE_PARTITION_SPECS -> "table-partition-specs-remove";
      case TABLE_MANAGE_STRUCTURE -> "table-structure-manage";

      case VIEW_CREATE -> "view-create";
      case VIEW_DROP -> "view-drop";
      case VIEW_LIST -> "view-list";
      case VIEW_READ_PROPERTIES -> "view-properties-read";
      case VIEW_WRITE_PROPERTIES -> "view-properties-write";
      case VIEW_FULL_METADATA -> "view-metadata-full";

      case POLICY_CREATE -> "policy-create";
      case POLICY_READ -> "policy-read";
      case POLICY_DROP -> "policy-drop";
      case POLICY_WRITE -> "policy-write";
      case POLICY_LIST -> "policy-list";
      case POLICY_FULL_METADATA -> "policy-metadata-full";
      case POLICY_ATTACH -> "policy-attach";
      case POLICY_DETACH -> "policy-detach";

      default ->
          throw new UnsupportedOperationException(
              privilege + ": unsupported permission in Ranger authorizer");
    };
  }

  public static RangerUserInfo toUserInfo(PolarisPrincipal principal) {
    return new RangerUserInfo(
        principal.getName(), getUserAttributes(principal), null, principal.getRoles());
  }

  public static RangerAccessInfo toAccessInfo(
      PolarisResolvedPathWrapper entity,
      PolarisAuthorizableOperation authzOp,
      EnumSet<PolarisPrivilege> privileges) {
    return new RangerAccessInfo(
        RangerUtils.toResourceInfo(entity), authzOp.name(), RangerUtils.toPermissions(privileges));
  }

  public static String toResourcePath(List<PolarisResolvedPathWrapper> resolvedPaths) {
    return resolvedPaths.stream().map(RangerUtils::toResourcePath).collect(Collectors.joining(","));
  }

  public static String toResourcePath(PolarisResolvedPathWrapper resolvedPath) {
    StringBuilder sb = new StringBuilder();
    String resourceType =
        toResourceType(resolvedPath.getResolvedLeafEntity().getEntity().getType());

    sb.append(resourceType).append(RangerResourceNameParser.RRN_RESOURCE_TYPE_SEP);

    boolean isFirst = true;

    for (ResolvedPolarisEntity entity : resolvedPath.getResolvedFullPath()) {
      if (!isFirst) {
        sb.append(RangerResourceNameParser.DEFAULT_RRN_RESOURCE_SEP);
      } else {
        isFirst = false;
      }

      sb.append(entity.getEntity().getName());
    }

    return sb.toString();
  }

  private static RangerResourceInfo toResourceInfo(PolarisResolvedPathWrapper resourcePath) {
    RangerResourceInfo ret = new RangerResourceInfo();

    ret.setName(toResourcePath(resourcePath));
    ret.setAttributes(getResourceAttributes(resourcePath));

    return ret;
  }

  private static Set<String> toPermissions(EnumSet<PolarisPrivilege> privileges) {
    return privileges.stream().map(RangerUtils::toAccessType).collect(Collectors.toSet());
  }

  private static Map<String, Object> getResourceAttributes(
      PolarisResolvedPathWrapper resourcePath) {
    Map<String, Object> ret = null;

    for (ResolvedPolarisEntity resolvedEntity : resourcePath.getResolvedFullPath()) {
      PolarisEntity entity = resolvedEntity.getEntity();

      if (StringUtils.isNotBlank(entity.getProperties())) {
        if (ret == null) {
          ret = new HashMap<>(entity.getPropertiesAsMap());
        } else {
          ret.putAll(entity.getPropertiesAsMap());
        }
      }
    }

    return ret == null ? Collections.emptyMap() : ret;
  }

  private static Map<String, Object> getUserAttributes(PolarisPrincipal principal) {
    Map<String, String> properties = principal.getProperties();

    return (properties == null || properties.isEmpty())
        ? Collections.emptyMap()
        : new HashMap<>(properties);
  }
}
