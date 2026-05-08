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
      default -> throw new UnsupportedOperationException(entityType + ": unsupported entity type");
    };
  }

  public static RangerUserInfo toUserInfo(PolarisPrincipal principal) {
    return new RangerUserInfo(
        principal.getName(), getUserAttributes(principal), null, principal.getRoles());
  }

  public static RangerAccessInfo toAccessInfo(
      PolarisResolvedPathWrapper entity,
      PolarisAuthorizableOperation authzOp,
      Set<String> privileges,
      String realmContextId) {
    return new RangerAccessInfo(
        RangerUtils.toResourceInfo(entity, realmContextId), authzOp.name(), privileges);
  }

  public static String toResourcePath(
      List<PolarisResolvedPathWrapper> resolvedPaths, String realmContextId) {
    return resolvedPaths.stream()
        .map(s -> RangerUtils.toResourcePath(s, realmContextId))
        .collect(Collectors.joining(","));
  }

  public static String toResourcePath(
      PolarisResolvedPathWrapper resolvedPath, String realmContextId) {
    StringBuilder sb = new StringBuilder();
    String resourceType =
        toResourceType(resolvedPath.getResolvedLeafEntity().getEntity().getType());

    sb.append(resourceType).append(RangerResourceNameParser.RRN_RESOURCE_TYPE_SEP);

    boolean isFirst = true;
    for (ResolvedPolarisEntity entity : resolvedPath.getResolvedFullPath()) {
      if (isFirst) {
        sb.append(realmContextId);
        isFirst = false;
        if (entity.getEntity().getType() != PolarisEntityType.ROOT) {
          sb.append(RangerResourceNameParser.DEFAULT_RRN_RESOURCE_SEP)
              .append(entity.getEntity().getName());
        }
      } else {
        sb.append(RangerResourceNameParser.DEFAULT_RRN_RESOURCE_SEP)
            .append(entity.getEntity().getName());
      }
    }
    return sb.toString();
  }

  private static RangerResourceInfo toResourceInfo(
      PolarisResolvedPathWrapper resourcePath, String realmContextId) {
    RangerResourceInfo ret = new RangerResourceInfo();

    ret.setName(toResourcePath(resourcePath, realmContextId));
    ret.setAttributes(getResourceAttributes(resourcePath));

    return ret;
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
