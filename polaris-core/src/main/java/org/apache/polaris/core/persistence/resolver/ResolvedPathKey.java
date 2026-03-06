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
package org.apache.polaris.core.persistence.resolver;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.entity.PolarisEntityType;

/** Canonical lookup key for resolved paths in a {@link PolarisResolutionManifest}. */
public record ResolvedPathKey(List<String> entityNames, PolarisEntityType entityType) {

  public ResolvedPathKey {
    entityNames = List.copyOf(entityNames);
  }

  public static ResolvedPathKey of(List<String> entityNames, PolarisEntityType entityType) {
    return new ResolvedPathKey(entityNames, entityType);
  }

  public static ResolvedPathKey of(ResolverPath path) {
    return new ResolvedPathKey(path.entityNames(), path.lastEntityType());
  }

  public static ResolvedPathKey ofNamespace(Namespace namespace) {
    return new ResolvedPathKey(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE);
  }

  public static ResolvedPathKey ofTableLike(TableIdentifier identifier) {
    return new ResolvedPathKey(
        PolarisCatalogHelpers.tableIdentifierToList(identifier), PolarisEntityType.TABLE_LIKE);
  }

  public static ResolvedPathKey ofPolicy(Namespace namespace, String name) {
    return new ResolvedPathKey(
        PolarisCatalogHelpers.identifierToList(namespace, name), PolarisEntityType.POLICY);
  }

  public static ResolvedPathKey ofCatalogRole(String roleName) {
    return new ResolvedPathKey(List.of(roleName), PolarisEntityType.CATALOG_ROLE);
  }
}
