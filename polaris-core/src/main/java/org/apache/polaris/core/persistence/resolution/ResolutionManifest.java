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
package org.apache.polaris.core.persistence.resolution;

import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;

/** Holds already resolved "name paths" to entities. */
public interface ResolutionManifest {
  PolarisResolvedPathWrapper getResolvedReferenceCatalogEntity();

  default PolarisResolvedPathWrapper getResolvedPath(Namespace namespace) {
    return getResolvedPath(namespace, false);
  }

  PolarisResolvedPathWrapper getResolvedPath(Namespace namespace, boolean prependRootContainer);

  default PolarisResolvedPathWrapper getResolvedPath(TableIdentifier tableIdentifier) {
    return getResolvedPath(tableIdentifier, false);
  }

  /**
   * @param tableIdentifier the key associated with the path to retrieve that was specified in
   *     addPath
   * @param prependRootContainer if true, also includes the rootContainer as the first element of
   *     the path; otherwise, the first element begins with the referenceCatalog.
   * @return null if the path resolved for {@code key} isn't fully-resolved when specified as
   *     "optional"
   */
  PolarisResolvedPathWrapper getResolvedPath(
      TableIdentifier tableIdentifier, boolean prependRootContainer);

  default PolarisResolvedPathWrapper getResolvedPath(
      TableIdentifier tableIdentifier, PolarisEntitySubType subType) {
    return getResolvedPath(tableIdentifier, subType, false);
  }

  PolarisResolvedPathWrapper getResolvedPath(
      TableIdentifier tableIdentifier, PolarisEntitySubType subType, boolean prependRootContainer);

  default PolarisResolvedPathWrapper getResolvedPath(String name) {
    return getResolvedPath(name, false);
  }

  PolarisResolvedPathWrapper getResolvedPath(String name, boolean prependRootContainer);

  PolarisResolvedPathWrapper getResolvedRootContainerEntityAsPath();

  PolarisResolvedPathWrapper getPassthroughResolvedPath(Namespace namespace);

  PolarisResolvedPathWrapper getPassthroughResolvedPath(
      TableIdentifier tableIdentifier, PolarisEntitySubType subType);

  PolarisResolvedPathWrapper getResolvedTopLevelEntity(
      String name, PolarisEntityType polarisEntityType);

  Set<PolarisBaseEntity> getAllActivatedPrincipalRoleEntities();

  Set<PolarisBaseEntity> getAllActivatedCatalogRoleAndPrincipalRoles();

  PolarisEntitySubType getLeafSubType(TableIdentifier tableIdentifier);
}
