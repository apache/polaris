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
package org.apache.polaris.core.persistence;

import jakarta.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;

/**
 * Holds fully-resolved path of PolarisEntities representing the targetEntity with all its grants
 * and grant records.
 */
public class PolarisResolvedPathWrapper {
  private final List<ResolvedPolarisEntity> resolvedPath;

  // TODO: Distinguish between whether parentPath had a null in the chain or whether only
  // the leaf element was null.
  public PolarisResolvedPathWrapper(List<ResolvedPolarisEntity> resolvedPath) {
    this.resolvedPath = resolvedPath;
  }

  public ResolvedPolarisEntity getResolvedLeafEntity() {
    if (resolvedPath == null || resolvedPath.isEmpty()) {
      return null;
    }
    return resolvedPath.get(resolvedPath.size() - 1);
  }

  public PolarisEntity getRawLeafEntity() {
    ResolvedPolarisEntity resolvedEntity = getResolvedLeafEntity();
    if (resolvedEntity != null) {
      return resolvedEntity.getEntity();
    }
    return null;
  }

  public List<ResolvedPolarisEntity> getResolvedFullPath() {
    return resolvedPath;
  }

  public List<PolarisEntity> getRawFullPath() {
    if (resolvedPath == null) {
      return null;
    }
    return resolvedPath.stream().map(ResolvedPolarisEntity::getEntity).collect(Collectors.toList());
  }

  public List<ResolvedPolarisEntity> getResolvedParentPath() {
    if (resolvedPath == null) {
      return null;
    }
    return resolvedPath.subList(0, resolvedPath.size() - 1);
  }

  public List<PolarisEntity> getRawParentPath() {
    if (resolvedPath == null) {
      return null;
    }
    return getResolvedParentPath().stream()
        .map(ResolvedPolarisEntity::getEntity)
        .collect(Collectors.toList());
  }

  /**
   * Checks if a namespace is fully resolved.
   *
   * @param catalogName the name of the catalog
   * @param namespace the namespace we're trying to resolve
   * @return true if the namespace is considered fully resolved for the given catalog type
   */
  public boolean isFullyResolvedNamespace(
      @Nonnull String catalogName, @Nonnull Namespace namespace) {
    if (resolvedPath == null) {
      return false;
    }

    List<PolarisEntity> fullPath = getRawFullPath();
    int expectedPathLength = 1 + namespace.levels().length;
    if (fullPath.size() != expectedPathLength) {
      return false;
    }

    if (!fullPath.get(0).getName().equals(catalogName)) {
      return false;
    }

    String[] namespaceLevels = namespace.levels();
    int levelsLength = namespaceLevels.length;
    Iterator<PolarisEntity> fullPathIterator = fullPath.iterator();
    fullPathIterator.next();
    for (int i = 0; i < levelsLength; i++) {
      PolarisEntity entity = fullPathIterator.next();
      if (!entity.getName().equals(namespaceLevels[i])
          || entity.getType() != PolarisEntityType.NAMESPACE) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return "resolvedPath:" + resolvedPath;
  }
}
