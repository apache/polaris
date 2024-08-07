/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.core.persistence;

import io.polaris.core.entity.PolarisEntity;
import java.util.List;
import java.util.stream.Collectors;

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

  @Override
  public String toString() {
    return "resolvedPath:" + resolvedPath;
  }
}
