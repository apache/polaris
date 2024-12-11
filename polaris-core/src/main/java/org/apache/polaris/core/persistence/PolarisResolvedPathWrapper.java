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

import java.util.List;
import org.apache.polaris.core.entity.PolarisEntity;

/**
 * Holds fully-resolved path of PolarisEntities representing the targetEntity with all its grants
 * and grant records.
 */
public class PolarisResolvedPathWrapper {
  private final List<PolarisEntity> resolvedPath;

  // TODO: Distinguish between whether parentPath had a null in the chain or whether only
  // the leaf element was null.
  public PolarisResolvedPathWrapper(List<PolarisEntity> resolvedPath) {
    this.resolvedPath = resolvedPath;
  }

  public PolarisEntity getResolvedLeafEntity() {
    if (resolvedPath == null || resolvedPath.isEmpty()) {
      return null;
    }
    return resolvedPath.get(resolvedPath.size() - 1);
  }

  public PolarisEntity getRawLeafEntity() {
    return getResolvedLeafEntity();
  }

  public List<PolarisEntity> getRawFullPath() {
    return resolvedPath;
  }

  public List<PolarisEntity> getRawParentPath() {
    if (resolvedPath == null) {
      return null;
    }
    return resolvedPath.subList(0, resolvedPath.size() - 1);
  }

  @Override
  public String toString() {
    return "resolvedPath:" + resolvedPath;
  }
}
