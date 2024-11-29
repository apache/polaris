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

import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;

/**
 * Defines which sub-set of a {@link PolarisResolutionManifest} should be used for various
 * authorization check parameters.
 */
public interface AuthEntitySelector {

  PolarisResolvedPathWrapper fromManifest(PolarisResolutionManifest manifest);

  static AuthEntitySelector rootContainer() {
    return PolarisResolutionManifest::getResolvedRootContainerEntityAsPath;
  }

  static AuthEntitySelector topLevelEntity(String entityName, PolarisEntityType type) {
    return m -> m.getResolvedTopLevelEntity(entityName, type);
  }

  static AuthEntitySelector path(Object key, PolarisEntitySubType subType) {
    return m -> m.getResolvedPath(key, subType, true);
  }

  static AuthEntitySelector path(
      Object key, PolarisEntitySubType subType, Runnable notFoundHandler) {
    return m -> {
      PolarisResolvedPathWrapper resolved = m.getResolvedPath(key, subType, true);
      if (resolved == null) {
        notFoundHandler.run();
      }
      return resolved;
    };
  }

  static AuthEntitySelector path(Object key) {
    return m -> m.getResolvedPath(key, true);
  }

  static AuthEntitySelector path(Object key, Runnable notFoundHandler) {
    return m -> {
      PolarisResolvedPathWrapper resolved = m.getResolvedPath(key, true);
      if (resolved == null) {
        notFoundHandler.run();
      }
      return resolved;
    };
  }
}
