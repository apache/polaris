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

import jakarta.annotation.Nullable;
import java.util.Optional;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;

/**
 * Defines the methods by which a Catalog is expected to access resolved catalog-path entities,
 * typically backed by a PolarisResolutionManifest.
 */
public interface PolarisResolutionManifestCatalogView {
  PolarisResolvedPathWrapper getResolvedReferenceCatalogEntity();

  default @Nullable CatalogEntity getResolvedCatalogEntity() {
    return Optional.ofNullable(getResolvedReferenceCatalogEntity())
        .map(PolarisResolvedPathWrapper::getRawLeafEntity)
        .map(CatalogEntity::of)
        .orElse(null);
  }

  PolarisResolvedPathWrapper getResolvedPath(Object key);

  PolarisResolvedPathWrapper getResolvedPath(
      Object key, PolarisEntityType entityType, PolarisEntitySubType subType);

  PolarisResolvedPathWrapper getPassthroughResolvedPath(Object key);

  PolarisResolvedPathWrapper getPassthroughResolvedPath(
      Object key, PolarisEntityType entityType, PolarisEntitySubType subType);
}
