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
package org.apache.polaris.service.catalog;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;

/**
 * Resolves whether a catalog is case-insensitive. Provides lightweight lookup for adapter-layer
 * normalization decisions without full authorization resolution.
 */
@RequestScoped
public class CatalogCaseSensitivityResolver {

  private final PolarisMetaStoreManager metaStoreManager;
  private final CallContext callContext;

  @Inject
  public CatalogCaseSensitivityResolver(
      PolarisMetaStoreManager metaStoreManager, CallContext callContext) {
    this.metaStoreManager = metaStoreManager;
    this.callContext = callContext;
  }

  /**
   * Check if a catalog is case-insensitive. Uses a lightweight lookup that only reads the catalog
   * entity without full authorization resolution.
   *
   * @param catalogName the name of the catalog to check
   * @return true if the catalog is case-insensitive, false otherwise (including when catalog
   *     doesn't exist)
   */
  public boolean isCaseInsensitive(String catalogName) {
    if (catalogName == null || catalogName.isEmpty()) {
      return false;
    }

    EntityResult result =
        metaStoreManager.readEntityByName(
            callContext.getPolarisCallContext(),
            null, // catalogPath is null for top-level catalog entities
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            catalogName);

    if (!result.isSuccess() || result.getEntity() == null || result.getEntity().isDropped()) {
      // If catalog doesn't exist or is dropped, default to case-sensitive
      return false;
    }

    CatalogEntity catalogEntity = CatalogEntity.of(result.getEntity());
    return catalogEntity != null && catalogEntity.isCaseInsensitive();
  }
}
