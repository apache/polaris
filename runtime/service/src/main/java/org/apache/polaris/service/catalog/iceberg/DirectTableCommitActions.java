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
package org.apache.polaris.service.catalog.iceberg;

import java.util.List;
import org.apache.iceberg.CatalogUtil;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.jspecify.annotations.NonNull;

/** Applies table-commit actions immediately. */
final class DirectTableCommitActions implements TableCommitActions {
  private final PolarisMetaStoreManager metaStoreManager;

  DirectTableCommitActions(@NonNull PolarisMetaStoreManager metaStoreManager) {
    this.metaStoreManager = metaStoreManager;
  }

  @Override
  public EntityResult updateEntityPropertiesIfNotChanged(
      @NonNull PolarisCallContext callContext,
      @NonNull List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity) {
    return metaStoreManager.updateEntityPropertiesIfNotChanged(callContext, catalogPath, entity);
  }

  @Override
  public void cleanupMetadataFiles(@NonNull MetadataFileCleanup cleanup) {
    CatalogUtil.deleteRemovedMetadataFiles(
        cleanup.io(), cleanup.baseMetadata(), cleanup.newMetadata());
  }
}
