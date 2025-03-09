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
package org.apache.polaris.core.persistence.transactional;

import static org.apache.polaris.core.entity.PolarisEntityType.NULL_TYPE;
import static org.apache.polaris.core.entity.PolarisEntityType.ROOT;

import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.persistence.dao.CommonDao;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.ChangeTrackingResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.jetbrains.annotations.NotNull;

public class FdbCommonDaoImpl implements CommonDao {
  PolarisMetaStoreManagerImpl metaStoreManager = new PolarisMetaStoreManagerImpl();

  @NotNull
  @Override
  public GenerateEntityIdResult generateNewEntityId(@NotNull PolarisCallContext callCtx) {
    return metaStoreManager.generateNewEntityId(callCtx);
  }

  @NotNull
  @Override
  public BaseResult bootstrapPolarisService(@NotNull PolarisCallContext callCtx) {
    return metaStoreManager.bootstrapPolarisService(callCtx);
  }

  @NotNull
  @Override
  public BaseResult purge(@NotNull PolarisCallContext callCtx) {
    return metaStoreManager.purge(callCtx);
  }

  @NotNull
  @Override
  public ResolvedEntityResult loadResolvedEntityByName(
      @NotNull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @NotNull String entityName) {
    return metaStoreManager.loadResolvedEntityByName(
        callCtx, entityCatalogId, parentId, ROOT, entityName);
  }

  @NotNull
  @Override
  public ChangeTrackingResult loadEntitiesChangeTracking(
      @NotNull PolarisCallContext callCtx, @NotNull List<PolarisEntityId> entityIds) {
    return metaStoreManager.loadEntitiesChangeTracking(callCtx, entityIds);
  }

  @NotNull
  @Override
  public ResolvedEntityResult refreshResolvedEntity(
      @NotNull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      long entityCatalogId,
      long entityId) {
    return metaStoreManager.refreshResolvedEntity(
        callCtx, entityVersion, entityGrantRecordsVersion, ROOT, entityCatalogId, entityId);
  }

  @NotNull
  @Override
  public EntityResult loadEntity(
      @NotNull PolarisCallContext callCtx, long entityCatalogId, long entityId) {
    return metaStoreManager.loadEntity(callCtx, entityCatalogId, entityId, NULL_TYPE);
  }
}
