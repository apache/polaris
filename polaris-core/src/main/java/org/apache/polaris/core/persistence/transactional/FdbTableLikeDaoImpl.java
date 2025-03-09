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

import static org.apache.polaris.core.entity.PolarisEntityType.TABLE_LIKE;

import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.persistence.dao.TableLikeDao;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FdbTableLikeDaoImpl implements TableLikeDao {
  // TODO we need a map to cache the PolarisMetaStoreManagerImpl as well
  PolarisMetaStoreManagerImpl metaStoreManager = new PolarisMetaStoreManagerImpl();

  @Override
  public @NotNull EntityResult readEntityByName(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntitySubType entitySubType,
      @NotNull String name) {
    return metaStoreManager.readEntityByName(callCtx, catalogPath, TABLE_LIKE, entitySubType, name);
  }

  @NotNull
  @Override
  public EntityResult loadEntity(
      @NotNull PolarisCallContext callCtx, long entityCatalogId, long entityId) {
    return metaStoreManager.loadEntity(callCtx, entityCatalogId, entityId, TABLE_LIKE);
  }

  @NotNull
  @Override
  public ListEntitiesResult listEntities(
      @NotNull PolarisCallContext callCtx,
      @NotNull List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntitySubType entitySubType) {
    return metaStoreManager.listEntities(callCtx, catalogPath, TABLE_LIKE, entitySubType);
  }

  @NotNull
  @Override
  public EntityResult renameEntity(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @NotNull PolarisEntity renamedEntity) {
    return metaStoreManager.renameEntity(
        callCtx, catalogPath, entityToRename, newCatalogPath, renamedEntity);
  }

  @NotNull
  @Override
  public ResolvedEntityResult loadResolvedEntityById(
      @NotNull PolarisCallContext callCtx, long entityCatalogId, long entityId) {
    return metaStoreManager.loadResolvedEntityById(callCtx, entityCatalogId, entityId, TABLE_LIKE);
  }

  @NotNull
  @Override
  public ResolvedEntityResult loadResolvedEntityByName(
      @NotNull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @NotNull String entityName) {
    return metaStoreManager.loadResolvedEntityByName(
        callCtx, entityCatalogId, parentId, TABLE_LIKE, entityName);
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
        callCtx, entityVersion, entityGrantRecordsVersion, TABLE_LIKE, entityCatalogId, entityId);
  }

  @NotNull
  @Override
  public EntityResult createEntityIfNotExists(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisBaseEntity entity) {
    return metaStoreManager.createEntityIfNotExists(callCtx, catalogPath, entity);
  }

  @NotNull
  @Override
  public EntityResult updateEntityPropertiesIfNotChanged(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisBaseEntity entity) {
    return metaStoreManager.updateEntityPropertiesIfNotChanged(callCtx, catalogPath, entity);
  }

  @NotNull
  @Override
  public DropEntityResult dropEntityIfExists(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    return metaStoreManager.dropEntityIfExists(
        callCtx, catalogPath, entityToDrop, cleanupProperties, cleanup);
  }

  @NotNull
  @Override
  public EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @NotNull PolarisCallContext callCtx, @NotNull List<EntityWithPath> entities) {
    return metaStoreManager.updateEntitiesPropertiesIfNotChanged(callCtx, entities);
  }
}
