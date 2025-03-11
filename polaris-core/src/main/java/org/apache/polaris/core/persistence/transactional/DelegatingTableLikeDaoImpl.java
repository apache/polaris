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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
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

public class DelegatingTableLikeDaoImpl implements TableLikeDao {
  PolarisMetaStoreManagerImpl metaStoreManager = new PolarisMetaStoreManagerImpl();

  @Override
  public @Nonnull EntityResult readEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name) {
    return metaStoreManager.readEntityByName(callCtx, catalogPath, TABLE_LIKE, entitySubType, name);
  }

  @Nonnull
  @Override
  public EntityResult loadEntity(
      @Nonnull PolarisCallContext callCtx, long entityCatalogId, long entityId) {
    return metaStoreManager.loadEntity(callCtx, entityCatalogId, entityId, TABLE_LIKE);
  }

  @Nonnull
  @Override
  public ListEntitiesResult listEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntitySubType entitySubType) {
    return metaStoreManager.listEntities(callCtx, catalogPath, TABLE_LIKE, entitySubType);
  }

  @Nonnull
  @Override
  public EntityResult renameEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @Nonnull PolarisEntity renamedEntity) {
    return metaStoreManager.renameEntity(
        callCtx, catalogPath, entityToRename, newCatalogPath, renamedEntity);
  }

  @Nonnull
  @Override
  public ResolvedEntityResult loadResolvedEntityById(
      @Nonnull PolarisCallContext callCtx, long entityCatalogId, long entityId) {
    return metaStoreManager.loadResolvedEntityById(callCtx, entityCatalogId, entityId, TABLE_LIKE);
  }

  @Nonnull
  @Override
  public ResolvedEntityResult loadResolvedEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @Nonnull String entityName) {
    return metaStoreManager.loadResolvedEntityByName(
        callCtx, entityCatalogId, parentId, TABLE_LIKE, entityName);
  }

  @Nonnull
  @Override
  public ResolvedEntityResult refreshResolvedEntity(
      @Nonnull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      long entityCatalogId,
      long entityId) {
    return metaStoreManager.refreshResolvedEntity(
        callCtx, entityVersion, entityGrantRecordsVersion, TABLE_LIKE, entityCatalogId, entityId);
  }

  @Nonnull
  @Override
  public EntityResult createEntityIfNotExists(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    return metaStoreManager.createEntityIfNotExists(callCtx, catalogPath, entity);
  }

  @Nonnull
  @Override
  public EntityResult updateEntityPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    return metaStoreManager.updateEntityPropertiesIfNotChanged(callCtx, catalogPath, entity);
  }

  @Nonnull
  @Override
  public DropEntityResult dropEntityIfExists(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    return metaStoreManager.dropEntityIfExists(
        callCtx, catalogPath, entityToDrop, cleanupProperties, cleanup);
  }

  @Nonnull
  @Override
  public EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<EntityWithPath> entities) {
    return metaStoreManager.updateEntitiesPropertiesIfNotChanged(callCtx, entities);
  }
}
