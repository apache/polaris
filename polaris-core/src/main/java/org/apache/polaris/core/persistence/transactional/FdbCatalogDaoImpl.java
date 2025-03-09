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

import static org.apache.polaris.core.entity.PolarisEntitySubType.ANY_SUBTYPE;
import static org.apache.polaris.core.entity.PolarisEntityType.CATALOG;

import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.dao.CatalogDao;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FdbCatalogDaoImpl implements CatalogDao {
  // TODO we need a map to cache the PolarisMetaStoreManagerImpl as well
  PolarisMetaStoreManagerImpl metaStoreManager = new PolarisMetaStoreManagerImpl();

  @Override
  public CreateCatalogResult createCatalog(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisBaseEntity catalog,
      @NotNull List<PolarisEntityCore> principalRoles) {
    return metaStoreManager.createCatalog(callCtx, catalog, principalRoles);
  }

  @NotNull
  @Override
  public EntityResult readEntityByName(@NotNull PolarisCallContext callCtx, @NotNull String name) {
    return metaStoreManager.readEntityByName(callCtx, null, CATALOG, ANY_SUBTYPE, name);
  }

  @NotNull
  @Override
  public EntityResult loadEntity(
      @NotNull PolarisCallContext callCtx, long entityCatalogId, long entityId) {
    return metaStoreManager.loadEntity(callCtx, entityCatalogId, entityId, CATALOG);
  }

  @NotNull
  @Override
  public ListEntitiesResult listEntities(@NotNull PolarisCallContext callCtx) {
    return metaStoreManager.listEntities(callCtx, null, CATALOG, ANY_SUBTYPE);
  }

  @NotNull
  @Override
  public EntityResult updateEntityPropertiesIfNotChanged(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity entity) {
    return metaStoreManager.updateEntityPropertiesIfNotChanged(callCtx, null, entity);
  }

  @NotNull
  @Override
  public DropEntityResult dropEntityIfExists(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisEntityCore entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    return metaStoreManager.dropEntityIfExists(
        callCtx, null, entityToDrop, cleanupProperties, cleanup);
  }

  @NotNull
  @Override
  public ResolvedEntityResult loadResolvedEntityById(
      @NotNull PolarisCallContext callCtx, long entityCatalogId, long entityId) {
    return metaStoreManager.loadResolvedEntityById(callCtx, entityCatalogId, entityId, CATALOG);
  }

  @NotNull
  @Override
  public ResolvedEntityResult loadResolvedEntityByName(
      @NotNull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @NotNull String entityName) {
    return metaStoreManager.loadResolvedEntityByName(
        callCtx, entityCatalogId, parentId, CATALOG, entityName);
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
        callCtx, entityVersion, entityGrantRecordsVersion, CATALOG, entityCatalogId, entityId);
  }
}
