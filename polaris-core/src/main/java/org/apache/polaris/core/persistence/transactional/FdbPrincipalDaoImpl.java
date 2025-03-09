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

import static org.apache.polaris.core.entity.PolarisEntitySubType.NULL_SUBTYPE;
import static org.apache.polaris.core.entity.PolarisEntityType.PRINCIPAL;

import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.dao.PrincipalDao;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FdbPrincipalDaoImpl implements PrincipalDao {
  PolarisMetaStoreManagerImpl metaStoreManager = new PolarisMetaStoreManagerImpl();

  @Override
  public @NotNull EntityResult readEntityByName(
      @NotNull PolarisCallContext callCtx, @NotNull String name) {
    return metaStoreManager.readEntityByName(callCtx, null, PRINCIPAL, NULL_SUBTYPE, name);
  }

  @NotNull
  @Override
  public EntityResult loadEntity(@NotNull PolarisCallContext callCtx, long id) {
    return metaStoreManager.loadEntity(callCtx, 0L, id, PRINCIPAL);
  }

  @NotNull
  @Override
  public ListEntitiesResult listEntities(@NotNull PolarisCallContext callCtx) {
    return metaStoreManager.listEntities(callCtx, null, PRINCIPAL, NULL_SUBTYPE);
  }

  @Override
  public CreatePrincipalResult createPrincipal(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity principal) {
    return metaStoreManager.createPrincipal(callCtx, principal);
  }

  @NotNull
  @Override
  public ResolvedEntityResult loadResolvedEntityById(@NotNull PolarisCallContext callCtx, long id) {
    return metaStoreManager.loadResolvedEntityById(callCtx, 0L, id, PRINCIPAL);
  }

  @NotNull
  @Override
  public ResolvedEntityResult loadResolvedEntityByName(
      @NotNull PolarisCallContext callCtx, long parentId, @NotNull String name) {
    return metaStoreManager.loadResolvedEntityByName(callCtx, 0L, parentId, PRINCIPAL, name);
  }

  @NotNull
  @Override
  public ResolvedEntityResult refreshResolvedEntity(
      @NotNull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      long id) {
    return metaStoreManager.refreshResolvedEntity(
        callCtx, entityVersion, entityGrantRecordsVersion, PRINCIPAL, 0L, id);
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
}
