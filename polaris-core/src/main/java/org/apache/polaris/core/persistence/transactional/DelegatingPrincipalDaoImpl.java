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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
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

public class DelegatingPrincipalDaoImpl implements PrincipalDao {
  PolarisMetaStoreManagerImpl metaStoreManager = new PolarisMetaStoreManagerImpl();

  @Override
  public @Nonnull EntityResult readEntityByName(
      @Nonnull PolarisCallContext callCtx, @Nonnull String name) {
    return metaStoreManager.readEntityByName(callCtx, null, PRINCIPAL, NULL_SUBTYPE, name);
  }

  @Nonnull
  @Override
  public EntityResult loadEntity(@Nonnull PolarisCallContext callCtx, long id) {
    return metaStoreManager.loadEntity(callCtx, 0L, id, PRINCIPAL);
  }

  @Nonnull
  @Override
  public ListEntitiesResult listEntities(@Nonnull PolarisCallContext callCtx) {
    return metaStoreManager.listEntities(callCtx, null, PRINCIPAL, NULL_SUBTYPE);
  }

  @Override
  public CreatePrincipalResult createPrincipal(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity principal) {
    return metaStoreManager.createPrincipal(callCtx, principal);
  }

  @Nonnull
  @Override
  public ResolvedEntityResult loadResolvedEntityById(@Nonnull PolarisCallContext callCtx, long id) {
    return metaStoreManager.loadResolvedEntityById(callCtx, 0L, id, PRINCIPAL);
  }

  @Nonnull
  @Override
  public ResolvedEntityResult loadResolvedEntityByName(
      @Nonnull PolarisCallContext callCtx, long parentId, @Nonnull String name) {
    return metaStoreManager.loadResolvedEntityByName(callCtx, 0L, parentId, PRINCIPAL, name);
  }

  @Nonnull
  @Override
  public ResolvedEntityResult refreshResolvedEntity(
      @Nonnull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      long id) {
    return metaStoreManager.refreshResolvedEntity(
        callCtx, entityVersion, entityGrantRecordsVersion, PRINCIPAL, 0L, id);
  }

  @Nonnull
  @Override
  public EntityResult updateEntityPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    return metaStoreManager.updateEntityPropertiesIfNotChanged(callCtx, null, entity);
  }

  @Nonnull
  @Override
  public DropEntityResult dropEntityIfExists(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    return metaStoreManager.dropEntityIfExists(
        callCtx, null, entityToDrop, cleanupProperties, cleanup);
  }
}
