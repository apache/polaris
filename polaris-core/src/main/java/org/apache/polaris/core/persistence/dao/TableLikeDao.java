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
package org.apache.polaris.core.persistence.dao;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;

public interface TableLikeDao {
  @Nonnull
  EntityResult readEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name);

  @Nonnull
  EntityResult loadEntity(@Nonnull PolarisCallContext callCtx, long entityCatalogId, long entityId);

  @Nonnull
  ListEntitiesResult listEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntitySubType entitySubType);

  @Nonnull
  EntityResult renameEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @Nonnull PolarisEntity renamedEntity);

  @Nonnull
  ResolvedEntityResult loadResolvedEntityById(
      @Nonnull PolarisCallContext callCtx, long entityCatalogId, long entityId);

  @Nonnull
  ResolvedEntityResult loadResolvedEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @Nonnull String entityName);

  @Nonnull
  ResolvedEntityResult refreshResolvedEntity(
      @Nonnull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      long entityCatalogId,
      long entityId);

  @Nonnull
  EntityResult createEntityIfNotExists(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity);

  @Nonnull
  EntityResult updateEntityPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity);

  @Nonnull
  DropEntityResult dropEntityIfExists(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup);

  /**
   * This allows to operate on multiple tables at once. Just loop through the list, calling each
   * entity update and return null if any of those fail.
   *
   * @param callCtx call context
   * @param entities the set of entities to update
   * @return list of all entities we updated or null if the client should retry because one update
   *     failed
   */
  @Nonnull
  EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<EntityWithPath> entities);
}
