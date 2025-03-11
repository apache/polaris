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
import static org.apache.polaris.core.entity.PolarisEntityType.TASK;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.dao.TaskDao;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;

public class DelegatingTaskDaoImpl implements TaskDao {
  PolarisMetaStoreManagerImpl metaStoreManager = new PolarisMetaStoreManagerImpl();

  @Override
  public @Nonnull EntityResult readEntityByName(
      @Nonnull PolarisCallContext callCtx, @Nonnull String name) {
    // Task shouldn't have a catalog path and subtype
    return metaStoreManager.readEntityByName(callCtx, null, TASK, ANY_SUBTYPE, name);
  }

  @Nonnull
  @Override
  public ListEntitiesResult listEntities(@Nonnull PolarisCallContext callCtx) {
    return metaStoreManager.listEntities(callCtx, null, TASK, ANY_SUBTYPE);
  }

  @Nonnull
  @Override
  public EntitiesResult loadTasks(
      @Nonnull PolarisCallContext callCtx, String executorId, int limit) {
    return metaStoreManager.loadTasks(callCtx, executorId, limit);
  }

  @Nonnull
  @Override
  public EntityResult createEntityIfNotExists(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    return metaStoreManager.createEntityIfNotExists(callCtx, null, entity);
  }

  @Nonnull
  @Override
  public EntitiesResult createTasksIfNotExist(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<? extends PolarisBaseEntity> entities) {
    return metaStoreManager.createEntitiesIfNotExist(callCtx, null, entities);
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

  @Nonnull
  @Override
  public EntityResult loadEntity(@Nonnull PolarisCallContext callCtx, long entityId) {
    return metaStoreManager.loadEntity(callCtx, 0L, entityId, TASK);
  }
}
