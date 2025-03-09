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

import static org.apache.polaris.core.entity.PolarisEntityType.TASK;

import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.dao.TaskDao;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FdbTaskDaoImpl implements TaskDao {
  PolarisMetaStoreManagerImpl metaStoreManager = new PolarisMetaStoreManagerImpl();

  @NotNull
  @Override
  public EntitiesResult loadTasks(
      @NotNull PolarisCallContext callCtx, String executorId, int limit) {
    return metaStoreManager.loadTasks(callCtx, executorId, limit);
  }

  @NotNull
  @Override
  public EntitiesResult createEntitiesIfNotExist(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull List<? extends PolarisBaseEntity> entities) {
    return metaStoreManager.createEntitiesIfNotExist(callCtx, catalogPath, entities);
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
  public EntityResult loadEntity(
      @NotNull PolarisCallContext callCtx, long entityCatalogId, long entityId) {
    return metaStoreManager.loadEntity(callCtx, entityCatalogId, entityId, TASK);
  }
}
