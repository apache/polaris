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
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.jetbrains.annotations.NotNull;

public interface PrincipalDao {
  @NotNull
  EntityResult readEntityByName(@NotNull PolarisCallContext callCtx, @NotNull String name);

  @Nonnull
  EntityResult loadEntity(@Nonnull PolarisCallContext callCtx, long id);

  @Nonnull
  ListEntitiesResult listEntities(@Nonnull PolarisCallContext callCtx);

  CreatePrincipalResult createPrincipal(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity principal);

  @Nonnull
  ResolvedEntityResult loadResolvedEntityById(@Nonnull PolarisCallContext callCtx, long id);

  @Nonnull
  ResolvedEntityResult loadResolvedEntityByName(
      @Nonnull PolarisCallContext callCtx, long parentId, @Nonnull String name);

  @Nonnull
  ResolvedEntityResult refreshResolvedEntity(
      @Nonnull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      long id);

  @Nonnull
  EntityResult updateEntityPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity);

  @Nonnull
  DropEntityResult dropEntityIfExists(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup);
}
