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
import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.ChangeTrackingResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;

public interface CommonDao {
  /**
   * Generate a new unique id that can be used by the Polaris client when it needs to create a new
   * entity
   *
   * @param callCtx call context
   * @return the newly created id, not expected to fail
   */
  @Nonnull
  GenerateEntityIdResult generateNewEntityId(@Nonnull PolarisCallContext callCtx);

  /**
   * Bootstrap the Polaris service, creating the root catalog, root principal, and associated
   * service admin role. Will fail if the service has already been bootstrapped.
   *
   * @param callCtx call context
   * @return the result of the bootstrap attempt
   */
  @Nonnull
  BaseResult bootstrapPolarisService(@Nonnull PolarisCallContext callCtx);

  /**
   * Purge all metadata associated with the Polaris service, resetting the metastore to the state it
   * was in prior to bootstrapping.
   *
   * <p>*************************** WARNING ************************
   *
   * <p>This will destroy whatever Polaris metadata exists in the metastore
   *
   * @param callCtx call context
   * @return always success or unexpected error
   */
  @Nonnull
  BaseResult purge(@Nonnull PolarisCallContext callCtx);

  /** For ROOT only */
  @Nonnull
  ResolvedEntityResult loadResolvedEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @Nonnull String entityName);

  @Nonnull
  ChangeTrackingResult loadEntitiesChangeTracking(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<PolarisEntityId> entityIds);

  /** For ROOT only */
  @Nonnull
  ResolvedEntityResult refreshResolvedEntity(
      @Nonnull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      long entityCatalogId,
      long entityId);

  /**
   * only for NULL_TYPE, looks like this is only used by Tests. We could remove this method if it is
   * only used by tests.
   */
  @Nonnull
  EntityResult loadEntity(@Nonnull PolarisCallContext callCtx, long entityCatalogId, long entityId);
}
