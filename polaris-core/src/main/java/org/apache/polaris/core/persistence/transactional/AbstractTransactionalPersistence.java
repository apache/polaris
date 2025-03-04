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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntityCore;

/**
 * Extends BasePersistence to express a more "transaction-oriented" control flow for backing stores
 * which can support a runInTransaction semantic, while providing default implementations of some of
 * the BasePersistence methods in terms of lower-level methods that subclasses must implement.
 */
public abstract class AbstractTransactionalPersistence implements TransactionalPersistence {

  /** {@inheritDoc} */
  @Override
  public void writeEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisBaseEntity> entities,
      @Nullable List<PolarisBaseEntity> originalEntities) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /** {@inheritDoc} */
  @Override
  public PolarisBaseEntity lookupEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name) {
    // TODO: Consistently pull down the runInTransaction logic without running into conflicting
    // nested transactions into here so that instead of having the caller be responsible for
    // initiating the runInReadTransaction, we make this method call inherently safe to do
    // the two-phase lookup.
    PolarisEntitiesActiveKey entityActiveKey =
        new PolarisEntitiesActiveKey(catalogId, parentId, typeCode, name);

    // ensure that the entity exists
    EntityNameLookupRecord entityActiveRecord = lookupEntityActive(callCtx, entityActiveKey);

    // if not found, return null
    if (entityActiveRecord == null) {
      return null;
    }

    // lookup the entity, should be there
    PolarisBaseEntity entity =
        lookupEntity(callCtx, entityActiveRecord.getCatalogId(), entityActiveRecord.getId());
    callCtx
        .getDiagServices()
        .checkNotNull(
            entity, "unexpected_not_found_entity", "entityActiveRecord={}", entityActiveRecord);

    // return it now
    return entity;
  }

  /** {@inheritDoc} */
  @Override
  public EntityNameLookupRecord lookupEntityIdAndSubTypeByName(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name) {
    PolarisEntitiesActiveKey entityActiveKey =
        new PolarisEntitiesActiveKey(catalogId, parentId, typeCode, name);
    return lookupEntityActive(callCtx, entityActiveKey);
  }

  /**
   * Lookup an entity by entityActiveKey
   *
   * @param callCtx call context
   * @param entityActiveKey key by name
   * @return null if the specified entity does not exist or has been dropped.
   */
  @Nullable
  protected abstract EntityNameLookupRecord lookupEntityActive(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntitiesActiveKey entityActiveKey);

  /** {@inheritDoc} */
  @Override
  public void writeEntityInOuterTransaction(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity) {
    // TODO: Pull down relevant compare-and-swap semantics from PolarisMetaStoreManagerImpl
    // into this layer.
    writeToEntities(callCtx, entity);
    writeToEntitiesChangeTracking(callCtx, entity);

    if (nameOrParentChanged) {
      if (originalEntity != null) {
        // In our case, rename isn't automatically handled when the main "entities" slice
        // is updated; instead we must explicitly remove from the old entitiesActive
        // key as well.
        deleteFromEntitiesActive(callCtx, originalEntity);
      }
      writeToEntitiesActive(callCtx, entity);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void writeEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity) {
    runActionInTransaction(
        callCtx,
        () ->
            this.writeEntityInOuterTransaction(
                callCtx, entity, nameOrParentChanged, originalEntity));
  }

  /**
   * Write the base entity to the entities table. If there is a conflict (existing record with the
   * same id), all attributes of the new record will replace the existing one.
   *
   * @param callCtx call context
   * @param entity entity record to write, potentially replacing an existing entity record with the
   *     same key
   */
  protected abstract void writeToEntities(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity);

  /**
   * Write the base entity to the entities_active table. If there is a conflict (existing record
   * with the same PK), all attributes of the new record will replace the existing one.
   *
   * @param callCtx call context
   * @param entity entity record to write, potentially replacing an existing entity record with the
   *     same key
   */
  protected abstract void writeToEntitiesActive(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity);

  /**
   * Write the base entity to the entities change tracking table. If there is a conflict (existing
   * record with the same id), all attributes of the new record will replace the existing one.
   *
   * @param callCtx call context
   * @param entity entity record to write, potentially replacing an existing entity record with the
   *     same key
   */
  protected abstract void writeToEntitiesChangeTracking(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity);

  /** {@inheritDoc} */
  @Override
  public void deleteEntity(@Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    deleteFromEntitiesActive(callCtx, entity);
    deleteFromEntities(callCtx, entity);
    deleteFromEntitiesChangeTracking(callCtx, entity);
  }

  /**
   * Delete the base entity from the entities table.
   *
   * @param callCtx call context
   * @param entity entity record to delete
   */
  protected abstract void deleteFromEntities(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore entity);

  /**
   * Delete the base entity from the entities_active table.
   *
   * @param callCtx call context
   * @param entity entity record to delete
   */
  protected abstract void deleteFromEntitiesActive(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore entity);

  /**
   * Delete the base entity from the entities change tracking table
   *
   * @param callCtx call context
   * @param entity entity record to delete
   */
  protected abstract void deleteFromEntitiesChangeTracking(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore entity);
}
