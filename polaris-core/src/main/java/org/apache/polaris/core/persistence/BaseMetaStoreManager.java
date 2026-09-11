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
package org.apache.polaris.core.persistence;

import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.jspecify.annotations.NonNull;

/** Shared basic PolarisMetaStoreManager logic for transactional and non-transactional impls. */
public abstract class BaseMetaStoreManager implements PolarisMetaStoreManager {

  private final PolarisDiagnostics diagnostics;

  protected BaseMetaStoreManager(PolarisDiagnostics diagnostics) {
    this.diagnostics = diagnostics;
  }

  protected PolarisDiagnostics getDiagnostics() {
    return diagnostics;
  }

  /**
   * Performs basic validation of expected invariants on a new entity, then returns the entity with
   * fields filled out for which the persistence layer is responsible.
   *
   * @param callCtx call context
   * @param ms meta store in read/write mode
   * @param entity entity we need a new persisted record for
   */
  protected PolarisBaseEntity prepareToPersistNewEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull BasePersistence ms,
      @NonNull PolarisBaseEntity entity) {

    // validate the entity type and subtype
    getDiagnostics().checkNotNull(entity, "unexpected_null_entity");
    getDiagnostics().checkNotNull(entity.getName(), "unexpected_null_name", "entity={}", entity);
    PolarisEntityType type = PolarisEntityType.fromCode(entity.getTypeCode());
    getDiagnostics().checkNotNull(type, "unknown_type", "entity={}", entity);
    PolarisEntitySubType subType = PolarisEntitySubType.fromCode(entity.getSubTypeCode());
    getDiagnostics().checkNotNull(subType, "unexpected_null_subType", "entity={}", entity);
    getDiagnostics()
        .check(
            subType.getParentType() == null || subType.getParentType() == type,
            "invalid_subtype",
            "type={} subType={}",
            type,
            subType);

    // if top-level entity, its parent should be the account
    getDiagnostics()
        .check(
            !type.isTopLevel() || entity.getParentId() == PolarisEntityConstants.getRootEntityId(),
            "top_level_parent_should_be_account",
            "entity={}",
            entity);

    // id should not be null
    getDiagnostics()
        .check(
            entity.getId() != 0 || type == PolarisEntityType.ROOT,
            "id_not_set",
            "entity={}",
            entity);

    // creation timestamp must be filled
    getDiagnostics().check(entity.getCreateTimestamp() != 0, "null_create_timestamp");

    PolarisBaseEntity.Builder entityBuilder = new PolarisBaseEntity.Builder(entity);
    entityBuilder.lastUpdateTimestamp(entity.getCreateTimestamp());
    entityBuilder.dropTimestamp(0);
    entityBuilder.purgeTimestamp(0);
    entityBuilder.toPurgeTimestamp(0);
    return entityBuilder.build();
  }

  /**
   * Performs basic validation of expected invariants on a changed entity, then returns the entity
   * with fields filled out for which the persistence layer is responsible.
   *
   * @param callCtx call context
   * @param ms meta store
   * @param entity the entity which has been changed
   * @param nameOrParentChanged indicates if parent or name changed
   * @param originalEntity the original state of the entity before changes
   * @return the entity with its version and lastUpdateTimestamp updated
   */
  protected @NonNull PolarisBaseEntity prepareToPersistEntityAfterChange(
      @NonNull PolarisCallContext callCtx,
      @NonNull BasePersistence ms,
      @NonNull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @NonNull PolarisBaseEntity originalEntity) {

    // validate the entity type and subtype
    getDiagnostics().checkNotNull(entity, "unexpected_null_entity");
    getDiagnostics().checkNotNull(entity.getName(), "unexpected_null_name", "entity={}", entity);
    PolarisEntityType type = entity.getType();
    getDiagnostics().checkNotNull(type, "unexpected_null_type", "entity={}", entity);
    PolarisEntitySubType subType = entity.getSubType();
    getDiagnostics().checkNotNull(subType, "unexpected_null_subType", "entity={}", entity);
    getDiagnostics()
        .check(
            subType.getParentType() == null || subType.getParentType() == type,
            "invalid_subtype",
            "type={} subType={} entity={}",
            type,
            subType,
            entity);

    // entity should not have been dropped
    getDiagnostics().check(entity.getDropTimestamp() == 0, "entity_dropped", "entity={}", entity);

    // creation timestamp must be filled
    long createTimestamp = entity.getCreateTimestamp();
    getDiagnostics().check(createTimestamp != 0, "null_create_timestamp", "entity={}", entity);

    // ensure time is not moving backward...
    long now = System.currentTimeMillis();
    if (now < entity.getCreateTimestamp()) {
      now = entity.getCreateTimestamp() + 1;
    }

    PolarisBaseEntity.Builder entityBuilder = new PolarisBaseEntity.Builder(entity);
    entityBuilder.lastUpdateTimestamp(now).entityVersion(entity.getEntityVersion() + 1);
    return entityBuilder.build();
  }

  /** {@inheritDoc} */
  @Override
  public @NonNull GenerateEntityIdResult generateNewEntityId(@NonNull PolarisCallContext callCtx) {
    // get meta store we should be using
    BasePersistence ms = callCtx.getMetaStore();

    return new GenerateEntityIdResult(ms.generateNewId(callCtx));
  }

  /**
   * Convert a manager-level change set into SPI mutations, filling persistence-owned fields on
   * entity creates and updates.
   */
  protected @NonNull List<PersistenceMutation> prepareChangeSetMutations(
      @NonNull PolarisCallContext callCtx,
      @NonNull BasePersistence ms,
      @NonNull MetaStoreChangeSet changeSet) {
    List<PersistenceMutation> mutations =
        new ArrayList<>(
            changeSet.creates().size()
                + changeSet.updates().size()
                + changeSet.grantRecordsToCreate().size()
                + changeSet.grantRecordsToDelete().size());
    for (EntityWithPath create : changeSet.creates()) {
      mutations.add(
          PersistenceMutation.createEntity(
              prepareToPersistNewEntity(
                  callCtx, ms, new PolarisBaseEntity.Builder(create.entity()).build())));
    }
    for (EntityWithPath update : changeSet.updates()) {
      mutations.add(
          PersistenceMutation.updateEntity(
              prepareToPersistEntityAfterChange(
                  callCtx,
                  ms,
                  new PolarisBaseEntity.Builder(update.entity()).build(),
                  false,
                  update.entity()),
              update.originalEntity()));
    }
    for (PolarisGrantRecord grant : changeSet.grantRecordsToCreate()) {
      mutations.add(PersistenceMutation.createGrant(grant));
    }
    for (PolarisGrantRecord grant : changeSet.grantRecordsToDelete()) {
      mutations.add(PersistenceMutation.deleteGrant(grant));
    }
    return mutations;
  }

  /**
   * Map backend conflict exceptions onto the existing {@link EntitiesResult} statuses used by
   * {@link #createEntityIfNotExists} and {@link
   * PolarisMetaStoreManager#updateEntitiesPropertiesIfNotChanged}.
   */
  protected @NonNull EntitiesResult mapChangeSetConflicts(@NonNull RuntimeException e) {
    if (e instanceof EntityAlreadyExistsException eaee) {
      PolarisBaseEntity existing = eaee.getExistingEntity();
      return new EntitiesResult(
          BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS,
          existing == null
              ? e.getMessage()
              : String.format(
                  "Existing entity id: '%s', type %s subtype %s",
                  existing.getId(), existing.getTypeCode(), existing.getSubTypeCode()));
    }
    if (e instanceof RetryOnConcurrencyException) {
      return new EntitiesResult(
          BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, e.getMessage());
    }
    throw e;
  }

  protected @NonNull EntitiesResult entitiesResultFromMutations(
      @NonNull List<PersistenceMutation> mutations) {
    List<PolarisBaseEntity> entities = new ArrayList<>();
    for (PersistenceMutation mutation : mutations) {
      if (mutation instanceof PersistenceMutation.Entity entityMutation) {
        entities.add(entityMutation.entity());
      }
    }
    return new EntitiesResult(Page.fromItems(entities));
  }
}
