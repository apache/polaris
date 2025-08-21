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

import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;

/** Shared basic PolarisMetaStoreManager logic for transactional and non-transactional impls. */
public abstract class BaseMetaStoreManager implements PolarisMetaStoreManager {

  public static PolarisStorageConfigurationInfo extractStorageConfiguration(
      @Nonnull PolarisDiagnostics diagnostics, PolarisBaseEntity reloadedEntity) {
    Map<String, String> propMap = reloadedEntity.getInternalPropertiesAsMap();
    String storageConfigInfoStr =
        propMap.get(PolarisEntityConstants.getStorageConfigInfoPropertyName());

    diagnostics.check(
        storageConfigInfoStr != null,
        "missing_storage_configuration_info",
        "catalogId={}, entityId={}",
        reloadedEntity.getCatalogId(),
        reloadedEntity.getId());
    return PolarisStorageConfigurationInfo.deserialize(storageConfigInfoStr);
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
      @Nonnull PolarisCallContext callCtx,
      @Nonnull BasePersistence ms,
      @Nonnull PolarisBaseEntity entity) {

    // validate the entity type and subtype
    callCtx.getDiagServices().checkNotNull(entity, "unexpected_null_entity");
    callCtx
        .getDiagServices()
        .checkNotNull(entity.getName(), "unexpected_null_name", "entity={}", entity);
    PolarisEntityType type = PolarisEntityType.fromCode(entity.getTypeCode());
    callCtx.getDiagServices().checkNotNull(type, "unknown_type", "entity={}", entity);
    PolarisEntitySubType subType = PolarisEntitySubType.fromCode(entity.getSubTypeCode());
    callCtx.getDiagServices().checkNotNull(subType, "unexpected_null_subType", "entity={}", entity);
    callCtx
        .getDiagServices()
        .check(
            subType.getParentType() == null || subType.getParentType() == type,
            "invalid_subtype",
            "type={} subType={}",
            type,
            subType);

    // if top-level entity, its parent should be the account
    callCtx
        .getDiagServices()
        .check(
            !type.isTopLevel() || entity.getParentId() == PolarisEntityConstants.getRootEntityId(),
            "top_level_parent_should_be_account",
            "entity={}",
            entity);

    // id should not be null
    callCtx
        .getDiagServices()
        .check(
            entity.getId() != 0 || type == PolarisEntityType.ROOT,
            "id_not_set",
            "entity={}",
            entity);

    // creation timestamp must be filled
    callCtx.getDiagServices().check(entity.getCreateTimestamp() != 0, "null_create_timestamp");

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
  protected @Nonnull PolarisBaseEntity prepareToPersistEntityAfterChange(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull BasePersistence ms,
      @Nonnull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nonnull PolarisBaseEntity originalEntity) {

    // validate the entity type and subtype
    callCtx.getDiagServices().checkNotNull(entity, "unexpected_null_entity");
    callCtx
        .getDiagServices()
        .checkNotNull(entity.getName(), "unexpected_null_name", "entity={}", entity);
    PolarisEntityType type = entity.getType();
    callCtx.getDiagServices().checkNotNull(type, "unexpected_null_type", "entity={}", entity);
    PolarisEntitySubType subType = entity.getSubType();
    callCtx.getDiagServices().checkNotNull(subType, "unexpected_null_subType", "entity={}", entity);
    callCtx
        .getDiagServices()
        .check(
            subType.getParentType() == null || subType.getParentType() == type,
            "invalid_subtype",
            "type={} subType={} entity={}",
            type,
            subType,
            entity);

    // entity should not have been dropped
    callCtx
        .getDiagServices()
        .check(entity.getDropTimestamp() == 0, "entity_dropped", "entity={}", entity);

    // creation timestamp must be filled
    long createTimestamp = entity.getCreateTimestamp();
    callCtx
        .getDiagServices()
        .check(createTimestamp != 0, "null_create_timestamp", "entity={}", entity);

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
  public @Nonnull GenerateEntityIdResult generateNewEntityId(@Nonnull PolarisCallContext callCtx) {
    // get meta store we should be using
    BasePersistence ms = callCtx.getMetaStore();

    return new GenerateEntityIdResult(ms.generateNewId(callCtx));
  }
}
