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

import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * One item in a {@link BasePersistence#commitChangeSet} batch. The batch contract is independent of
 * any one record family: kind, operation, target, payload, and preconditions travel with the item
 * so a later family (policy mappings, secrets) can be added without widening {@link
 * BasePersistence}.
 *
 * <p>Phase 1 carries two kinds:
 *
 * <ul>
 *   <li>{@link Entity} — target is the entity identity, payload is {@code entity}, and
 *       preconditions are {@code originalEntity} (required for {@link Operation#UPDATE}).
 *   <li>{@link Grant} — target and payload are the grant-record primary key; there are no
 *       preconditions. Duplicate creates are a no-op.
 * </ul>
 */
public sealed interface PersistenceMutation
    permits PersistenceMutation.Entity, PersistenceMutation.Grant {

  /** Durable record family this mutation applies to. */
  enum Kind {
    ENTITY,
    GRANT_RECORD
  }

  /** Operation to apply to the target record. */
  enum Operation {
    CREATE,
    UPDATE,
    DELETE
  }

  @NonNull Kind kind();

  @NonNull Operation operation();

  /** Create a new entity. {@code originalEntity} is null. */
  static @NonNull PersistenceMutation createEntity(@NonNull PolarisBaseEntity entity) {
    return new Entity(Operation.CREATE, entity, null);
  }

  /** CAS-update an entity. {@code originalEntity} is the compare-and-swap baseline. */
  static @NonNull PersistenceMutation updateEntity(
      @NonNull PolarisBaseEntity entity, @NonNull PolarisBaseEntity originalEntity) {
    return new Entity(Operation.UPDATE, entity, originalEntity);
  }

  /** Delete an entity. */
  static @NonNull PersistenceMutation deleteEntity(@NonNull PolarisBaseEntity entity) {
    return new Entity(Operation.DELETE, entity, null);
  }

  /** Create a grant record. A duplicate primary key is a no-op. */
  static @NonNull PersistenceMutation createGrant(@NonNull PolarisGrantRecord grantRecord) {
    return new Grant(Operation.CREATE, grantRecord);
  }

  /** Delete a grant record. */
  static @NonNull PersistenceMutation deleteGrant(@NonNull PolarisGrantRecord grantRecord) {
    return new Grant(Operation.DELETE, grantRecord);
  }

  /**
   * Entity create, CAS-update, or delete.
   *
   * @param operation CREATE, UPDATE, or DELETE
   * @param entity payload to persist, or the entity to delete
   * @param originalEntity CAS baseline for UPDATE; null for CREATE and DELETE
   */
  record Entity(
      @NonNull Operation operation,
      @NonNull PolarisBaseEntity entity,
      @Nullable PolarisBaseEntity originalEntity)
      implements PersistenceMutation {

    public Entity {
      if (operation == Operation.UPDATE && originalEntity == null) {
        throw new IllegalArgumentException(
            "UPDATE mutation missing originalEntity for entity id " + entity.getId());
      }
    }

    @Override
    public @NonNull Kind kind() {
      return Kind.ENTITY;
    }
  }

  /**
   * Grant-record create or delete.
   *
   * @param operation CREATE or DELETE
   * @param grantRecord the grant record to write or delete
   */
  record Grant(@NonNull Operation operation, @NonNull PolarisGrantRecord grantRecord)
      implements PersistenceMutation {

    public Grant {
      if (operation == Operation.UPDATE) {
        throw new IllegalArgumentException("GRANT_RECORD does not support UPDATE");
      }
    }

    @Override
    public @NonNull Kind kind() {
      return Kind.GRANT_RECORD;
    }
  }
}
