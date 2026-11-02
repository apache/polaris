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
package org.apache.polaris.core.persistence.dao.entity;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Entity writes that are applied atomically, and only if all conditions hold. See {@code
 * PolarisMetaStoreManager#writeEntitiesIfConditionsHold}.
 *
 * <p>Each entity may be the subject of at most one condition or write, so a conditional write can
 * neither check and write the same entity (a write already compares the entity's versions) nor
 * write it twice.
 */
public record ConditionalEntityWrite(
    @NonNull List<Condition> conditions, @NonNull List<Write> writes) {

  /**
   * A condition that must hold for the writes to be applied. Conditions are evaluated atomically
   * with the writes. Backends must handle every kind and must never ignore one.
   */
  public sealed interface Condition {

    /**
     * The entity still exists, with the given entity version. The entity is only checked: it is not
     * written, and its version, last-update time and properties stay as they are.
     *
     * <p>Only {@link PolarisEntityCore#getEntityVersion()} is compared. A change of the grant
     * records version does not fail the condition, because it does not change the entity itself.
     *
     * @param catalogPath path to the entity's parent, null if the entity is top-level; carried but
     *     not validated
     * @param entity identity and expected version of the entity
     */
    record Unchanged(
        @Nullable List<PolarisEntityCore> catalogPath, @NonNull PolarisEntityCore entity)
        implements Condition {}
  }

  /** A write, applied only if all conditions hold. Backends must handle every kind. */
  public sealed interface Write {

    /**
     * Updates an entity, if it is still at the versions of {@code entity}: the entity version and
     * the grant records version. This has the same semantics as {@code
     * PolarisMetaStoreManager#updateEntityPropertiesIfNotChanged}.
     *
     * @param catalogPath path to the entity's parent, null if the entity is top-level; carried but
     *     not validated
     * @param entity the entity to write, at the versions it was read
     */
    record Update(@Nullable List<PolarisEntityCore> catalogPath, @NonNull PolarisBaseEntity entity)
        implements Write {}
  }

  public ConditionalEntityWrite {
    conditions = List.copyOf(conditions);
    writes = List.copyOf(writes);

    Set<EntityKey> entities = new HashSet<>();
    for (Write write : writes) {
      if (!(write instanceof Write.Update update)) {
        throw new IllegalArgumentException("Unsupported write " + write);
      }
      requireOnce(entities, update.entity());
    }
    for (Condition condition : conditions) {
      if (!(condition instanceof Condition.Unchanged unchanged)) {
        throw new IllegalArgumentException("Unsupported condition " + condition);
      }
      requireOnce(entities, unchanged.entity());
    }
  }

  private static void requireOnce(Set<EntityKey> entities, PolarisEntityCore entity) {
    if (!entities.add(EntityKey.of(entity))) {
      throw new IllegalArgumentException(
          "Entity " + entity.getId() + " is the subject of more than one condition or write");
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private record EntityKey(long catalogId, long id) {
    static EntityKey of(PolarisEntityCore entity) {
      return new EntityKey(entity.getCatalogId(), entity.getId());
    }
  }

  public static final class Builder {
    private final List<Condition> conditions = new ArrayList<>();
    private final List<Write> writes = new ArrayList<>();

    /** Requires {@code entity} to be unchanged, without writing it. */
    public Builder unchanged(
        @Nullable List<PolarisEntityCore> catalogPath, @NonNull PolarisEntityCore entity) {
      conditions.add(new Condition.Unchanged(catalogPath, entity));
      return this;
    }

    /** Updates {@code entity}, if it is still at the versions it was read. */
    public Builder update(
        @Nullable List<PolarisEntityCore> catalogPath, @NonNull PolarisBaseEntity entity) {
      writes.add(new Write.Update(catalogPath, entity));
      return this;
    }

    public ConditionalEntityWrite build() {
      return new ConditionalEntityWrite(conditions, writes);
    }
  }
}
