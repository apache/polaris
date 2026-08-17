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
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Represents a single entity mutation (create, update, or delete) as part of an atomic {@link
 * BasePersistence#commitChangeSet} operation.
 *
 * @param entity the entity to write or delete
 * @param originalEntity the original state for CAS comparison on updates, or null for creates and
 *     deletes
 * @param type the type of mutation
 */
public record EntityMutation(
    @NonNull PolarisBaseEntity entity,
    @Nullable PolarisBaseEntity originalEntity,
    @NonNull MutationType type) {

  public enum MutationType {
    CREATE,
    UPDATE,
    DELETE
  }

  /** Create a new entity mutation (originalEntity is null). */
  public static EntityMutation create(@NonNull PolarisBaseEntity entity) {
    return new EntityMutation(entity, null, MutationType.CREATE);
  }

  /** Create an update mutation with CAS baseline. */
  public static EntityMutation update(
      @NonNull PolarisBaseEntity entity, @NonNull PolarisBaseEntity originalEntity) {
    return new EntityMutation(entity, originalEntity, MutationType.UPDATE);
  }

  /** Create a delete mutation. */
  public static EntityMutation delete(@NonNull PolarisBaseEntity entity) {
    return new EntityMutation(entity, null, MutationType.DELETE);
  }
}
