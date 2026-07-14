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

import java.util.List;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Represents a set of metastore mutations to be committed atomically by {@link
 * PolarisMetaStoreManager#commitTransactionBatch}. Simpler single-entity operations ({@link
 * PolarisMetaStoreManager#createEntityIfNotExists}, {@link
 * PolarisMetaStoreManager#updateEntitiesPropertiesIfNotChanged}) can be expressed as a changeset
 * with a single entry, allowing them to ride on top of the same atomic commit path.
 *
 * <p>This is intentionally minimal: only creates and CAS-updates are supported now. Drops and
 * renames are explicit empty lists, reserved for future support.
 */
public record MetaStoreChangeSet(
    @NonNull List<EntityWithPath> creates, @NonNull List<EntityWithPath> updates) {

  /** A changeset with no mutations — no-op commit. */
  public static final MetaStoreChangeSet EMPTY = new MetaStoreChangeSet(List.of(), List.of());

  /** A changeset for a single entity creation. */
  public static MetaStoreChangeSet ofCreate(
      @Nullable List<PolarisEntityCore> catalogPath, @NonNull PolarisBaseEntity entity) {
    return new MetaStoreChangeSet(List.of(new EntityWithPath(catalogPath, entity)), List.of());
  }

  /** A changeset for a single entity CAS-update. */
  public static MetaStoreChangeSet ofUpdate(
      @Nullable List<PolarisEntityCore> catalogPath, @NonNull PolarisBaseEntity entity) {
    return new MetaStoreChangeSet(List.of(), List.of(new EntityWithPath(catalogPath, entity)));
  }

  /** A changeset for a batch of CAS-updates. */
  public static MetaStoreChangeSet ofUpdates(@NonNull List<EntityWithPath> updates) {
    return new MetaStoreChangeSet(List.of(), updates);
  }

  /** A changeset for a mixed batch of creates and CAS-updates. */
  public static MetaStoreChangeSet ofBatch(
      @NonNull List<EntityWithPath> creates, @NonNull List<EntityWithPath> updates) {
    return new MetaStoreChangeSet(creates, updates);
  }
}
