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
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisGrantRecord;
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
 * <p>This is intentionally minimal: it supports creates, CAS-updates, and grant-record
 * creates/deletes. {@link #toMutations()} flattens the grouped fields into the ordered SPI list.
 * Entity deletes are deferred to a follow-up.
 */
public record MetaStoreChangeSet(
    @NonNull List<EntityWithPath> creates,
    @NonNull List<EntityWithPath> updates,
    @NonNull List<PolarisGrantRecord> grantRecordsToCreate,
    @NonNull List<PolarisGrantRecord> grantRecordsToDelete) {

  /** A changeset with no mutations — no-op commit. */
  public static final MetaStoreChangeSet EMPTY =
      new MetaStoreChangeSet(List.of(), List.of(), List.of(), List.of());

  /** A changeset for a single entity creation. */
  public static MetaStoreChangeSet ofCreate(
      @Nullable List<PolarisEntityCore> catalogPath, @NonNull PolarisBaseEntity entity) {
    return new MetaStoreChangeSet(
        List.of(new EntityWithPath(catalogPath, entity)), List.of(), List.of(), List.of());
  }

  /** A changeset for a single entity CAS-update. */
  public static MetaStoreChangeSet ofUpdate(
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity,
      @NonNull PolarisBaseEntity originalEntity) {
    return new MetaStoreChangeSet(
        List.of(),
        List.of(new EntityWithPath(catalogPath, entity, originalEntity)),
        List.of(),
        List.of());
  }

  /** A changeset for a batch of CAS-updates. */
  public static MetaStoreChangeSet ofUpdates(@NonNull List<EntityWithPath> updates) {
    return new MetaStoreChangeSet(List.of(), updates, List.of(), List.of());
  }

  /** A changeset for a mixed batch of creates and CAS-updates. */
  public static MetaStoreChangeSet ofBatch(
      @NonNull List<EntityWithPath> creates, @NonNull List<EntityWithPath> updates) {
    return new MetaStoreChangeSet(creates, updates, List.of(), List.of());
  }

  /** Returns true if this changeset contains no mutations. */
  public boolean isEmpty() {
    return creates.isEmpty()
        && updates.isEmpty()
        && grantRecordsToCreate.isEmpty()
        && grantRecordsToDelete.isEmpty();
  }

  /**
   * Flatten this change set into the ordered SPI mutation list: creates, then updates, then grant
   * creates, then grant deletes. Callers that need CAS-prepared entity payloads should run {@code
   * prepareToPersist*} before constructing the corresponding {@link PersistenceMutation}.
   */
  public @NonNull List<PersistenceMutation> toMutations() {
    List<PersistenceMutation> mutations =
        new ArrayList<>(
            creates.size()
                + updates.size()
                + grantRecordsToCreate.size()
                + grantRecordsToDelete.size());
    for (EntityWithPath create : creates) {
      mutations.add(PersistenceMutation.createEntity(create.entity()));
    }
    for (EntityWithPath update : updates) {
      mutations.add(PersistenceMutation.updateEntity(update.entity(), update.originalEntity()));
    }
    for (PolarisGrantRecord grant : grantRecordsToCreate) {
      mutations.add(PersistenceMutation.createGrant(grant));
    }
    for (PolarisGrantRecord grant : grantRecordsToDelete) {
      mutations.add(PersistenceMutation.deleteGrant(grant));
    }
    return List.copyOf(mutations);
  }

  /** Returns a new builder pre-populated with this changeset's contents. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  /** Builder for constructing complex changesets incrementally. */
  public static class Builder {
    private final List<EntityWithPath> creates = new ArrayList<>();
    private final List<EntityWithPath> updates = new ArrayList<>();
    private final List<PolarisGrantRecord> grantRecordsToCreate = new ArrayList<>();
    private final List<PolarisGrantRecord> grantRecordsToDelete = new ArrayList<>();

    public Builder() {}

    public Builder(MetaStoreChangeSet existing) {
      this.creates.addAll(existing.creates);
      this.updates.addAll(existing.updates);
      this.grantRecordsToCreate.addAll(existing.grantRecordsToCreate);
      this.grantRecordsToDelete.addAll(existing.grantRecordsToDelete);
    }

    public Builder addCreate(
        @Nullable List<PolarisEntityCore> catalogPath, @NonNull PolarisBaseEntity entity) {
      this.creates.add(new EntityWithPath(catalogPath, entity));
      return this;
    }

    public Builder addUpdate(
        @Nullable List<PolarisEntityCore> catalogPath,
        @NonNull PolarisBaseEntity entity,
        @NonNull PolarisBaseEntity originalEntity) {
      this.updates.add(new EntityWithPath(catalogPath, entity, originalEntity));
      return this;
    }

    public Builder addGrantRecordCreate(@NonNull PolarisGrantRecord grantRecord) {
      this.grantRecordsToCreate.add(grantRecord);
      return this;
    }

    public Builder addGrantRecordDelete(@NonNull PolarisGrantRecord grantRecord) {
      this.grantRecordsToDelete.add(grantRecord);
      return this;
    }

    public MetaStoreChangeSet build() {
      return new MetaStoreChangeSet(
          List.copyOf(creates),
          List.copyOf(updates),
          List.copyOf(grantRecordsToCreate),
          List.copyOf(grantRecordsToDelete));
    }
  }
}
