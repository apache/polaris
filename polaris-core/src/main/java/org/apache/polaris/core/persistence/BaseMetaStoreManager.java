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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.exceptions.PolarisServiceUnavailableException;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.tag.PolarisTagAssignmentManager.TargetLevel;
import org.apache.polaris.core.tag.TagAssignmentRecord;
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

  // --------------------------------------------------------------------------------------------
  // Tag read coherence: the comparison half of "validate and retry"
  //
  // A tag read that takes its assignment rows, its tag definitions and its target chain from
  // separate statements can return a combination that never existed. Replace an assignment between
  // the assignment read and the definition read, rename its definition in the same window, and the
  // response pairs the old row with the new name, a pair no client could have observed. Separate
  // statements alone therefore establish nothing.
  //
  // Callers perform the reads with their own accessors -- transactional or not -- and ask the
  // helpers below whether what they read describes one state. entity_version is the instrument, and
  // it is monotonic: every entity write bumps it, so equal versions at two instants prove no write
  // in between rather than merely equal values at the ends. Only entity_version is compared, never
  // grantRecordsVersion: a grant change bumps that counter alone and cannot alter a name, a kind or
  // an assignment row.
  // --------------------------------------------------------------------------------------------

  /** Indexes the result of a bulk version lookup by the id each version was requested for. */
  protected static Map<PolarisEntityId, PolarisChangeTrackingVersions> indexVersionsById(
      @NonNull List<PolarisEntityId> ids, @NonNull List<PolarisChangeTrackingVersions> versions) {
    Map<PolarisEntityId, PolarisChangeTrackingVersions> byId = new HashMap<>();
    for (int i = 0; i < ids.size() && i < versions.size(); i++) {
      PolarisChangeTrackingVersions version = versions.get(i);
      if (version != null) {
        byId.put(ids.get(i), version);
      }
    }
    return byId;
  }

  /**
   * The version each requested level carried when its caller resolved it, keyed the way a bulk
   * entity lookup keys it. The caller hands the read its already-resolved level entities, so this
   * costs no read of its own.
   */
  protected static Map<PolarisEntityId, Integer> tagLevelResolvedVersions(
      @NonNull List<TargetLevel> levels) {
    Map<PolarisEntityId, Integer> resolved = new LinkedHashMap<>();
    for (TargetLevel level : levels) {
      resolved.putIfAbsent(
          new PolarisEntityId(level.entity().getCatalogId(), level.entity().getId()),
          level.entity().getEntityVersion());
    }
    return resolved;
  }

  /**
   * The levels whose entity moved since the caller resolved it: a different entity_version, or gone
   * altogether. An empty result is the fast path, and it proves the strong statement: those
   * entities did not change at all between resolution and the version re-read, so the names,
   * parentage and kinds this read is about to return still belong to the state it read its rows
   * from.
   */
  protected static List<PolarisEntityId> tagLevelsThatMoved(
      @NonNull Map<PolarisEntityId, Integer> resolvedVersions,
      @NonNull Map<PolarisEntityId, PolarisChangeTrackingVersions> versionsNow) {
    List<PolarisEntityId> moved = new ArrayList<>();
    resolvedVersions.forEach(
        (id, resolvedVersion) -> {
          PolarisChangeTrackingVersions now = versionsNow.get(id);
          if (now == null || now.entityVersion() != resolvedVersion) {
            moved.add(id);
          }
        });
    return moved;
  }

  /**
   * Whether a level entity that moved still presents the same identity to this read. A tag read
   * depends on exactly these properties of a level: which entity it is, which parent it hangs from,
   * what kind it is (the source-kind and queried-kind filters) and the name printed in the
   * response. A write that changed anything else, an Iceberg table commit rewriting a metadata
   * location for instance, bumps entity_version without making the response describe a state that
   * never existed, and must not fail the read.
   *
   * <p>This is the weaker of the two checks, and deliberately so: unlike the version comparison it
   * cannot rule out a change and a change back inside the read's own window.
   */
  protected static boolean tagLevelIdentityUnchanged(
      @NonNull PolarisEntityCore resolved, PolarisBaseEntity current) {
    return current != null
        && current.getDropTimestamp() == 0
        && current.getId() == resolved.getId()
        && current.getCatalogId() == resolved.getCatalogId()
        && current.getParentId() == resolved.getParentId()
        && current.getTypeCode() == resolved.getTypeCode()
        && Objects.equals(current.getName(), resolved.getName());
  }

  /**
   * Whether a level that moved still presents everything this read depends on. Identity is the
   * whole question for a whole-object level, and {@link #tagLevelIdentityUnchanged} answers it. A
   * field level asks for one thing more: the caller resolved the requested field against a schema
   * before this read began, and that schema came from an Iceberg metadata pointer. The pointer is
   * therefore one of the facts the response is built from, not an unrelated detail of the entity.
   *
   * <p>That is why the identity check deliberately ignores a metadata-location change and this one
   * does not. For a whole-object level a table commit changes nothing the response says, so failing
   * would be wrong. For a field level the same commit can drop the very column being reported, and
   * then a parent assignment can change: accepting it would pair a column with a value that never
   * applied to it.
   *
   * <p>A level with no recorded pointer falls back to the identity answer, so a caller that
   * resolved a field without one is no worse off than before.
   */
  protected static boolean tagLevelUnchanged(
      @NonNull TargetLevel level, PolarisBaseEntity current) {
    if (!tagLevelIdentityUnchanged(level.entity(), current)) {
      return false;
    }
    if (level.fieldId() == 0 || level.metadataLocation() == null) {
      return true;
    }
    return Objects.equals(
        level.metadataLocation(), IcebergTableLikeEntity.of(current).getMetadataLocation());
  }

  /**
   * Whether every tag definition this read loaded is still at the version it was loaded at. A
   * definition rewritten or deleted in the window leaves the pairing of rows and definitions
   * unverifiable, so the read must not return it. A definition that was already unresolved when it
   * was loaded stays out of this check: the caller hides such a row either way, which is the orphan
   * rule, not a coherence failure.
   */
  protected static boolean tagDefinitionsUnchanged(
      @NonNull List<PolarisBaseEntity> definitions,
      @NonNull Map<PolarisEntityId, PolarisChangeTrackingVersions> versionsNow) {
    for (PolarisBaseEntity definition : definitions) {
      if (definition == null) {
        continue;
      }
      PolarisChangeTrackingVersions now =
          versionsNow.get(new PolarisEntityId(definition.getCatalogId(), definition.getId()));
      if (now == null || now.entityVersion() != definition.getEntityVersion()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Whether two reads of the same assignment keys returned the same rows. Equality here is what
   * lets the definitions loaded for the first read stand for the rows returned by the second: the
   * two reads reference the same set of definition ids, so no returned row is missing its
   * definition and none was fetched for a row that is no longer there. Rows are unique on their
   * whole key, so set equality plus a size check is exact.
   */
  protected static boolean tagAssignmentRowsUnchanged(
      @NonNull List<TagAssignmentRecord> first, @NonNull List<TagAssignmentRecord> second) {
    return first.size() == second.size() && new HashSet<>(first).containsAll(second);
  }

  /**
   * The failure a tag read owes its caller when it cannot establish that its result describes one
   * state. Transient by nature: the writes that caused it have settled by the time the client asks
   * again, which is what 503 with a Retry-After says. Returning a possibly-incoherent result
   * instead is not an option the read guarantees leave open.
   */
  protected static PolarisServiceUnavailableException concurrentTagReadModification(String what) {
    return new PolarisServiceUnavailableException(
        1, "Concurrent modification prevented a coherent tag read (%s); retry the request", what);
  }
}
