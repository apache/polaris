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
package org.apache.polaris.core.tag;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

public interface TransactionalTagAssignmentPersistence {
  /**
   * Persists the given record without validating it. Unlike {@link
   * TagAssignmentPersistence#writeToTagAssignmentRecords}, this method does not check the record's
   * value against the tag definition's allowed values: the caller must have already performed that
   * check, inside the same transaction, before invoking this method.
   */
  default void writeToTagAssignmentRecordsInCurrentTxn(
      @NonNull PolarisCallContext callCtx, @NonNull TagAssignmentRecord record) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }

  /** See {@link TagAssignmentPersistence#deleteFromTagAssignmentRecords} */
  default boolean deleteFromTagAssignmentRecordsInCurrentTxn(
      @NonNull PolarisCallContext callCtx, @NonNull TagAssignmentRecord record) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }

  /** See {@link TagAssignmentPersistence#deleteAllEntityTagAssignmentRecords} */
  default void deleteAllEntityTagAssignmentRecordsInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity entity,
      @NonNull List<TagAssignmentRecord> assignmentsOnTag,
      @NonNull List<TagAssignmentRecord> assignmentsOnTarget) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }

  /** See {@link TagAssignmentPersistence#lookupTagAssignmentRecord} */
  @Nullable
  default TagAssignmentRecord lookupTagAssignmentRecordInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int fieldId,
      long tagCatalogId,
      long tagId) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }

  /** See {@link TagAssignmentPersistence#loadTagAssignmentsOnTargetFields} */
  @NonNull
  default List<TagAssignmentRecord> loadTagAssignmentsOnTargetFieldsInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<TargetField> targetFields,
      int candidateBudget) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }

  /** See {@link TagAssignmentPersistence#loadAllTagAssignmentsOnTargetEntity} */
  @NonNull
  default List<TagAssignmentRecord> loadAllTagAssignmentsOnTargetEntityInCurrentTxn(
      @NonNull PolarisCallContext callCtx, long targetCatalogId, long targetId) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }

  /**
   * Removes every assignment row of the given tag definition inside the current transaction. One
   * shared implementation of the load-then-delete pair, used both by {@link
   * TagAssignmentPersistence#deleteTagAndAllAssignmentRecords} implementations and by manager-level
   * tag drops that already run inside their own transaction.
   */
  default void deleteAllTagAssignmentsOnTagInCurrentTxn(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisBaseEntity tagEntity) {
    List<TagAssignmentRecord> assignmentsOnTag =
        loadAllTargetsOnTagInCurrentTxn(
            callCtx,
            tagEntity.getCatalogId(),
            tagEntity.getId(),
            null,
            PageToken.readEverything(),
            CandidateBudget.unbounded());
    deleteAllEntityTagAssignmentRecordsInCurrentTxn(
        callCtx, tagEntity, assignmentsOnTag, List.of());
  }

  /**
   * Same as {@link #deleteAllTagAssignmentsOnTagInCurrentTxn}, but only while every row the
   * definition still holds is one the caller already classified; see {@link
   * TagAssignmentPersistence#deleteTagAndClassifiedAssignmentRecords} for what that means and why
   * the comparison is by identity rather than by record.
   *
   * <p>The rows are re-read here, inside the transaction, rather than taken from the caller: the
   * re-read is what makes the check current, and the delete below consumes the row list to maintain
   * its own secondary index, so it must be given the rows that are actually there.
   *
   * @return true when the rows were deleted; false when an unclassified row was present, in which
   *     case nothing was changed
   */
  default boolean deleteClassifiedTagAssignmentsOnTagInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity tagEntity,
      @NonNull Set<ClassifiedAssignment> classifiedAssignments) {
    Map<TagAssignmentIdentity, OptionalLong> judgedFrom = new HashMap<>();
    for (ClassifiedAssignment classified : classifiedAssignments) {
      judgedFrom.put(classified.identity(), classified.targetEntityVersion());
    }
    List<TagAssignmentRecord> assignmentsOnTag =
        loadAllTargetsOnTagInCurrentTxn(
            callCtx,
            tagEntity.getCatalogId(),
            tagEntity.getId(),
            null,
            PageToken.readEverything(),
            CandidateBudget.unbounded());
    for (TagAssignmentRecord record : assignmentsOnTag) {
      if (!judgedFrom.containsKey(TagAssignmentIdentity.of(record))) {
        return false;
      }
    }
    // The target check. This transaction is the store's exclusive writer, so reading each target
    // here and comparing is equivalent to taking a lock on it: no target change can commit between
    // this read and this transaction's own commit.
    Map<PolarisEntityId, OptionalLong> judgedTargets = new LinkedHashMap<>();
    for (Map.Entry<TagAssignmentIdentity, OptionalLong> entry : judgedFrom.entrySet()) {
      judgedTargets.put(entry.getKey().targetEntityId(), entry.getValue());
    }
    if (!judgedTargets.isEmpty()) {
      List<PolarisEntityId> targetIds = List.copyOf(judgedTargets.keySet());
      List<PolarisBaseEntity> targets = lookupTargetEntitiesInCurrentTxn(callCtx, targetIds);
      for (int i = 0; i < targetIds.size(); i++) {
        PolarisBaseEntity target = i < targets.size() ? targets.get(i) : null;
        OptionalLong judged = judgedTargets.get(targetIds.get(i));
        if (target == null) {
          // Judged from a target that did not resolve, and it still does not: the orphan case.
          if (judged.isPresent()) {
            return false;
          }
        } else if (judged.isEmpty() || target.getEntityVersion() != judged.getAsLong()) {
          return false;
        }
      }
    }
    deleteAllEntityTagAssignmentRecordsInCurrentTxn(
        callCtx, tagEntity, assignmentsOnTag, List.of());
    return true;
  }

  /**
   * Reads assignment targets inside the current transaction, positionally, with a null where no
   * entity with that id exists. Declared here because {@link
   * #deleteClassifiedTagAssignmentsOnTagInCurrentTxn} has to check the state its caller judged each
   * row from, and a target may be an entity of any type, so the read cannot be filtered by one.
   */
  @NonNull
  default List<PolarisBaseEntity> lookupTargetEntitiesInCurrentTxn(
      @NonNull PolarisCallContext callCtx, @NonNull List<PolarisEntityId> targetEntityIds) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }

  /** See {@link TagAssignmentPersistence#loadAllTargetsOnTag} */
  @NonNull
  default List<TagAssignmentRecord> loadAllTargetsOnTagInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      long tagCatalogId,
      long tagId,
      @Nullable String valueFilter,
      @NonNull PageToken pageToken,
      @NonNull CandidateBudget candidateBudget) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }
}
