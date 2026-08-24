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

import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
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
            callCtx, tagEntity.getCatalogId(), tagEntity.getId(), null, PageToken.readEverything());
    deleteAllEntityTagAssignmentRecordsInCurrentTxn(
        callCtx, tagEntity, assignmentsOnTag, List.of());
  }

  /** See {@link TagAssignmentPersistence#loadAllTargetsOnTag} */
  @NonNull
  default List<TagAssignmentRecord> loadAllTargetsOnTagInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      long tagCatalogId,
      long tagId,
      @Nullable String valueFilter,
      @NonNull PageToken pageToken) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }
}
