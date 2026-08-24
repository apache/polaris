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

/**
 * Interface for interacting with the Polaris persistence backend for tag assignment operations.
 * This interface provides methods to persist and retrieve tag assignment records, which store one
 * selected value of one tag definition on one target in Polaris.
 *
 * <p>Note that APIs to the actual persistence store are very basic, often point read or write to
 * the underlying data store. The goal is to make it really easy to back this using databases like
 * Postgres or simpler KV store. Each API in this interface needs to be atomic.
 *
 * <p>Every method has a default implementation that throws {@link UnsupportedOperationException}: a
 * backend that does not support tag assignments is a valid configuration, and callers must treat
 * the exception as "this backend cannot perform this tag assignment operation".
 */
public interface TagAssignmentPersistence {

  /**
   * Write the specified tag assignment record to the tag_assignment_record table. If there is a
   * conflict (existing record with the same identity), the new record's value replaces the existing
   * one.
   *
   * <p>Implementations must validate that the record's value appears in the tag definition's
   * current allowed values within the same atomicity boundary as the write, so that a concurrent
   * definition update and an assignment behave as if one happened before the other.
   *
   * @param callCtx call context
   * @param record tag assignment record to write, potentially replacing the value of an existing
   *     assignment with the same identity
   */
  default void writeToTagAssignmentRecords(
      @NonNull PolarisCallContext callCtx, @NonNull TagAssignmentRecord record) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }

  /**
   * Delete the specified tag assignment record from the tag_assignment_record table. The record is
   * identified by its identity tuple only; the stored value is not part of the deletion condition,
   * so a concurrent value replacement cannot turn the deletion into a no-op.
   *
   * @param callCtx call context
   * @param record tag assignment record to delete
   * @return whether an assignment row was removed; false if no row matched the identity
   */
  default boolean deleteFromTagAssignmentRecords(
      @NonNull PolarisCallContext callCtx, @NonNull TagAssignmentRecord record) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }

  /**
   * Delete all tag assignment records in the tag_assignment_record table for the specified entity.
   * Used by the best-effort cleanup that runs when a target entity is permanently removed, and by
   * the transactional detach-all path to remove a tag definition's assignments.
   *
   * @param callCtx call context
   * @param entity entity whose tag assignment records should be deleted
   * @param assignmentsOnTag all assignments of that tag definition. Empty list if that entity is
   *     not a tag definition
   * @param assignmentsOnTarget all assignments on that target entity. Empty list if that entity is
   *     not a target
   */
  default void deleteAllEntityTagAssignmentRecords(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity entity,
      @NonNull List<TagAssignmentRecord> assignmentsOnTag,
      @NonNull List<TagAssignmentRecord> assignmentsOnTarget) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }

  /**
   * Look up the specified tag assignment record from the tag_assignment_record table. Return NULL
   * if not found.
   *
   * @param callCtx call context
   * @param targetCatalogId catalog id of the target entity
   * @param targetId id of the target entity
   * @param fieldId 0 for a whole-object assignment, else the top-level Iceberg field id
   * @param tagCatalogId catalog id of the tag definition
   * @param tagId id of the tag definition
   * @return the tag assignment record if found, NULL if not found
   */
  @Nullable
  default TagAssignmentRecord lookupTagAssignmentRecord(
      @NonNull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int fieldId,
      long tagCatalogId,
      long tagId) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }

  /**
   * Get all tag assignments stored on the specified target entity across every field, including
   * whole-object assignments (field id 0) and every column assignment. Used by target lifecycle
   * cleanup, which must see the target's complete assignment set.
   *
   * @param callCtx call context
   * @param targetCatalogId catalog id of the target entity
   * @param targetId id of the target entity
   * @return the list of tag assignment records on the target entity, all fields
   */
  @NonNull
  default List<TagAssignmentRecord> loadAllTagAssignmentsOnTargetEntity(
      @NonNull PolarisCallContext callCtx, long targetCatalogId, long targetId) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }

  /**
   * Get targets carrying an assignment of the specified tag definition, optionally filtered by an
   * exact, case-sensitive selected value.
   *
   * @param callCtx call context
   * @param tagCatalogId catalog id of the tag definition
   * @param tagId id of the tag definition
   * @param valueFilter exact selected value to filter by, or null for all values
   * @param pageToken pagination token
   * @return one page of tag assignment records for the specified tag definition
   */
  @NonNull
  default List<TagAssignmentRecord> loadAllTargetsOnTag(
      @NonNull PolarisCallContext callCtx,
      long tagCatalogId,
      long tagId,
      @Nullable String valueFilter,
      @NonNull PageToken pageToken) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }

  /**
   * Atomically delete the specified tag definition entity and every tag assignment record that
   * references it, or leave both untouched. This is the persistence capability behind dropping a
   * tag definition together with all of its assignments: callers must observe either every
   * assignment and the definition removed, or no change at all.
   *
   * <p>Implementations that cannot provide this atomically must not implement this method. Callers
   * must treat {@link UnsupportedOperationException} as "this backend cannot support dropping a tag
   * definition together with its assignments" and reject the operation; they must never proceed
   * with a non-atomic fallback, because reporting success after partial work is not allowed for tag
   * operations.
   *
   * @param callCtx call context
   * @param tagEntity the tag definition entity to delete together with its assignment records
   */
  default void deleteTagAndAllAssignmentRecords(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisBaseEntity tagEntity) {
    throw new UnsupportedOperationException("this backend does not support tag assignments");
  }
}
