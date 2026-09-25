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
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.LoadAllTagAssignmentTargetsResult;
import org.apache.polaris.core.persistence.dao.entity.LoadTagAssignmentTargetsResult;
import org.apache.polaris.core.persistence.dao.entity.LoadTagAssignmentsResult;
import org.apache.polaris.core.persistence.dao.entity.TagAssignmentResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

public interface PolarisTagAssignmentManager {

  /**
   * Assign a tag to a target entity, for example assign a tag to a table or to one of its columns.
   *
   * <p>For one tag definition and one target, at most one assignment exists: assigning the same tag
   * to the same target replaces the stored value.
   *
   * @param callCtx call context
   * @param targetCatalogPath path to the target entity
   * @param target target entity; for a column assignment, the containing table
   * @param fieldId 0 to tag the whole target entity, else the top-level Iceberg field id of the
   *     column being tagged
   * @param tagCatalogPath path to the tag definition entity
   * @param tag tag definition entity
   * @param value the selected value; must appear in the definition's current allowed values
   * @return The tag assignment record we created or replaced for this assignment. Will return
   *     ENTITY_CANNOT_BE_RESOLVED if the specified target does not exist and ENTITY_NOT_FOUND if
   *     the tag definition does not exist; a target miss classifies before a tag miss.
   */
  @NonNull TagAssignmentResult assignTagToEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisEntityCore> targetCatalogPath,
      @NonNull PolarisEntityCore target,
      int fieldId,
      @NonNull List<PolarisEntityCore> tagCatalogPath,
      @NonNull TagEntity tag,
      @NonNull String value);

  /**
   * Unassign a tag from a target entity.
   *
   * @param callCtx call context
   * @param targetCatalogPath path to the target entity
   * @param target target entity; for a column assignment, the containing table
   * @param fieldId 0 for a whole-object assignment, else the top-level Iceberg field id
   * @param tagCatalogPath path to the tag definition entity
   * @param tag tag definition entity
   * @return The tag assignment record we removed. Will return ENTITY_CANNOT_BE_RESOLVED if the
   *     specified target does not exist and ENTITY_NOT_FOUND if the tag definition does not exist
   *     (a target miss classifies before a tag miss). Will return TAG_ASSIGNMENT_NOT_FOUND if the
   *     assignment cannot be found.
   */
  @NonNull TagAssignmentResult unassignTagFromEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisEntityCore> targetCatalogPath,
      @NonNull PolarisEntityCore target,
      int fieldId,
      @NonNull List<PolarisEntityCore> tagCatalogPath,
      @NonNull TagEntity tag);

  /**
   * Load every assignment record of the specified tag definition together with the entities those
   * records target, so a caller can tell which of them are still live. Unpaginated: a caller that
   * has to decide something about the whole set, such as whether any live assignment blocks a
   * definition delete, cannot answer it from one page.
   *
   * <p>A target entity comes back null when its id no longer resolves, which is how a row whose
   * target was permanently removed is recognized. Entity ids are never reused, so a null target
   * means that row can never become live again. Whether a returned entity counts as live is the
   * caller's decision, not this method's: it returns what the store holds and filters nothing.
   *
   * @param callCtx call context
   * @param tag tag definition entity
   * @return every assignment record of the definition, plus the target entities they reference, one
   *     per distinct target. Will return ENTITY_NOT_FOUND if the tag definition does not exist, and
   *     TAG_ASSIGNMENTS_NOT_SUPPORTED if the backend cannot store assignments at all.
   */
  @NonNull LoadAllTagAssignmentTargetsResult loadAllTargetsOnTagWithEntities(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisEntityCore tag);

  /**
   * Drop a tag definition together with the assignment rows a caller has already classified,
   * removing both or neither. This is the drop of a definition whose surviving assignments the
   * caller judged inert: deciding that a row is inert needs information this layer does not have
   * (whether a column of a table's current schema still exists), so the caller decides and this
   * method holds it to what it saw.
   *
   * <p>An assignment row outside {@code classifiedAssignments} was written after the caller
   * classified and was never judged, so it may be live, and a live assignment must block a drop
   * that does not detach. Such a row leaves the definition and every row untouched and answers
   * TARGET_ENTITY_CONCURRENTLY_MODIFIED, which the caller answers by classifying again on fresh
   * state rather than by reporting a conflict: nothing about the request has failed, only the state
   * it was decided against has moved.
   *
   * <p>{@link org.apache.polaris.core.persistence.PolarisMetaStoreManager#dropEntityIfExists} with
   * {@code cleanup=true} remains the unconditional removal that detach-all asks for; it is the
   * caller's statement that every assignment is to go regardless of what it names, so it carries no
   * classification and checks none.
   *
   * @param callCtx call context
   * @param catalogPath path to the tag definition, as it was resolved by the caller
   * @param tagToDrop the tag definition entity to drop, resolved by the caller
   * @param classifiedAssignments the identities of every assignment row the caller classified; an
   *     empty set requires the definition to hold no assignment row at all
   * @return success when the definition and its assignment rows were removed; ENTITY_NOT_FOUND when
   *     the definition no longer exists, TARGET_ENTITY_CONCURRENTLY_MODIFIED when the
   *     classification no longer describes the definition's rows, and TAG_ASSIGNMENTS_NOT_SUPPORTED
   *     when the backend cannot remove a definition and its assignments together
   */
  @NonNull DropEntityResult dropTagAndClassifiedAssignmentsIfExists(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity tagToDrop,
      @NonNull Set<ClassifiedAssignment> classifiedAssignments);

  /**
   * One level of a tag read: a target entity plus the field being queried on it. Used to load every
   * level of an effective-tag traversal (or the single level of a direct read) in one call.
   *
   * <p>The entity is the caller's own resolved copy, and the version it carries is part of the
   * request: the read validates that the level still looks the way the caller resolved it, so pass
   * the entity as resolved rather than a freshly built stand-in.
   *
   * @param entity target entity for this level, as the caller resolved it
   * @param fieldId 0 for whole-object assignments, else a top-level Iceberg field id
   * @param metadataLocation for a field level, the Iceberg metadata pointer the caller resolved
   *     that field against, so the read can establish that the schema it used and the assignments
   *     it returns describe one state; null for a whole-object level
   */
  record TargetLevel(
      @NonNull PolarisEntityCore entity, int fieldId, @Nullable String metadataLocation) {

    /**
     * A level whose assignments are addressed by the object alone. Whole-object levels resolve no
     * schema, so they carry no metadata pointer.
     */
    public TargetLevel(@NonNull PolarisEntityCore entity, int fieldId) {
      this(entity, fieldId, null);
    }
  }

  /**
   * Load all tag assignments stored on a list of (target, field) levels, in one read spanning every
   * requested level.
   *
   * <p>Implementations must return one snapshot across every requested level: the levels loaded by
   * one call describe a real state that existed, never a result stitched from independent reads
   * taken at different times. Callers that need several levels to describe one coherent state, for
   * example an effective-tag traversal of a parent chain, must request them together through one
   * call rather than once per level.
   *
   * <p>The same rule covers the whole result, not just the rows. The returned assignment records,
   * the returned tag definitions, and the levels the caller passed in must all belong to one state
   * that existed. Reading rows and definitions in separate statements does not on its own establish
   * that: replace an assignment between the two reads, rename its definition in the same window,
   * and the result pairs the old row with the new name, a pair no caller could ever have observed.
   * An implementation is free to choose its mechanism -- a shared snapshot, a read transaction,
   * validate and retry -- but an implementation that cannot establish the guarantee must fail
   * rather than return a result that may not be coherent.
   *
   * <p>The levels bound how far the read walks, but they do not bound how much it reads: one level
   * may carry any number of assignments. The caller therefore states how many candidate rows the
   * whole read may consume, and the result carries at most one row beyond that so the caller can
   * tell a trimmed read from a complete one and answer its own request rather than report a partial
   * set as the whole.
   *
   * @param callCtx call context
   * @param levels the (target, fieldId) levels to load, in no particular order
   * @param candidateBudget the greatest number of assignment rows this read may consume across
   *     every requested level; {@link Integer#MAX_VALUE} for an unbounded read
   * @return every tag assignment record found across the requested levels, up to one row beyond the
   *     budget, plus every referenced tag definition entity. Will return ENTITY_NOT_FOUND if any
   *     requested target no longer exists: a missing level fails the whole read rather than a
   *     partial result.
   * @throws org.apache.polaris.core.exceptions.PolarisServiceUnavailableException if concurrent
   *     writes prevent the implementation from establishing that the result describes one state
   */
  @NonNull LoadTagAssignmentsResult loadTagsOnEntities(
      @NonNull PolarisCallContext callCtx, @NonNull List<TargetLevel> levels, int candidateBudget);

  /**
   * Load one page of the targets carrying an assignment of the specified tag definition, optionally
   * filtered by an exact, case-sensitive selected value. Returns only direct assignments, in
   * deterministic (targetId, fieldId) order when pagination is requested.
   *
   * <p>Each returned row and the target entity it names must belong to one assignment state. A row
   * carries a stored value and the entity carries the names a caller renders the target from, so
   * reading them in separate statements does not on its own establish that: replace an assignment's
   * value between the two reads, rename its target in the same window, and the result pairs the old
   * value with the new name, a pair no caller could ever have observed. As with {@link
   * #loadTagsOnEntities}, an implementation chooses its mechanism -- a shared snapshot, a read
   * transaction, validate and retry -- and one that cannot establish the guarantee must fail rather
   * than return a result that may not be coherent.
   *
   * @param callCtx call context
   * @param tag tag definition entity
   * @param valueFilter exact selected value to filter by, or null for all values
   * @param pageToken pagination token
   * @param candidateBudget how many candidate rows this read may still examine, shared with every
   *     other read the same request makes; a store charges it for the rows it examines, or for the
   *     rows it returns where its reads are bounded that way, and {@link
   *     CandidateBudget#unbounded()} for a read that has no page to bound its work with
   * @return one page of assignment records plus the referenced target entities. Will return
   *     ENTITY_NOT_FOUND if the tag definition does not exist. Returned target entities may be null
   *     (purged) or soft-dropped: callers must filter before exposing results.
   * @throws org.apache.polaris.core.exceptions.PolarisServiceUnavailableException if concurrent
   *     writes prevent the implementation from establishing that each row and the target it names
   *     describe one state
   * @throws org.apache.polaris.core.tag.exceptions.CandidateBudgetExceededException if the read had
   *     rows left to examine and no budget to examine them with; it never answers short in that
   *     case, because a short answer would read as the end of the definition
   */
  @NonNull LoadTagAssignmentTargetsResult loadTargetsOnTag(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore tag,
      @Nullable String valueFilter,
      @NonNull PageToken pageToken,
      @NonNull CandidateBudget candidateBudget);
}
