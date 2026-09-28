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
import org.apache.polaris.core.persistence.dao.entity.TagAssignmentResult;
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
}
