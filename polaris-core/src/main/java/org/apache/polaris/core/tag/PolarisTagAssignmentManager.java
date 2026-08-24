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
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.dao.entity.TagAssignmentResult;
import org.jspecify.annotations.NonNull;

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
}
