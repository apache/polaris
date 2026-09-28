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
package org.apache.polaris.core.persistence.dao.entity;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.tag.TagAssignmentRecord;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** result of a reverse lookup of the targets carrying assignments of one tag definition */
public class LoadTagAssignmentTargetsResult extends BaseResult {
  // null if not success. Else one page of tag assignment records of the tag definition
  private final Page<TagAssignmentRecord> assignments;

  // null if not success. Else the target entities referenced by the page's records, one per
  // distinct (targetCatalogId, targetId). Elements may be null when the target has been purged,
  // and returned entities may be soft-dropped: callers must filter before exposing results.
  private final List<PolarisBaseEntity> targetEntities;

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public LoadTagAssignmentTargetsResult(
      @NonNull ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.assignments = null;
    this.targetEntities = null;
  }

  /**
   * Constructor for success
   *
   * @param assignments one page of tag assignment records
   * @param targetEntities target entities referenced by the page's records
   */
  public LoadTagAssignmentTargetsResult(
      @NonNull Page<TagAssignmentRecord> assignments,
      @NonNull List<PolarisBaseEntity> targetEntities) {
    super(ReturnStatus.SUCCESS);
    this.assignments = assignments;
    this.targetEntities = targetEntities;
  }

  public Page<TagAssignmentRecord> getAssignments() {
    return assignments;
  }

  /** Non-null target entities keyed by entity id; soft-dropped entities are still included. */
  public Map<Long, PolarisBaseEntity> getTargetEntitiesAsMap() {
    return targetEntities == null
        ? null
        : targetEntities.stream()
            .filter(entity -> entity != null)
            .collect(Collectors.toMap(PolarisBaseEntity::getId, entity -> entity));
  }
}
