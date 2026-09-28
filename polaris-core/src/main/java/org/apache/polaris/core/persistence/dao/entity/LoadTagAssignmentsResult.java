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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.tag.TagAssignmentRecord;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Result of {@code loadTagsOnEntities}, covering every requested (target, field) level in one read.
 * The reverse lookup from a tag definition's own side has its own result type, {@link
 * LoadTagAssignmentTargetsResult}.
 */
public class LoadTagAssignmentsResult extends BaseResult {
  // null if not success. Else every tag assignment record found across every requested (target,
  // field) level, or from a tag definition
  private final List<TagAssignmentRecord> assignmentRecords;

  // null if not success. Else, for each tag assignment record, list of target or tag entities
  private final List<PolarisBaseEntity> entities;

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public LoadTagAssignmentsResult(
      @NonNull ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.assignmentRecords = null;
    this.entities = null;
  }

  /**
   * Constructor for success
   *
   * @param assignmentRecords tag assignment records
   * @param entities tag definition or target entities
   */
  public LoadTagAssignmentsResult(
      @NonNull List<TagAssignmentRecord> assignmentRecords,
      @NonNull List<PolarisBaseEntity> entities) {
    super(ReturnStatus.SUCCESS);
    this.assignmentRecords = assignmentRecords;
    this.entities = entities;
  }

  @JsonCreator
  private LoadTagAssignmentsResult(
      @JsonProperty("returnStatus") @NonNull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") String extraInformation,
      @JsonProperty("tagAssignmentRecords") List<TagAssignmentRecord> assignmentRecords,
      @JsonProperty("entities") List<PolarisBaseEntity> entities) {
    super(returnStatus, extraInformation);
    this.assignmentRecords = assignmentRecords;
    this.entities = entities;
  }

  public List<TagAssignmentRecord> getTagAssignmentRecords() {
    return assignmentRecords;
  }

  public List<PolarisBaseEntity> getEntities() {
    return entities;
  }

  @JsonIgnore
  @Override
  public String toString() {
    return "LoadTagAssignmentsResult{"
        + "assignmentRecords="
        + assignmentRecords
        + ", entities="
        + entities
        + '}';
  }
}
