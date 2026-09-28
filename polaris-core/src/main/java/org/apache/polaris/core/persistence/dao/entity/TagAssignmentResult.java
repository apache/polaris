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
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.polaris.core.tag.TagAssignmentRecord;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** result of an assign/unassign tag operation */
public class TagAssignmentResult extends BaseResult {
  // null if not success
  private final TagAssignmentRecord assignmentRecord;

  /**
   * Constructor for an error
   *
   * @param errorStatus error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public TagAssignmentResult(@NonNull ReturnStatus errorStatus, @Nullable String extraInformation) {
    super(errorStatus, extraInformation);
    this.assignmentRecord = null;
  }

  /**
   * Constructor for success
   *
   * @param assignmentRecord tag assignment record being assigned/unassigned
   */
  public TagAssignmentResult(@NonNull TagAssignmentRecord assignmentRecord) {
    super(ReturnStatus.SUCCESS);
    this.assignmentRecord = assignmentRecord;
  }

  @JsonCreator
  private TagAssignmentResult(
      @JsonProperty("returnStatus") @NonNull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") String extraInformation,
      @JsonProperty("tagAssignmentRecord") TagAssignmentRecord assignmentRecord) {
    super(returnStatus, extraInformation);
    this.assignmentRecord = assignmentRecord;
  }

  public TagAssignmentRecord getTagAssignmentRecord() {
    return assignmentRecord;
  }
}
