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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.persistence.BaseResult;

// the return the result of a drop entity
public class DropEntityResult extends BaseResult {

  /** If cleanup was requested and a task was successfully scheduled, */
  private final Long cleanupTaskId;

  /**
   * Constructor for an error
   *
   * @param errorStatus error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public DropEntityResult(@Nonnull ReturnStatus errorStatus, @Nullable String extraInformation) {
    super(errorStatus, extraInformation);
    this.cleanupTaskId = null;
  }

  /** Constructor for success when no cleanup needs to be performed */
  public DropEntityResult() {
    super(ReturnStatus.SUCCESS);
    this.cleanupTaskId = null;
  }

  /**
   * Constructor for success when a cleanup task has been scheduled
   *
   * @param cleanupTaskId id of the task which was created to clean up the table drop
   */
  public DropEntityResult(long cleanupTaskId) {
    super(ReturnStatus.SUCCESS);
    this.cleanupTaskId = cleanupTaskId;
  }

  @JsonCreator
  private DropEntityResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") String extraInformation,
      @JsonProperty("cleanupTaskId") Long cleanupTaskId) {
    super(returnStatus, extraInformation);
    this.cleanupTaskId = cleanupTaskId;
  }

  public Long getCleanupTaskId() {
    return cleanupTaskId;
  }

  @JsonIgnore
  public boolean failedBecauseNotEmpty() {
    ReturnStatus status = this.getReturnStatus();
    return status == ReturnStatus.CATALOG_NOT_EMPTY || status == ReturnStatus.NAMESPACE_NOT_EMPTY;
  }

  public boolean isEntityUnDroppable() {
    return this.getReturnStatus() == ReturnStatus.ENTITY_UNDROPPABLE;
  }
}
