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
package org.apache.polaris.delegation.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import java.time.OffsetDateTime;
import java.util.Objects;

/**
 * Common payload data included in all delegation tasks.
 *
 * <p>Contains global task information that applies to all task types within the delegation service.
 * Uses the {@link TaskType} enum to ensure type safety for operation types.
 */
public class CommonPayload {

  @NotNull private final TaskType taskType;

  @NotNull private final OffsetDateTime requestTimestampUtc;

  @NotNull private final String realmIdentifier;

  @JsonCreator
  public CommonPayload(
      @JsonProperty("task_type") @NotNull TaskType taskType,
      @JsonProperty("request_timestamp_utc") @NotNull OffsetDateTime requestTimestampUtc,
      @JsonProperty("realm_identifier") @NotNull String realmIdentifier) {
    this.taskType = taskType;
    this.requestTimestampUtc = requestTimestampUtc;
    this.realmIdentifier = realmIdentifier;
  }

  @JsonProperty("task_type")
  public TaskType getTaskType() {
    return taskType;
  }

  @JsonProperty("request_timestamp_utc")
  public OffsetDateTime getRequestTimestampUtc() {
    return requestTimestampUtc;
  }

  @JsonProperty("realm_identifier")
  public String getRealmIdentifier() {
    return realmIdentifier;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CommonPayload that = (CommonPayload) o;
    return Objects.equals(taskType, that.taskType)
        && Objects.equals(requestTimestampUtc, that.requestTimestampUtc)
        && Objects.equals(realmIdentifier, that.realmIdentifier);
  }

  @Override
  public int hashCode() {
    return Objects.hash(taskType, requestTimestampUtc, realmIdentifier);
  }

  @Override
  public String toString() {
    return "CommonPayload{"
        + "taskType="
        + taskType
        + ", requestTimestampUtc="
        + requestTimestampUtc
        + ", realmIdentifier='"
        + realmIdentifier
        + '\''
        + '}';
  }
}
