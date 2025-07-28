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
import java.util.Objects;

/**
 * Response from executing a delegated task.
 *
 * <p>Contains the execution result and timing information for the completed task. Since the
 * delegation service executes tasks synchronously in the MVP, this response indicates the task has
 * already been completed.
 */
public class TaskExecutionResponse {

  @NotNull private final String status;

  private final String resultSummary;

  @JsonCreator
  public TaskExecutionResponse(
      @JsonProperty("status") @NotNull String status,
      @JsonProperty("result_summary") String resultSummary) {
    this.status = status;
    this.resultSummary = resultSummary;
  }

  @JsonProperty("status")
  public String getStatus() {
    return status;
  }

  @JsonProperty("result_summary")
  public String getResultSummary() {
    return resultSummary;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TaskExecutionResponse that = (TaskExecutionResponse) o;
    return Objects.equals(status, that.status) && Objects.equals(resultSummary, that.resultSummary);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, resultSummary);
  }

  @Override
  public String toString() {
    return "TaskExecutionResponse{"
        + "status='"
        + status
        + '\''
        + ", resultSummary='"
        + resultSummary
        + '\''
        + '}';
  }
}
