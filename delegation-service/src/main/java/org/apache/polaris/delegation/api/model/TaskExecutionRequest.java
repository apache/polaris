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
 * Request to execute a delegated task.
 *
 * <p>Contains all the information needed to submit a task to the delegation service, structured
 * according to the delegation service task payload schema.
 */
public class TaskExecutionRequest {

  @NotNull private final CommonPayload commonPayload;

  @NotNull private final OperationParameters operationParameters;

  @JsonCreator
  public TaskExecutionRequest(
      @JsonProperty("common_payload") @NotNull CommonPayload commonPayload,
      @JsonProperty("operation_parameters") @NotNull OperationParameters operationParameters) {
    this.commonPayload = commonPayload;
    this.operationParameters = operationParameters;
  }

  @JsonProperty("common_payload")
  public CommonPayload getCommonPayload() {
    return commonPayload;
  }

  @JsonProperty("operation_parameters")
  public OperationParameters getOperationParameters() {
    return operationParameters;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TaskExecutionRequest that = (TaskExecutionRequest) o;
    return Objects.equals(commonPayload, that.commonPayload)
        && Objects.equals(operationParameters, that.operationParameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commonPayload, operationParameters);
  }

  @Override
  public String toString() {
    return "TaskExecutionRequest{"
        + "commonPayload="
        + commonPayload
        + ", operationParameters="
        + operationParameters
        + '}';
  }
}
