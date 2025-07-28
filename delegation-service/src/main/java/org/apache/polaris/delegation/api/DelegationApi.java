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
package org.apache.polaris.delegation.api;

import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.polaris.delegation.api.model.TaskExecutionRequest;
import org.apache.polaris.delegation.api.model.TaskExecutionResponse;
import org.apache.polaris.delegation.api.model.TaskType;
import org.apache.polaris.delegation.service.TaskExecutionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST API for the Polaris Delegation Service.
 *
 * <p>Provides endpoints for submitting long-running tasks for synchronous execution. This API
 * allows the main Polaris catalog to offload resource-intensive operations to maintain low-latency
 * performance.
 *
 * <p><strong>Note:</strong> This is the initial API framework implementation. The actual task
 * execution logic will be implemented in future development phases.
 */
@Path("/api/v1/tasks/execute")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class DelegationApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(DelegationApi.class);

  @Inject private TaskExecutionService taskExecutionService;

  /**
   * Submit a task for delegated execution.
   *
   * <p>The task will be executed synchronously and the response will contain the execution result.
   * This is a blocking operation that will wait for the task to complete before returning.
   *
   * @param request the task execution request
   * @return the task execution response with completion information
   */
  @POST
  @Path("/synchronous")
  public Response submitTask(@Valid @NotNull TaskExecutionRequest request) {
    TaskType taskType = request.getCommonPayload().getTaskType();
    String realmId = request.getCommonPayload().getRealmIdentifier();
    LOGGER.info("Delegation API called - task_type={}, realm_id={}", taskType, realmId);

    try {
      // Execute the task synchronously using the TaskExecutionService
      TaskExecutionResponse response = taskExecutionService.executeTask(request);

      LOGGER.info(
          "Task execution completed - task_type={}, status={}", taskType, response.getStatus());
      return Response.ok(response).build();

    } catch (Exception e) {
      LOGGER.error("Failed to process task request", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new ErrorResponse("Task processing failed: " + e.getMessage()))
          .build();
    }
  }

  /** Simple error response model. */
  public static class ErrorResponse {
    private final String message;

    public ErrorResponse(String message) {
      this.message = message;
    }

    public String getMessage() {
      return message;
    }
  }
}
