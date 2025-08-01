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
package org.apache.polaris.delegation.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.delegation.api.model.TableIdentity;
import org.apache.polaris.delegation.api.model.TablePurgeParameters;
import org.apache.polaris.delegation.api.model.TaskExecutionRequest;
import org.apache.polaris.delegation.api.model.TaskExecutionResponse;
import org.apache.polaris.delegation.api.model.TaskType;
import org.apache.polaris.delegation.service.storage.StorageFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service responsible for executing delegated tasks.
 *
 * <p>This service handles the actual execution of tasks delegated from the main Polaris catalog,
 * including data file cleanup, table purging, and other long-running operations.
 */
@ApplicationScoped
public class TaskExecutionService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutionService.class);

  @Inject private StorageFileManager storageFileManager;

  /**
   * Executes a delegated task synchronously.
   *
   * @param request the task execution request
   * @return the task execution response with completion status
   */
  public TaskExecutionResponse executeTask(TaskExecutionRequest request) {
    TaskType taskType = request.getCommonPayload().getTaskType();
    String realmId = request.getCommonPayload().getRealmIdentifier();

    LOGGER.info("Starting task execution: type={}, realm={}", taskType, realmId);

    try {
      switch (taskType) {
        case PURGE_TABLE:
          return executePurgeTableTask(request);
        default:
          LOGGER.warn("Unknown task type: {}", taskType);
          return new TaskExecutionResponse("failed", "Unknown task type: " + taskType);
      }
    } catch (Exception e) {
      LOGGER.error("Task execution failed for type={}, realm={}", taskType, realmId, e);
      return new TaskExecutionResponse("failed", "Task execution failed: " + e.getMessage());
    }
  }

  /** Executes a table purge task by cleaning up data files. */
  private TaskExecutionResponse executePurgeTableTask(TaskExecutionRequest request) {
    String realmIdentifier = request.getCommonPayload().getRealmIdentifier();
    TablePurgeParameters purgeParams = request.getOperationParameters();
    TableIdentity tableIdentity = purgeParams.getTableIdentity();

    LOGGER.info(
        "Executing table purge for table: {}.{}.{} in realm: {}",
        tableIdentity.getCatalogName(),
        String.join(".", tableIdentity.getNamespaceLevels()),
        tableIdentity.getTableName(),
        realmIdentifier);

    try {
      // Execute the actual data file cleanup
      long startTime = System.currentTimeMillis();

      StorageFileManager.CleanupResult result =
          storageFileManager.cleanupTableFiles(tableIdentity, realmIdentifier);

      long duration = System.currentTimeMillis() - startTime;

      if (result.isSuccess()) {
        String summary =
            String.format(
                "Successfully cleaned up %d data files in %d ms",
                result.getFilesDeleted(), duration);
        LOGGER.info("Table purge completed successfully: {}", summary);
        return new TaskExecutionResponse("success", summary);
      } else {
        String errorSummary =
            String.format("Failed to clean up table files: %s", result.getErrorMessage());
        LOGGER.error("Table purge failed: {}", errorSummary);
        return new TaskExecutionResponse("failed", errorSummary);
      }
    } catch (Exception e) {
      LOGGER.error(
          "Failed to execute purge task for table: {}.{}.{} in realm: {}",
          tableIdentity.getCatalogName(),
          String.join(".", tableIdentity.getNamespaceLevels()),
          tableIdentity.getTableName(),
          realmIdentifier,
          e);
      return new TaskExecutionResponse("failed", "Table purge failed: " + e.getMessage());
    }
  }

  /**
   * Executes a task asynchronously with timeout (for future use). Currently not used as we're
   * implementing synchronous execution for MVP.
   */
  public CompletableFuture<TaskExecutionResponse> executeTaskAsync(
      TaskExecutionRequest request, long timeoutSeconds) {
    return CompletableFuture.supplyAsync(() -> executeTask(request))
        .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
        .exceptionally(
            throwable -> {
              LOGGER.error("Async task execution failed or timed out", throwable);
              return new TaskExecutionResponse(
                  "failed", "Task execution failed or timed out: " + throwable.getMessage());
            });
  }
}
