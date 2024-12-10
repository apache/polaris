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
package org.apache.polaris.service.task;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a list of registered {@link TaskHandler}s, execute tasks asynchronously with the provided
 * {@link CallContext}.
 */
public class TaskExecutorImpl implements TaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutorImpl.class);
  public static final long TASK_RETRY_DELAY = 1000;
  private final ExecutorService executorService;
  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final List<TaskHandler> taskHandlers = new ArrayList<>();

  public TaskExecutorImpl(
      ExecutorService executorService, MetaStoreManagerFactory metaStoreManagerFactory) {
    this.executorService = executorService;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
  }

  /**
   * Add a {@link TaskHandler}. {@link TaskEntity}s will be tested against the {@link
   * TaskHandler#canHandleTask(TaskEntity)} method and will be handled by the first handler that
   * responds true.
   */
  public void addTaskHandler(TaskHandler taskHandler) {
    taskHandlers.add(taskHandler);
  }

  /**
   * Register a {@link CallContext} for a specific task id. That task will be loaded and executed
   * asynchronously with a clone of the provided {@link CallContext}.
   */
  @Override
  public void addTaskHandlerContext(long taskEntityId, CallContext callContext) {
    CallContext clone = CallContext.copyOf(callContext);
    tryHandleTask(taskEntityId, clone, null, 1);
  }

  private @Nonnull CompletableFuture<Void> tryHandleTask(
      long taskEntityId, CallContext clone, Throwable e, int attempt) {
    if (attempt > 3) {
      return CompletableFuture.failedFuture(e);
    }
    return CompletableFuture.runAsync(
            () -> {
              // set the call context INSIDE the async task
              try (CallContext ctx = CallContext.setCurrentContext(CallContext.copyOf(clone))) {
                PolarisMetaStoreManager metaStoreManager =
                    metaStoreManagerFactory.getOrCreateMetaStoreManager(ctx.getRealmContext());
                PolarisBaseEntity taskEntity =
                    metaStoreManager
                        .loadEntity(ctx.getPolarisCallContext(), 0L, taskEntityId)
                        .getEntity();
                if (!PolarisEntityType.TASK.equals(taskEntity.getType())) {
                  throw new IllegalArgumentException("Provided taskId must be a task entity type");
                }
                TaskEntity task = TaskEntity.of(taskEntity);
                Optional<TaskHandler> handlerOpt =
                    taskHandlers.stream().filter(th -> th.canHandleTask(task)).findFirst();
                if (handlerOpt.isEmpty()) {
                  LOGGER
                      .atWarn()
                      .addKeyValue("taskEntityId", taskEntityId)
                      .addKeyValue("taskType", task.getTaskType())
                      .log("Unable to find handler for task type");
                  return;
                }
                TaskHandler handler = handlerOpt.get();
                boolean success = handler.handleTask(task);
                if (success) {
                  LOGGER
                      .atInfo()
                      .addKeyValue("taskEntityId", taskEntityId)
                      .addKeyValue("handlerClass", handler.getClass())
                      .log("Task successfully handled");
                  metaStoreManager.dropEntityIfExists(
                      ctx.getPolarisCallContext(),
                      null,
                      PolarisEntity.toCore(taskEntity),
                      Map.of(),
                      false);
                } else {
                  LOGGER
                      .atWarn()
                      .addKeyValue("taskEntityId", taskEntityId)
                      .addKeyValue("taskEntityName", taskEntity.getName())
                      .log("Unable to execute async task");
                }
              }
            },
            executorService)
        .exceptionallyComposeAsync(
            (t) -> {
              LOGGER.warn("Failed to handle task entity id {}", taskEntityId, t);
              return tryHandleTask(taskEntityId, clone, t, attempt + 1);
            },
            CompletableFuture.delayedExecutor(
                TASK_RETRY_DELAY * (long) attempt, TimeUnit.MILLISECONDS, executorService));
  }
}
