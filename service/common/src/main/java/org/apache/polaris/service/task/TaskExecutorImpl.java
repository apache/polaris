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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.events.AfterTaskAttemptedEvent;
import org.apache.polaris.service.events.BeforeTaskAttemptedEvent;
import org.apache.polaris.service.events.PolarisEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a list of registered {@link TaskHandler}s, execute tasks asynchronously with the provided
 * {@link CallContext}.
 */
public class TaskExecutorImpl implements TaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutorImpl.class);
  private static final long TASK_RETRY_DELAY = 1000;

  private final Executor executor;
  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final TaskFileIOSupplier fileIOSupplier;
  private final List<TaskHandler> taskHandlers = new CopyOnWriteArrayList<>();
  private final PolarisEventListener polarisEventListener;

  public TaskExecutorImpl(
      Executor executor,
      MetaStoreManagerFactory metaStoreManagerFactory,
      TaskFileIOSupplier fileIOSupplier,
      PolarisEventListener polarisEventListener) {
    this.executor = executor;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.fileIOSupplier = fileIOSupplier;
    this.polarisEventListener = polarisEventListener;
  }

  public void init() {
    addTaskHandler(new TableCleanupTaskHandler(this, metaStoreManagerFactory, fileIOSupplier));
    addTaskHandler(
        new ManifestFileCleanupTaskHandler(
            fileIOSupplier, Executors.newVirtualThreadPerTaskExecutor()));
    addTaskHandler(
        new BatchFileCleanupTaskHandler(
            fileIOSupplier, Executors.newVirtualThreadPerTaskExecutor()));
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
  @SuppressWarnings("FutureReturnValueIgnored") // it _should_ be okay in this particular case
  public void addTaskHandlerContext(long taskEntityId, CallContext callContext) {
    // Unfortunately CallContext is a request-scoped bean and must be cloned now,
    // because its usage inside the TaskExecutor thread pool will outlive its
    // lifespan, so the original CallContext will eventually be closed while
    // the task is still running.
    // Note: PolarisCallContext has request-scoped beans as well, and must be cloned.
    // FIXME replace with context propagation?
    CallContext clone = CallContext.copyOf(callContext);
    tryHandleTask(taskEntityId, clone, null, 1);
  }

  private @Nonnull CompletableFuture<Void> tryHandleTask(
      long taskEntityId, CallContext callContext, Throwable e, int attempt) {
    if (attempt > 3) {
      return CompletableFuture.failedFuture(e);
    }
    return CompletableFuture.runAsync(
            () -> handleTask(taskEntityId, callContext, attempt), executor)
        .exceptionallyComposeAsync(
            (t) -> {
              LOGGER.warn("Failed to handle task entity id {}", taskEntityId, t);
              return tryHandleTask(taskEntityId, callContext, t, attempt + 1);
            },
            CompletableFuture.delayedExecutor(
                TASK_RETRY_DELAY * (long) attempt, TimeUnit.MILLISECONDS, executor));
  }

  protected void handleTask(long taskEntityId, CallContext ctx, int attempt) {
    polarisEventListener.onBeforeTaskAttempted(
        new BeforeTaskAttemptedEvent(taskEntityId, ctx, attempt));

    boolean success = false;
    try {
      // set the call context INSIDE the async task
      CallContext.setCurrentContext(ctx);
      LOGGER.info("Handling task entity id {}", taskEntityId);
      PolarisMetaStoreManager metaStoreManager =
          metaStoreManagerFactory.getOrCreateMetaStoreManager(ctx.getRealmContext());
      PolarisBaseEntity taskEntity =
          metaStoreManager
              .loadEntity(ctx.getPolarisCallContext(), 0L, taskEntityId, PolarisEntityType.TASK)
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
      success = handler.handleTask(task, ctx);
      if (success) {
        LOGGER
            .atInfo()
            .addKeyValue("taskEntityId", taskEntityId)
            .addKeyValue("handlerClass", handler.getClass())
            .log("Task successfully handled");
        metaStoreManager.dropEntityIfExists(
            ctx.getPolarisCallContext(), null, taskEntity, Map.of(), false);
      } else {
        LOGGER
            .atWarn()
            .addKeyValue("taskEntityId", taskEntityId)
            .addKeyValue("taskEntityName", taskEntity.getName())
            .log("Unable to execute async task");
      }
    } finally {
      polarisEventListener.onAfterTaskAttempted(
          new AfterTaskAttemptedEvent(taskEntityId, ctx, attempt, success));
    }
  }
}
