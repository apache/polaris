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
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmId;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a list of registered {@link TaskHandler}s, execute tasks asynchronously with the provided
 * {@link RealmId}.
 */
public class TaskExecutorImpl implements TaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutorImpl.class);
  private static final long TASK_RETRY_DELAY = 1000;

  private final Executor executor;
  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final PolarisConfigurationStore configurationStore;
  private final PolarisDiagnostics diagnostics;
  private final TaskFileIOSupplier fileIOSupplier;
  private final Clock clock;
  private final List<TaskHandler> taskHandlers = new CopyOnWriteArrayList<>();

  public TaskExecutorImpl(
      Executor executor,
      MetaStoreManagerFactory metaStoreManagerFactory,
      PolarisConfigurationStore configurationStore,
      PolarisDiagnostics diagnostics,
      TaskFileIOSupplier fileIOSupplier,
      Clock clock) {
    this.executor = executor;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.configurationStore = configurationStore;
    this.diagnostics = diagnostics;
    this.fileIOSupplier = fileIOSupplier;
    this.clock = clock;
  }

  public void init() {
    addTaskHandler(
        new TableCleanupTaskHandler(
            this, metaStoreManagerFactory, configurationStore, diagnostics, fileIOSupplier, clock));
    addTaskHandler(
        new ManifestFileCleanupTaskHandler(
            fileIOSupplier, Executors.newVirtualThreadPerTaskExecutor(), diagnostics));
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
   * Register a {@link RealmId} for a specific task id. That task will be loaded and executed
   * asynchronously with a copy of the provided {@link RealmId} (because the realm context is a
   * request-scoped component).
   */
  @Override
  public void addTaskHandlerContext(long taskEntityId, RealmId realmId) {
    tryHandleTask(taskEntityId, realmId, null, 1);
  }

  private @Nonnull CompletableFuture<Void> tryHandleTask(
      long taskEntityId, RealmId realmId, Throwable e, int attempt) {
    if (attempt > 3) {
      return CompletableFuture.failedFuture(e);
    }
    return CompletableFuture.runAsync(() -> handleTask(taskEntityId, realmId, attempt), executor)
        .exceptionallyComposeAsync(
            (t) -> {
              LOGGER.warn("Failed to handle task entity id {}", taskEntityId, t);
              return tryHandleTask(taskEntityId, realmId, t, attempt + 1);
            },
            CompletableFuture.delayedExecutor(
                TASK_RETRY_DELAY * (long) attempt, TimeUnit.MILLISECONDS, executor));
  }

  protected void handleTask(long taskEntityId, RealmId realmId, int attempt) {
    LOGGER.info("Handling task entity id {}", taskEntityId);
    PolarisMetaStoreManager metaStoreManager =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(realmId);
    PolarisMetaStoreSession metaStoreSession =
        metaStoreManagerFactory.getOrCreateSessionSupplier(realmId).get();
    PolarisBaseEntity taskEntity =
        metaStoreManager.loadEntity(metaStoreSession, 0L, taskEntityId).getEntity();
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
          .addKeyValue("taskType", task.getTaskType(diagnostics))
          .log("Unable to find handler for task type");
      return;
    }
    TaskHandler handler = handlerOpt.get();
    boolean success = handler.handleTask(task, realmId);
    if (success) {
      LOGGER
          .atInfo()
          .addKeyValue("taskEntityId", taskEntityId)
          .addKeyValue("handlerClass", handler.getClass())
          .log("Task successfully handled");
      metaStoreManager.dropEntityIfExists(
          metaStoreSession, null, PolarisEntity.toCore(taskEntity), Map.of(), false);
    } else {
      LOGGER
          .atWarn()
          .addKeyValue("taskEntityId", taskEntityId)
          .addKeyValue("taskEntityName", taskEntity.getName())
          .log("Unable to execute async task");
    }
  }
}
