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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.quarkus.runtime.Startup;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.events.AfterAttemptTaskEvent;
import org.apache.polaris.service.events.BeforeAttemptTaskEvent;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.tracing.TracingFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a list of registered {@link TaskHandler}s, execute tasks asynchronously with the provided
 * {@link RealmContext}.
 */
@ApplicationScoped
public class TaskExecutorImpl implements TaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutorImpl.class);
  private static final long TASK_RETRY_DELAY = 1000;

  private final Executor executor;
  private final Clock clock;
  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final TaskFileIOSupplier fileIOSupplier;
  private final List<TaskHandler> taskHandlers = new CopyOnWriteArrayList<>();
  private final PolarisEventListener polarisEventListener;
  @Nullable private final Tracer tracer;

  @SuppressWarnings("unused") // Required by CDI
  protected TaskExecutorImpl() {
    this(null, null, null, null, null, null);
  }

  @Inject
  public TaskExecutorImpl(
      @Identifier("task-executor") Executor executor,
      Clock clock,
      MetaStoreManagerFactory metaStoreManagerFactory,
      TaskFileIOSupplier fileIOSupplier,
      PolarisEventListener polarisEventListener,
      @Nullable Tracer tracer) {
    this.executor = executor;
    this.clock = clock;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.fileIOSupplier = fileIOSupplier;
    this.polarisEventListener = polarisEventListener;
    this.tracer = tracer;
  }

  @Startup
  public void init() {
    addTaskHandler(
        new TableCleanupTaskHandler(this, clock, metaStoreManagerFactory, fileIOSupplier));
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
   * Register a {@link RealmContext} for a specific task id. That task will be loaded and executed
   * asynchronously with a clone of the provided {@link RealmContext}.
   */
  @Override
  @SuppressWarnings("FutureReturnValueIgnored") // it _should_ be okay in this particular case
  public void addTaskHandlerContext(
      RealmContext realmContext, RealmConfig realmConfig, long taskEntityId) {
    tryHandleTask(realmContext, realmConfig, taskEntityId, null, 1);
  }

  private @Nonnull CompletableFuture<Void> tryHandleTask(
      RealmContext realmContext,
      RealmConfig realmConfig,
      long taskEntityId,
      Throwable e,
      int attempt) {
    if (attempt > 3) {
      return CompletableFuture.failedFuture(e);
    }
    return CompletableFuture.runAsync(
            () -> handleTaskWithTracing(realmContext, realmConfig, taskEntityId, attempt), executor)
        .exceptionallyComposeAsync(
            (t) -> {
              LOGGER.warn("Failed to handle task entity id {}", taskEntityId, t);
              return tryHandleTask(realmContext, realmConfig, taskEntityId, t, attempt + 1);
            },
            CompletableFuture.delayedExecutor(
                TASK_RETRY_DELAY * (long) attempt, TimeUnit.MILLISECONDS, executor));
  }

  protected void handleTask(
      RealmContext realmContext, RealmConfig realmConfig, long taskEntityId, int attempt) {
    polarisEventListener.onBeforeAttemptTask(new BeforeAttemptTaskEvent(taskEntityId, attempt));

    boolean success = false;
    try {
      LOGGER.info("Handling task entity id {}", taskEntityId);
      PolarisMetaStoreManager metaStoreManager =
          metaStoreManagerFactory.createMetaStoreManager(realmContext);
      PolarisBaseEntity taskEntity =
          metaStoreManager.loadEntity(0L, taskEntityId, PolarisEntityType.TASK).getEntity();
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
      success = handler.handleTask(realmContext, realmConfig, task);
      if (success) {
        LOGGER
            .atInfo()
            .addKeyValue("taskEntityId", taskEntityId)
            .addKeyValue("handlerClass", handler.getClass())
            .log("Task successfully handled");
        metaStoreManager.dropEntityIfExists(null, taskEntity, Map.of(), false);
      } else {
        LOGGER
            .atWarn()
            .addKeyValue("taskEntityId", taskEntityId)
            .addKeyValue("taskEntityName", taskEntity.getName())
            .log("Unable to execute async task");
      }
    } finally {
      polarisEventListener.onAfterAttemptTask(
          new AfterAttemptTaskEvent(taskEntityId, attempt, success));
    }
  }

  protected void handleTaskWithTracing(
      RealmContext realmContext, RealmConfig realmConfig, long taskEntityId, int attempt) {
    if (tracer == null) {
      handleTask(realmContext, realmConfig, taskEntityId, attempt);
    } else {
      Span span =
          tracer
              .spanBuilder("polaris.task")
              .setParent(Context.current())
              .setAttribute(TracingFilter.REALM_ID_ATTRIBUTE, realmContext.getRealmIdentifier())
              .setAttribute("polaris.task.entity.id", taskEntityId)
              .setAttribute("polaris.task.attempt", attempt)
              .startSpan();
      try (Scope ignored = span.makeCurrent()) {
        handleTask(realmContext, realmConfig, taskEntityId, attempt);
      } finally {
        span.end();
      }
    }
  }
}
