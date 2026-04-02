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
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.function.TriConsumer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventDispatcher;
import org.apache.polaris.service.events.PolarisEventMetadata;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.tracing.TracingFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a list of registered {@link TaskHandler}s, execute tasks asynchronously with the provided
 * {@link CallContext}.
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
  private final Optional<TriConsumer<Long, Boolean, Throwable>> errorHandler;
  private final PolarisEventDispatcher polarisEventDispatcher;
  private final PolarisEventMetadataFactory eventMetadataFactory;
  @Nullable private final Tracer tracer;
  private final Instance<AsyncContextPropagator> contextPropagators;

  @SuppressWarnings("unused") // Required by CDI
  protected TaskExecutorImpl() {
    this(null, null, null, null, null, null, null, null, null);
  }

  @Inject
  public TaskExecutorImpl(
      @Identifier("task-executor") Executor executor,
      @Identifier("task-error-handler")
          Instance<TriConsumer<Long, Boolean, Throwable>> errorHandler,
      Clock clock,
      MetaStoreManagerFactory metaStoreManagerFactory,
      TaskFileIOSupplier fileIOSupplier,
      PolarisEventDispatcher polarisEventDispatcher,
      PolarisEventMetadataFactory eventMetadataFactory,
      @Nullable Tracer tracer,
      Instance<AsyncContextPropagator> contextPropagators) {
    this.executor = executor;
    this.clock = clock;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.fileIOSupplier = fileIOSupplier;
    this.polarisEventDispatcher = polarisEventDispatcher;
    this.eventMetadataFactory = eventMetadataFactory;
    this.tracer = tracer;
    this.contextPropagators = contextPropagators;

    if (errorHandler != null && errorHandler.isResolvable()) {
      this.errorHandler = Optional.of(errorHandler.get());
    } else {
      this.errorHandler = Optional.empty();
    }
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
   * Register a {@link CallContext} for a specific task id. That task will be loaded and executed
   * asynchronously with a clone of the provided {@link CallContext}.
   */
  @Override
  @SuppressWarnings("FutureReturnValueIgnored") // it _should_ be okay in this particular case
  public void addTaskHandlerContext(long taskEntityId, CallContext callContext) {
    errorHandler.ifPresent(h -> h.accept(taskEntityId, true, null));
    // Unfortunately CallContext is a request-scoped bean and must be cloned now,
    // because its usage inside the TaskExecutor thread pool will outlive its
    // lifespan, so the original CallContext will eventually be closed while
    // the task is still running.
    // Note: PolarisCallContext has request-scoped beans as well, and must be cloned.
    // FIXME replace with context propagation?
    CallContext clone = callContext.copy();

    // Capture the metadata now in order to capture the principal and request ID, if any.
    PolarisEventMetadata eventMetadata = eventMetadataFactory.create();

    // Capture request-scoped context for propagation into the task thread.
    // Each propagator independently snapshots its own piece of context.
    List<AsyncContextPropagator.RestoreAction> actions = new ArrayList<>();
    for (AsyncContextPropagator propagator : contextPropagators) {
      actions.add(propagator.capture());
    }

    tryHandleTask(taskEntityId, clone, eventMetadata, actions, null, 1);
  }

  private @Nonnull CompletableFuture<Void> tryHandleTask(
      long taskEntityId,
      CallContext callContext,
      PolarisEventMetadata eventMetadata,
      List<AsyncContextPropagator.RestoreAction> actions,
      Throwable previousError,
      int attempt) {
    if (attempt > 3) {
      return CompletableFuture.failedFuture(previousError);
    }

    return CompletableFuture.runAsync(
            () -> {
              handleTaskWithTracing(taskEntityId, callContext, actions, eventMetadata, attempt);
              errorHandler.ifPresent(h -> h.accept(taskEntityId, false, null));
            },
            executor)
        .exceptionallyComposeAsync(
            (t) -> {
              if (previousError != null) {
                t.addSuppressed(previousError);
              }
              LOGGER.warn("Failed to handle task entity id {}", taskEntityId, t);
              errorHandler.ifPresent(h -> h.accept(taskEntityId, false, t));
              return tryHandleTask(
                  taskEntityId, callContext, eventMetadata, actions, t, attempt + 1);
            },
            CompletableFuture.delayedExecutor(
                TASK_RETRY_DELAY * (long) attempt, TimeUnit.MILLISECONDS, executor));
  }

  protected void handleTask(
      long taskEntityId, CallContext ctx, PolarisEventMetadata eventMetadata, int attempt) {
    polarisEventDispatcher.dispatch(
        new PolarisEvent(
            PolarisEventType.BEFORE_ATTEMPT_TASK,
            eventMetadataFactory.copy(eventMetadata),
            new EventAttributeMap()
                .put(EventAttributes.TASK_ENTITY_ID, taskEntityId)
                .put(EventAttributes.TASK_ATTEMPT, attempt)));

    boolean success = false;
    try {
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
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.AFTER_ATTEMPT_TASK,
              eventMetadataFactory.copy(eventMetadata),
              new EventAttributeMap()
                  .put(EventAttributes.TASK_ENTITY_ID, taskEntityId)
                  .put(EventAttributes.TASK_ATTEMPT, attempt)
                  .put(EventAttributes.TASK_SUCCESS, success)));
    }
  }

  @ActivateRequestContext
  protected void handleTaskWithTracing(
      long taskEntityId,
      CallContext callContext,
      List<AsyncContextPropagator.RestoreAction> actions,
      PolarisEventMetadata eventMetadata,
      int attempt) {
    // Note: each call to this method runs in a new CDI request context.
    // Restore all propagated context into the fresh request scope.
    // A restore failure is fatal: running a task without proper realm or principal context
    // risks wrong-tenant or wrong-identity execution, so we abort rather than continue.
    // The restore loop is inside the try block so the finally block cleans up any
    // actions that were already restored before the failure.
    int restored = 0;
    try {
      for (AsyncContextPropagator.RestoreAction action : actions) {
        action.restore();
        restored++;
      }

      if (tracer == null) {
        handleTask(taskEntityId, callContext, eventMetadata, attempt);
      } else {
        Span span =
            tracer
                .spanBuilder("polaris.task")
                .setParent(Context.current())
                .setAttribute(
                    TracingFilter.REALM_ID_ATTRIBUTE,
                    callContext.getRealmContext().getRealmIdentifier())
                .setAttribute("polaris.task.entity.id", taskEntityId)
                .setAttribute("polaris.task.attempt", attempt)
                .startSpan();
        try (Scope ignored = span.makeCurrent()) {
          handleTask(taskEntityId, callContext, eventMetadata, attempt);
        } finally {
          span.end();
        }
      }
    } finally {
      // Close in reverse order (LIFO) so that later actions clean up before
      // earlier ones, matching standard stacked-context conventions.
      for (int i = restored - 1; i >= 0; i--) {
        try {
          actions.get(i).close();
        } catch (Exception e) {
          LOGGER.warn("Failed to close context propagator cleanup", e);
        }
      }
    }
  }
}
