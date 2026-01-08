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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.function.TriConsumer;
import org.apache.polaris.core.auth.ImmutablePolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.apache.polaris.service.events.AttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadata;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
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
  private final RealmContextHolder realmContextHolder;
  private final PolarisPrincipalHolder polarisPrincipalHolder;
  private final PolarisPrincipal polarisPrincipal;
  private final List<TaskHandler> taskHandlers = new CopyOnWriteArrayList<>();
  private final Optional<TriConsumer<Long, Boolean, Throwable>> errorHandler;
  private final PolarisEventListener polarisEventListener;
  private final PolarisEventMetadataFactory eventMetadataFactory;
  @Nullable private final Tracer tracer;

  @SuppressWarnings("unused") // Required by CDI
  protected TaskExecutorImpl() {
    this(null, null, null, null, null, null, null, null, null, null, null);
  }

  @Inject
  public TaskExecutorImpl(
      @Identifier("task-executor") Executor executor,
      @Identifier("task-error-handler")
          Instance<TriConsumer<Long, Boolean, Throwable>> errorHandler,
      Clock clock,
      MetaStoreManagerFactory metaStoreManagerFactory,
      TaskFileIOSupplier fileIOSupplier,
      RealmContextHolder realmContextHolder,
      PolarisEventListener polarisEventListener,
      PolarisEventMetadataFactory eventMetadataFactory,
      @Nullable Tracer tracer,
      PolarisPrincipalHolder polarisPrincipalHolder,
      PolarisPrincipal polarisPrincipal) {
    this.executor = executor;
    this.clock = clock;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.fileIOSupplier = fileIOSupplier;
    this.realmContextHolder = realmContextHolder;
    this.polarisEventListener = polarisEventListener;
    this.eventMetadataFactory = eventMetadataFactory;
    this.tracer = tracer;
    this.polarisPrincipalHolder = polarisPrincipalHolder;
    this.polarisPrincipal = polarisPrincipal;

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
    tryHandleTask(taskEntityId, clone, eventMetadata, null, 1);
  }

  private @Nonnull CompletableFuture<Void> tryHandleTask(
      long taskEntityId,
      CallContext callContext,
      PolarisEventMetadata eventMetadata,
      Throwable e,
      int attempt) {
    if (attempt > 3) {
      return CompletableFuture.failedFuture(e);
    }
    String realmId = callContext.getRealmContext().getRealmIdentifier();

    PolarisPrincipal principalClone =
        ImmutablePolarisPrincipal.builder().from(polarisPrincipal).build();

    return CompletableFuture.runAsync(
            () -> {
              handleTaskWithTracing(
                  realmId, taskEntityId, callContext, principalClone, eventMetadata, attempt);
              errorHandler.ifPresent(h -> h.accept(taskEntityId, false, null));
            },
            executor)
        .exceptionallyComposeAsync(
            (t) -> {
              LOGGER.warn("Failed to handle task entity id {}", taskEntityId, t);
              errorHandler.ifPresent(h -> h.accept(taskEntityId, false, e));
              return tryHandleTask(taskEntityId, callContext, eventMetadata, t, attempt + 1);
            },
            CompletableFuture.delayedExecutor(
                TASK_RETRY_DELAY * (long) attempt, TimeUnit.MILLISECONDS, executor));
  }

  protected void handleTask(
      long taskEntityId, CallContext ctx, PolarisEventMetadata eventMetadata, int attempt) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_ATTEMPT_TASK,
            eventMetadataFactory.copy(eventMetadata),
            new AttributeMap()
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
      polarisEventListener.onEvent(
          new PolarisEvent(
              PolarisEventType.AFTER_ATTEMPT_TASK,
              eventMetadataFactory.copy(eventMetadata),
              new AttributeMap()
                  .put(EventAttributes.TASK_ENTITY_ID, taskEntityId)
                  .put(EventAttributes.TASK_ATTEMPT, attempt)
                  .put(EventAttributes.TASK_SUCCESS, success)));
    }
  }

  @ActivateRequestContext
  protected void handleTaskWithTracing(
      String realmId,
      long taskEntityId,
      CallContext callContext,
      PolarisPrincipal principal,
      PolarisEventMetadata eventMetadata,
      int attempt) {
    // Note: each call to this method runs in a new CDI request context

    realmContextHolder.set(() -> realmId);
    // since this is now a different context we store clone of the principal in a holder object
    // which essentially reauthenticates the principal. PolarisPrincipal bean always looks for a
    // principal set in PolarisPrincipalHolder first and assumes that identity if set.
    polarisPrincipalHolder.set(principal);

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
  }
}
