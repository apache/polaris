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
package org.apache.polaris.async.vertx;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.polaris.async.AsyncConfiguration.DEFAULT_MAX_THREADS;
import static org.apache.polaris.async.AsyncConfiguration.DEFAULT_THREAD_KEEP_ALIVE;

import io.vertx.core.Vertx;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.async.AsyncConfiguration;
import org.apache.polaris.async.AsyncExec;
import org.apache.polaris.async.Cancelable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * CDI {@link ApplicationScoped} {@link AsyncExec} implementation that uses Vert.X timers, backed by
 * a Java thread pool to execute blocking tasks.
 */
@ApplicationScoped
class VertxAsyncExec implements AsyncExec, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(VertxAsyncExec.class.getName());
  private static final Duration MAX_DURATION = Duration.ofDays(7);

  public static final String EXECUTOR_THREAD_NAME_PREFIX = "VertxAsyncExec#";

  private final ThreadPoolExecutor executorService;
  private final Vertx vertx;

  /** Pool-ID generator, useful for debugging when multiple pools are created for multiple tests. */
  private static final AtomicInteger POOL_ID = new AtomicInteger();

  /** Pool-ID, useful for debugging when multiple pools are created for multiple tests. */
  private final int poolId = POOL_ID.incrementAndGet();

  private final AtomicInteger executorThreadId = new AtomicInteger();
  private volatile boolean shutdown;
  // Track live tasks for prompt cancellation on shutdown
  private final Set<CancelableFuture<?>> tasks = ConcurrentHashMap.newKeySet();

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  VertxAsyncExec(Vertx vertx, AsyncConfiguration asyncConfiguration) {
    this.vertx = vertx;

    executorService =
        new ThreadPoolExecutor(
            // core pool size
            0,
            // max pool size
            asyncConfiguration.maxThreads().orElse(DEFAULT_MAX_THREADS),
            // keep-alive time
            asyncConfiguration.threadKeepAlive().orElse(DEFAULT_THREAD_KEEP_ALIVE).toMillis(),
            MILLISECONDS,
            new SynchronousQueue<>(),
            // thread factory
            r -> {
              var t =
                  new Thread(
                      r,
                      EXECUTOR_THREAD_NAME_PREFIX
                          + poolId
                          + "-"
                          + executorThreadId.incrementAndGet());
              t.setDaemon(true);
              return t;
            },
            // rejected execution handler
            (r, executor) -> {
              Future<?> future = null;
              if (r instanceof CancelableFuture<?> cancelable) {
                cancelable.cancelTimer();
                future = cancelable.completable;
              } else if (r instanceof Future<?> f) {
                future = f;
              }
              if (future != null && future.isDone()) {
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(rejectedMessage(r, executor));
                }
                return;
              }
              var msg = rejectedMessage(r, executor);
              var ex = new RejectedExecutionException(msg);
              LOGGER.error(msg);
              throw ex;
            });
    executorService.allowCoreThreadTimeOut(true);
    LOGGER.debug("VertxAsyncExec initialized with pool ID {}", poolId);
  }

  private String rejectedMessage(Runnable r, ThreadPoolExecutor executor) {
    return format("Runnable '%s' rejected against pool ID %s / '%s'", r, poolId, executor);
  }

  @PreDestroy
  @Override
  public void close() {
    shutdown = true;
    LOGGER.debug("Shutting down VertxAsyncExec {} / '{}'", poolId, executorService);
    // Proactively cancel all timers/tasks
    tasks.forEach(CancelableFuture::cancel);
    tasks.clear();
    // Terminate the executor promptly
    executorService.shutdownNow();
    try {
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public <R> Cancelable<R> schedule(Callable<R> callable, Duration delay) {
    checkState(!shutdown, "Must not schedule new tasks after shutdown of pool %s", poolId);
    checkArgument(delay.compareTo(MAX_DURATION) < 0, "Delay is limited to %s", MAX_DURATION);
    var cf = new CancelableFuture<R>(callable, Math.max(delay.toMillis(), 0L));
    tasks.add(cf);
    return cf;
  }

  @Override
  public Cancelable<Void> schedulePeriodic(
      Runnable runnable, Duration initialDelay, Duration delay) {
    checkState(!shutdown, "Must not schedule new tasks after shutdown of pool %s", poolId);
    checkArgument(delay.isPositive(), "Delay must not be zero or negative");
    checkArgument(
        initialDelay.compareTo(MAX_DURATION) < 0, "Initial delay is limited to %s", MAX_DURATION);
    checkArgument(delay.compareTo(MAX_DURATION) < 0, "Delay is limited to %s", MAX_DURATION);

    var cf =
        new CancelableFuture<Void>(
            runnable, Math.max(initialDelay.toMillis(), 0L), Math.max(delay.toMillis(), 1L));
    tasks.add(cf);
    return cf;
  }

  private final class CancelableFuture<R> implements Cancelable<R>, Runnable {
    private final long timerId;

    /** Flag whether {@link #timerId} is valid and the Vert.X timer needs to be canceled. */
    private final boolean hasTimerId;

    private final CompletableFuture<R> completable = new CompletableFuture<>();
    private final Runnable runnable;
    private final Callable<R> callable;
    private final boolean periodic;

    // Prevent overlapping executions for periodic tasks
    private final AtomicBoolean running = new AtomicBoolean(false);
    // Track in-flight submission to allow cancelling/interrupting
    private final AtomicReference<Future<?>> runningFuture = new AtomicReference<>();

    CancelableFuture(Runnable runnable, long initialMillis, long delayMillis) {
      initialMillis = Math.max(initialMillis, 1L);
      delayMillis = Math.max(delayMillis, 1L);
      this.runnable = requireNonNull(runnable, "Runnable must not be null");
      this.callable = this::runnable;
      this.timerId = vertx.setPeriodic(initialMillis, delayMillis, this::execAsync);
      this.hasTimerId = true;
      this.periodic = true;
    }

    CancelableFuture(Callable<R> callable, long delayMillis) {
      this.runnable = null;
      this.callable = requireNonNull(callable, "Callable must not be null");
      this.periodic = false;
      if (delayMillis > 0) {
        this.hasTimerId = true;
        this.timerId = vertx.setTimer(delayMillis, this::execAsync);
      } else {
        // Execute immediately
        this.hasTimerId = false;
        this.timerId = 0L;
        execAsync(-1L);
      }
    }

    private R runnable() {
      runnable.run();
      return null;
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    void execAsync(long unused) {
      if (cancelledOrShutdown()) {
        cancel();
        return;
      }
      // Skip overlapping periodic executions
      if (periodic && running.get()) {
        return;
      }
      try {
        var f = executorService.submit(this);
        runningFuture.set(f);
      } catch (RejectedExecutionException rex) {
        completable.completeExceptionally(rex);
        cancel();
      }
    }

    @Override
    public void run() {
      if (cancelledOrShutdown()) {
        cancel();
        return;
      }
      if (periodic && !running.compareAndSet(false, true)) {
        // Defensive: in case a race got us here while running
        return;
      }
      try {
        var r = callable.call();
        if (!periodic) {
          completable.complete(r);
          // One-shot tasks can be released
          tasks.remove(this);
        }
      } catch (Throwable e) {
        completable.completeExceptionally(e);
        cancelTimer();
        tasks.remove(this);
      } finally {
        runningFuture.set(null);
        if (periodic) {
          running.set(false);
        }
        // Avoid ThreadLocal/MDC leakage across logically independent tasks
        MDC.clear();
      }
    }

    private void cancelTimer() {
      if (hasTimerId) {
        vertx.cancelTimer(timerId);
      }
    }

    private boolean cancelledOrShutdown() {
      return completable.isCancelled() || shutdown;
    }

    @Override
    public CompletionStage<R> completionStage() {
      return completable;
    }

    @Override
    public void cancel() {
      cancelTimer();
      completable.cancel(false);
      var f = runningFuture.getAndSet(null);
      if (f != null && !f.isDone()) {
        f.cancel(true);
      }
      tasks.remove(this);
    }

    @Override
    public String toString() {
      if (runnable != null) {
        return "VertxAsyncExec.Completable: Runnable " + runnable;
      } else {
        return "VertxAsyncExec.Completable: Callable " + callable;
      }
    }
  }

  @Override
  public String toString() {
    return "VertxAsyncExec with pool ID " + poolId + " backed by " + executorService;
  }
}
