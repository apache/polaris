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

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.polaris.async.AsyncConfiguration.DEFAULT_THREAD_KEEP_ALIVE;

import io.vertx.core.Vertx;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.async.AsyncConfiguration;
import org.apache.polaris.async.AsyncExec;
import org.apache.polaris.async.Cancelable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDI {@link ApplicationScoped} {@link AsyncExec} implementation that uses Vert.X timers, backed by
 * a Java thread pool to execute blocking tasks.
 */
@ApplicationScoped
class VertxAsyncExec implements AsyncExec, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(VertxAsyncExec.class.getName());
  public static final String EXECUTOR_THREAD_NAME_PREFIX = "VertxAsyncExec#";

  private final ThreadPoolExecutor executorService;
  private final Vertx vertx;

  private static final AtomicInteger POOL_ID = new AtomicInteger();
  private final int poolId = POOL_ID.incrementAndGet();
  private final AtomicInteger executorThreadId = new AtomicInteger();
  private volatile boolean shutdown;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  VertxAsyncExec(Vertx vertx, AsyncConfiguration asyncConfiguration) {
    this.vertx = vertx;

    executorService =
        new ThreadPoolExecutor(
            // core pool size
            0,
            // max pool size
            asyncConfiguration.maxThreads().orElse(MAX_VALUE),
            // keep alive time
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
              var msg =
                  format("Runnable '%s' rejected against pool ID %s / '%s'", r, poolId, executor);
              Future<?> future = null;
              if (r instanceof CancelableFuture<?> cancelable) {
                cancelable.cancelTimer();
                future = cancelable.completable;
              } else if (r instanceof Future<?> f) {
                future = f;
              }
              if (future != null && future.isDone()) {
                LOGGER.debug(msg);
                return;
              }
              var ex = new RejectedExecutionException(msg);
              LOGGER.error(msg);
              throw ex;
            });
    executorService.allowCoreThreadTimeOut(true);
    LOGGER.debug("VertxAsyncExec initialized with pool ID {}", poolId);
  }

  @PreDestroy
  @Override
  public void close() {
    shutdown = true;
    LOGGER.debug("Shutting down VertxAsyncExec {} / '{}'", poolId, executorService);
    executorService.close();
  }

  @Override
  public <R> Cancelable<R> schedule(Callable<R> callable, Duration delay) {
    checkState(!shutdown, "Must not schedule new tasks after shutdown of pool %s", poolId);

    var cf = new CancelableFuture<>(callable);
    cf.schedule(delay.toMillis());
    return cf;
  }

  @Override
  public Cancelable<Void> schedulePeriodic(
      Runnable runnable, Duration initialDelay, Duration delay) {
    checkState(!shutdown, "Must not schedule new tasks after shutdown of pool %s", poolId);

    var initialMillis = Math.max(initialDelay.toMillis(), 1L);
    var delayMillis = Math.max(delay.toMillis(), 1L);

    var cf = new CancelableFuture<Void>(runnable);
    cf.periodic(initialMillis, delayMillis);
    return cf;
  }

  private final class CancelableFuture<R> implements Cancelable<R>, Runnable {
    private long timerId;
    private final CompletableFuture<R> completable = new CompletableFuture<>();
    private final Runnable runnable;
    private final Callable<R> callable;
    private boolean periodic;

    CancelableFuture(Runnable runnable) {
      this.runnable = runnable;
      this.callable = this::runnable;
    }

    CancelableFuture(Callable<R> callable) {
      this.runnable = null;
      this.callable = callable;
    }

    @SuppressWarnings("DataFlowIssue")
    private R runnable() {
      runnable.run();
      return null;
    }

    void schedule(long delay) {
      if (delay > 0) {
        timerId = vertx.setTimer(delay, this::execAsync);
      } else {
        execAsync(-1L);
      }
    }

    void periodic(long initial, long repeat) {
      timerId = vertx.setPeriodic(initial, repeat, this::execAsync);
      periodic = true;
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    void execAsync(long unused) {
      if (cancelledOrShutdown()) {
        completable.cancel(false);
        return;
      }
      executorService.submit(this);
    }

    @Override
    public void run() {
      if (cancelledOrShutdown()) {
        completable.cancel(false);
        cancelTimer();
        return;
      }
      try {
        var r = callable.call();
        if (!periodic) {
          completable.complete(r);
        }
      } catch (Exception e) {
        completable.completeExceptionally(e);
        cancelTimer();
      }
    }

    private void cancelTimer() {
      if (periodic) {
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
      completable.completeExceptionally(new CancellationException());
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
