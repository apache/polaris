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
package org.apache.polaris.async.java;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.polaris.async.AsyncConfiguration.DEFAULT_THREAD_KEEP_ALIVE;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.async.AsyncConfiguration;
import org.apache.polaris.async.AsyncExec;
import org.apache.polaris.async.Cancelable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDI {@link ApplicationScoped} Java executor service based {@link AsyncExec} implementation using
 * {@link ScheduledThreadPoolExecutor} for the timers, backed by a Java thread pool to execute
 * blocking task.
 */
@ApplicationScoped
public class JavaPoolAsyncExec implements AsyncExec, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(JavaPoolAsyncExec.class.getName());
  public static final String EXECUTOR_THREAD_NAME_PREFIX = "JavaPoolTaskExecutor#";
  public static final String SCHEDULER_THREAD_NAME_PREFIX = "JavaPoolTaskScheduler#";

  private final ThreadPoolExecutor executorService;
  private final ScheduledThreadPoolExecutor scheduler;

  private static final AtomicInteger POOL_ID = new AtomicInteger();
  private final int poolId = POOL_ID.incrementAndGet();
  private final AtomicInteger executorThreadId = new AtomicInteger();
  private final AtomicInteger schedulerThreadId = new AtomicInteger();
  private volatile boolean shutdown;

  @VisibleForTesting
  public JavaPoolAsyncExec() {
    this(AsyncConfiguration.builder().build());
  }

  @Inject
  JavaPoolAsyncExec(Instance<AsyncConfiguration> asyncConfiguration) {
    this(
        asyncConfiguration.isResolvable()
            ? asyncConfiguration.get()
            : AsyncConfiguration.builder().build());
  }

  JavaPoolAsyncExec(AsyncConfiguration asyncConfiguration) {
    RejectedExecutionHandler rejectedExecutionHandler =
        (r, executor) -> {
          var msg = format("Runnable '%s' rejected against pool ID %s / '%s'", r, poolId, executor);
          Future<?> future = null;
          if (r instanceof CancelableFuture<?> cancelable) {
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
        };
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
            rejectedExecutionHandler);
    executorService.allowCoreThreadTimeOut(true);
    scheduler =
        new ScheduledThreadPoolExecutor(
            2,
            r -> {
              var t =
                  new Thread(
                      r,
                      SCHEDULER_THREAD_NAME_PREFIX
                          + poolId
                          + "-"
                          + schedulerThreadId.incrementAndGet());
              t.setDaemon(true);
              return t;
            },
            rejectedExecutionHandler);
    LOGGER.debug("JavaPoolAsyncExec initialized with pool ID {}", poolId);
  }

  @PreDestroy
  @VisibleForTesting
  @Override
  public void close() {
    shutdown = true;
    LOGGER.debug("Shutting down JavaPoolAsyncExec {} / '{}'", poolId, executorService);
    try {
      var remaining = scheduler.shutdownNow();
      LOGGER.debug("Scheduler shut down, {} dangling tasks", remaining.size());
      scheduler.close();
    } finally {
      executorService.close();
    }
  }

  @Override
  public <R> Cancelable<R> schedule(Callable<R> callable, Duration delay) {
    checkState(!shutdown, "Must not schedule new tasks after shutdown of pool %s", poolId);

    var delayMillis = Math.max(delay.toMillis(), 0L);

    var cf = new CancelableFuture<>(callable);

    if (delayMillis > 0) {
      delayed(cf, delayMillis);
    } else {
      immediate(cf);
    }

    return cf;
  }

  @Override
  public Cancelable<Void> schedulePeriodic(
      Runnable runnable, Duration initialDelay, Duration delay) {
    checkState(!shutdown, "Must not schedule new tasks after shutdown of pool %s", poolId);
    checkArgument(delay.isPositive(), "Delay must not be zero or negative");

    var initialMillis = initialDelay.toMillis();
    var delayMillis = Math.max(delay.toMillis(), 1L);

    var cf = new CancelableFuture<Void>(runnable, delayMillis);

    if (initialMillis > 0) {
      delayed(cf, initialMillis);
    } else {
      immediate(cf);
    }

    return cf;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void immediate(CancelableFuture<?> cancelable) {
    executorService.submit(cancelable);
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void delayed(CancelableFuture<?> cancelable, long delayMillis) {
    scheduler.schedule(() -> immediate(cancelable), delayMillis, MILLISECONDS);
  }

  private final class CancelableFuture<R> implements Cancelable<R>, Runnable {
    private final CompletableFuture<R> completable = new CompletableFuture<>();
    private final Runnable runnable;
    private final Callable<R> callable;
    private final long repeatMillis;

    CancelableFuture(Runnable runnable, long repeatMillis) {
      this.runnable = requireNonNull(runnable, "Runnable must not be null");
      this.callable = this::runnable;
      this.repeatMillis = repeatMillis;
    }

    CancelableFuture(Callable<R> callable) {
      this.runnable = null;
      this.callable = requireNonNull(callable, "Callable must not be null");
      this.repeatMillis = -1L;
    }

    @SuppressWarnings("DataFlowIssue")
    private R runnable() {
      if (cancelledOrShutdown()) {
        completable.cancel(false);
      } else {
        runnable.run();
      }
      return null;
    }

    @Override
    public void run() {
      if (cancelledOrShutdown()) {
        completable.cancel(false);
        return;
      }
      try {
        var r = callable.call();
        if (repeatMillis >= 0L) {
          if (!cancelledOrShutdown()) {
            delayed(this, repeatMillis);
          }
        } else {
          completable.complete(r);
        }
      } catch (Throwable t) {
        completable.completeExceptionally(new CompletionException(t));
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
      completable.cancel(false);
    }

    @Override
    public String toString() {
      if (runnable != null) {
        return "JavaPoolAsyncExec.Completable: Runnable " + runnable;
      } else {
        return "JavaPoolAsyncExec.Completable: Callable " + callable;
      }
    }
  }

  @Override
  public String toString() {
    return "JavaAsyncExec with pool ID " + poolId + " backed by " + executorService;
  }
}
