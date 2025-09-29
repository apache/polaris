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
package org.apache.polaris.nosql.async.java;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.polaris.nosql.async.AsyncConfiguration.DEFAULT_MAX_THREADS;
import static org.apache.polaris.nosql.async.AsyncConfiguration.DEFAULT_THREAD_KEEP_ALIVE;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.nosql.async.AsyncConfiguration;
import org.apache.polaris.nosql.async.AsyncExec;
import org.apache.polaris.nosql.async.Cancelable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * CDI {@link ApplicationScoped} Java executor service based {@link AsyncExec} implementation using
 * {@link ScheduledThreadPoolExecutor} for the timers, backed by a Java thread pool to execute
 * blocking tasks.
 */
@ApplicationScoped
public class JavaPoolAsyncExec implements AsyncExec, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(JavaPoolAsyncExec.class.getName());
  private static final Duration MAX_DURATION = Duration.ofDays(7);

  public static final String EXECUTOR_THREAD_NAME_PREFIX = "JavaPoolTaskExecutor#";
  public static final String SCHEDULER_THREAD_NAME_PREFIX = "JavaPoolTaskScheduler#";

  private final ThreadPoolExecutor executorService;
  private final ScheduledThreadPoolExecutor scheduler;

  /** Pool-ID generator, useful for debugging when multiple pools are created for multiple tests. */
  private static final AtomicInteger POOL_ID = new AtomicInteger();

  /** Pool-ID, useful for debugging when multiple pools are created for multiple tests. */
  private final int poolId = POOL_ID.incrementAndGet();

  private final AtomicInteger executorThreadId = new AtomicInteger();
  private final AtomicInteger schedulerThreadId = new AtomicInteger();
  private volatile boolean shutdown;
  // Track live tasks for prompt cancellation on shutdown
  private final Set<CancelableFuture<?>> tasks = ConcurrentHashMap.newKeySet();

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
          Future<?> future = null;
          if (r instanceof CancelableFuture<?> cancelable) {
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
        };
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
    scheduler.setRemoveOnCancelPolicy(true);
    LOGGER.debug("JavaPoolAsyncExec initialized with pool ID {}", poolId);
  }

  private String rejectedMessage(Runnable r, ThreadPoolExecutor executor) {
    return format("Runnable '%s' rejected against pool ID %s / '%s'", r, poolId, executor);
  }

  @PreDestroy
  @VisibleForTesting
  @Override
  public void close() {
    shutdown = true;
    LOGGER.debug("Shutting down JavaPoolAsyncExec {} / '{}'", poolId, executorService);
    try {
      // Proactively cancel all tasks and timers
      tasks.forEach(CancelableFuture::cancel);
      tasks.clear();

      var remaining = scheduler.shutdownNow();
      LOGGER.debug("Scheduler shut down, {} dangling tasks", remaining.size());
      scheduler.awaitTermination(10, TimeUnit.SECONDS);
      scheduler.close();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    } finally {
      executorService.shutdownNow();
      try {
        executorService.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
      executorService.close();
    }
  }

  @Override
  public <R> Cancelable<R> schedule(Callable<R> callable, Duration delay) {
    checkState(!shutdown, "Must not schedule new tasks after shutdown of pool %s", poolId);
    checkArgument(delay.compareTo(MAX_DURATION) < 0, "Delay is limited to %s", MAX_DURATION);

    var delayMillis = Math.max(delay.toMillis(), 0L);
    var cf = new CancelableFuture<>(callable);
    tasks.add(cf);

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
    checkArgument(
        initialDelay.compareTo(MAX_DURATION) < 0, "Initial delay is limited to %s", MAX_DURATION);
    checkArgument(delay.compareTo(MAX_DURATION) < 0, "Delay is limited to %s", MAX_DURATION);

    var initialMillis = initialDelay.toMillis();
    var delayMillis = Math.max(delay.toMillis(), 1L);

    var cf = new CancelableFuture<Void>(runnable, delayMillis);
    tasks.add(cf);

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
    cancelable.setScheduledFuture(
        scheduler.schedule(() -> immediate(cancelable), delayMillis, MILLISECONDS));
  }

  private final class CancelableFuture<R> implements Cancelable<R>, Runnable {
    private final CompletableFuture<R> completable = new CompletableFuture<>();
    private final Runnable runnable;
    private final Callable<R> callable;
    private final long repeatMillis;
    private final AtomicReference<ScheduledFuture<?>> scheduledFuture = new AtomicReference<>();

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

    void setScheduledFuture(ScheduledFuture<?> scheduledFuture) {
      var previous = this.scheduledFuture.getAndSet(scheduledFuture);
      if (previous != null) {
        previous.cancel(false);
      }
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
        tasks.remove(this);
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
          // One-shot tasks can be released
          tasks.remove(this);
        }
      } catch (Throwable t) {
        completable.completeExceptionally(new CompletionException(t));
        tasks.remove(this);
      } finally {
        // Avoid ThreadLocal/MDC leakage across logically independent tasks
        MDC.clear();
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
      var previous = this.scheduledFuture.getAndSet(null);
      if (previous != null) {
        previous.cancel(false);
      }
      completable.cancel(false);
      tasks.remove(this);
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
