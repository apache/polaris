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

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.polaris.async.AsyncConfiguration.DEFAULT_THREAD_KEEP_ALIVE;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.async.AsyncConfiguration;
import org.apache.polaris.async.AsyncExec;
import org.apache.polaris.async.Cancelable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDI {@link ApplicationScoped} Java executor service based {@link AsyncExec} implementation using
 * {@link CompletableFuture#delayedExecutor(long, TimeUnit, Executor)} for the timers, backed by a
 * Java thread pool to execute blocking task.
 */
@ApplicationScoped
public class JavaPoolAsyncExec implements AsyncExec {
  private static final Logger LOGGER = LoggerFactory.getLogger(JavaPoolAsyncExec.class.getName());

  private final ThreadPoolExecutor executorService;

  private static final AtomicInteger POOL_ID = new AtomicInteger();
  private final int poolId = POOL_ID.incrementAndGet();
  private final AtomicInteger threadId = new AtomicInteger();
  private volatile boolean shutdown;

  @VisibleForTesting
  public JavaPoolAsyncExec() {
    this(AsyncConfiguration.builder().build());
  }

  @Inject
  JavaPoolAsyncExec(AsyncConfiguration asyncConfiguration) {
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
                      r, "JavaPoolTaskExecutor#" + poolId + "-" + threadId.incrementAndGet());
              t.setDaemon(true);
              return t;
            },
            // rejected execution handler
            (r, executor) -> {
              var msg =
                  format("Runnable '%s' rejected against pool ID %s / '%s'", r, poolId, executor);
              if (r instanceof CancelableFuture<?> cancelable) {
                if (cancelable.completable.isDone()) {
                  LOGGER.debug(msg);
                  return;
                }
              }
              var ex = new RejectedExecutionException(msg);
              LOGGER.error(msg);
              throw ex;
            });
    executorService.allowCoreThreadTimeOut(true);
    LOGGER.debug("JavaPoolAsyncExec initialized with pool ID {}", poolId);
  }

  @PreDestroy
  @VisibleForTesting
  public void shutdown() {
    shutdown = true;
    LOGGER.debug("Shutting down JavaPoolAsyncExec {} / '{}'", poolId, executorService);
    executorService.close();
  }

  @Override
  public <R> Cancelable<R> schedule(Callable<R> callable, Duration delay) {
    checkState(!shutdown, "Must not schedule new tasks after shutdown of pool %s", poolId);

    var cf = new CancelableFuture<>(callable);

    CompletableFuture.delayedExecutor(Math.max(delay.toMillis(), 0L), MILLISECONDS, executorService)
        .execute(cf);

    return cf;
  }

  @Override
  public Cancelable<Void> schedulePeriodic(
      Runnable runnable, Duration initialDelay, Duration delay) {
    checkState(!shutdown, "Must not schedule new tasks after shutdown of pool %s", poolId);

    var initialMillis = initialDelay.toMillis();
    var delayMillis = Math.max(delay.toMillis(), 1L);

    var cf = new CancelableFuture<Void>(runnable, delayMillis);

    if (initialMillis > 0) {
      CompletableFuture.delayedExecutor(initialMillis, MILLISECONDS, executorService).execute(cf);
    } else {
      cf.immediately();
    }

    return cf;
  }

  // Helper function (instrumented by CDI) to run within the application scope
  <R> R withinApplicationContext(Callable<R> callable) throws Exception {
    return callable.call();
  }

  @Override
  public String toString() {
    return "JavaAsyncExec with pool ID " + poolId + " backed by " + executorService;
  }

  private final class CancelableFuture<R> implements Cancelable<R>, Runnable {
    private final CompletableFuture<R> completable = new CompletableFuture<>();
    private final Runnable runnable;
    private final Callable<R> callable;
    private final long repeat;

    CancelableFuture(Runnable runnable, long repeat) {
      this.runnable = runnable;
      this.callable = this::runnable;
      this.repeat = repeat;
    }

    CancelableFuture(Callable<R> callable) {
      this.runnable = null;
      this.callable = callable;
      this.repeat = -1L;
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

    @SuppressWarnings("FutureReturnValueIgnored")
    void immediately() {
      executorService.submit(this);
    }

    @Override
    public void run() {
      if (cancelledOrShutdown()) {
        completable.cancel(false);
        return;
      }
      try {
        var r = withinApplicationContext(callable);
        if (repeat >= 0L) {
          if (!cancelledOrShutdown()) {
            CompletableFuture.delayedExecutor(repeat, MILLISECONDS, executorService).execute(this);
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
}
