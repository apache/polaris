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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.nosql.async.AsyncConfiguration;
import org.apache.polaris.nosql.async.AsyncExecTestBase;
import org.apache.polaris.nosql.async.Cancelable;
import org.junit.jupiter.api.Test;

public class TestJavaPoolAsyncExec extends AsyncExecTestBase {
  @Override
  protected void threadAssertion() {
    var t = Thread.currentThread();
    soft.assertThat(t.getName()).startsWith(JavaPoolAsyncExec.EXECUTOR_THREAD_NAME_PREFIX);
  }

  /**
   * The Java pool implementation could race in its timer bookkeeping. The issue was that {@code
   * JavaPoolAsyncExec.delayed()} could install an already-fired ScheduledFuture after a newer
   * periodic timer has been installed, then cancel the newer one.
   */
  @Test
  public void periodicTaskContinuesWhenPreviousTimerIsRecordedAfterNextTimer() {
    var scheduledHooks = new AtomicInteger();
    var secondTimerRecorded = new CountDownLatch(1);
    var secondTimer = new AtomicReference<ScheduledFuture<?>>();

    try (var executor =
        new JavaPoolAsyncExec(AsyncConfiguration.builder().build()) {
          @Override
          void delayedTaskScheduledHook(ScheduledFuture<?> scheduledFuture) {
            if (scheduledHooks.incrementAndGet() == 1) {
              try {
                assertThat(secondTimerRecorded.await(5_000, MILLISECONDS)).isTrue();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
            }
          }

          @Override
          void delayedTaskRecordedHook(ScheduledFuture<?> scheduledFuture) {
            if (scheduledHooks.get() >= 2 && secondTimer.compareAndSet(null, scheduledFuture)) {
              secondTimerRecorded.countDown();
            }
          }
        }) {
      var periodic =
          executor.schedulePeriodic(() -> {}, Duration.ofMillis(1), Duration.ofSeconds(60));

      assertThat(secondTimer)
          .hasValueSatisfying(timer -> assertThat(timer.isCancelled()).isFalse());
      periodic.cancel();
    }
  }

  @Test
  public void immediateTaskIsTrackedBeforeExecution() {
    var tracked = new AtomicReference<Cancelable<?>>();

    try (var executor =
        new JavaPoolAsyncExec(AsyncConfiguration.builder().build()) {
          @Override
          void taskTrackedHook(Cancelable<?> cancelable) {
            tracked.set(cancelable);
          }
        }) {
      var task =
          executor.submit(
              () -> {
                assertThat(tracked).hasValueSatisfying(t -> assertThat(t).isNotNull());
                return "done";
              });

      assertThat(task.completionStage()).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("done");
    }
  }

  @Test
  public void delayedTaskCanBeCanceledBeforeTimerIsRecorded() {
    var ran = new CountDownLatch(1);
    var canceled = new AtomicInteger();

    try (var executor =
        new JavaPoolAsyncExec(AsyncConfiguration.builder().build()) {
          @Override
          void taskTrackedHook(Cancelable<?> cancelable) {
            if (canceled.incrementAndGet() == 1) {
              cancelable.cancel();
            }
          }
        }) {
      var task = executor.schedule(ran::countDown, Duration.ofMillis(1));

      assertThat(ran.await(100, MILLISECONDS)).isFalse();
      assertThat(task.completionStage().toCompletableFuture()).isCancelled();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Test
  public void immediateTaskRejectedWhenThreadPoolIsSaturated() throws Exception {
    var started = new CountDownLatch(1);
    var release = new Semaphore(0);

    try (var executor = new JavaPoolAsyncExec(AsyncConfiguration.builder().maxThreads(1).build())) {
      var blocker =
          executor.submit(
              () -> {
                started.countDown();
                assertThat(release.tryAcquire(5_000, MILLISECONDS)).isTrue();
                return null;
              });

      assertThat(started.await(5_000, MILLISECONDS)).isTrue();
      assertThatThrownBy(() -> executor.submit(() -> "rejected"))
          .isInstanceOf(RejectedExecutionException.class);

      release.release();
      assertThat(blocker.completionStage()).succeedsWithin(Duration.ofSeconds(5));
    }
  }
}
