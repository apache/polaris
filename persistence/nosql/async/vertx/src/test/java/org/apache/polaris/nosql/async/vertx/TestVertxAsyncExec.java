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
package org.apache.polaris.nosql.async.vertx;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.vertx.core.Vertx;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.nosql.async.AsyncConfiguration;
import org.apache.polaris.nosql.async.AsyncExecTestBase;
import org.apache.polaris.nosql.async.Cancelable;
import org.junit.jupiter.api.Test;

public class TestVertxAsyncExec extends AsyncExecTestBase {
  @Inject Vertx vertx;
  @Inject AsyncConfiguration asyncConfiguration;

  @Override
  protected void threadAssertion() {
    var t = Thread.currentThread();
    soft.assertThat(t.getName()).startsWith(VertxAsyncExec.EXECUTOR_THREAD_NAME_PREFIX);
  }

  @Test
  public void periodicTaskDoesNotSubmitAgainWhenPreviousRunIsPending() throws Exception {
    var submissions = new AtomicInteger();
    var runHooks = new AtomicInteger();
    var firstRunStarted = new CountDownLatch(1);
    var releaseFirstRun = new CountDownLatch(1);
    var secondSubmission = new CountDownLatch(1);

    try (var executor =
        new VertxAsyncExec(vertx, asyncConfiguration) {
          @Override
          void taskSubmittedHook(Future<?> future) {
            if (submissions.incrementAndGet() == 2) {
              secondSubmission.countDown();
            }
          }

          @Override
          void taskRunHook() {
            if (runHooks.incrementAndGet() == 1) {
              firstRunStarted.countDown();
              try {
                assertThat(releaseFirstRun.await(5, SECONDS)).isTrue();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
            }
          }
        }) {
      var periodic =
          executor.schedulePeriodic(() -> {}, Duration.ofMillis(1), Duration.ofMillis(1));

      assertThat(firstRunStarted.await(5, SECONDS)).isTrue();
      assertThat(secondSubmission.await(100, MILLISECONDS)).isFalse();

      releaseFirstRun.countDown();
      periodic.cancel();
    }
  }

  @Test
  public void immediateTaskIsTrackedBeforeExecution() {
    var tracked = new AtomicReference<Cancelable<?>>();

    try (var executor =
        new VertxAsyncExec(vertx, asyncConfiguration) {
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
  public void delayedTaskCanBeCanceledBeforeTimerStarts() throws Exception {
    var ran = new CountDownLatch(1);
    var canceled = new AtomicInteger();

    try (var executor =
        new VertxAsyncExec(vertx, asyncConfiguration) {
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
    }
  }

  @Test
  public void immediateTaskRetriesWhenThreadPoolIsSaturated() throws Exception {
    var started = new CountDownLatch(1);
    var release = new Semaphore(0);

    try (var executor =
        new VertxAsyncExec(vertx, AsyncConfiguration.builder().maxThreads(1).build())) {
      var blocker =
          executor.submit(
              () -> {
                started.countDown();
                assertThat(release.tryAcquire(5, SECONDS)).isTrue();
                return null;
              });

      assertThat(started.await(5, SECONDS)).isTrue();
      var delayed = executor.submit(() -> "delayed").completionStage().toCompletableFuture();
      assertThat(delayed).isNotDone();

      release.release();
      assertThat(blocker.completionStage()).succeedsWithin(Duration.ofSeconds(5));
      assertThat(delayed).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("delayed");
    }
  }

  @Test
  public void delayedTaskRetriesWhenThreadPoolIsSaturated() throws Exception {
    var started = new CountDownLatch(1);
    var release = new Semaphore(0);
    var rejected = new CountDownLatch(1);

    try (var executor =
        new VertxAsyncExec(vertx, AsyncConfiguration.builder().maxThreads(1).build()) {
          @Override
          void taskSubmissionRejectedHook(Cancelable<?> cancelable) {
            rejected.countDown();
          }
        }) {
      var blocker = executor.submit(blockingTask(started, release));

      await(started);
      var delayed =
          executor
              .schedule(() -> "delayed", Duration.ofMillis(1))
              .completionStage()
              .toCompletableFuture();

      await(rejected);
      assertThat(delayed).isNotDone();

      release.release();
      assertThat(blocker.completionStage()).succeedsWithin(Duration.ofSeconds(5));
      assertThat(delayed).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("delayed");
    }
  }

  @Test
  public void periodicTaskRetriesWhenThreadPoolIsSaturated() throws Exception {
    var started = new CountDownLatch(1);
    var release = new Semaphore(0);
    var rejected = new CountDownLatch(1);
    var ran = new CountDownLatch(1);

    try (var executor =
        new VertxAsyncExec(vertx, AsyncConfiguration.builder().maxThreads(1).build()) {
          @Override
          void taskSubmissionRejectedHook(Cancelable<?> cancelable) {
            rejected.countDown();
          }
        }) {
      var blocker = executor.submit(blockingTask(started, release));

      await(started);
      var periodic =
          executor.schedulePeriodic(ran::countDown, Duration.ofMillis(1), Duration.ofSeconds(60));

      await(rejected);
      assertThat(ran.await(100, MILLISECONDS)).isFalse();

      release.release();
      assertThat(blocker.completionStage()).succeedsWithin(Duration.ofSeconds(5));
      await(ran);
      periodic.cancel();
    }
  }

  @Test
  public void taskCanBeCanceledWhileSubmissionRetryIsPending() throws Exception {
    var started = new CountDownLatch(1);
    var release = new Semaphore(0);
    var rejected = new CountDownLatch(1);
    var ran = new AtomicBoolean();

    try (var executor =
        new VertxAsyncExec(vertx, AsyncConfiguration.builder().maxThreads(1).build()) {
          @Override
          void taskSubmissionRejectedHook(Cancelable<?> cancelable) {
            rejected.countDown();
          }
        }) {
      var blocker = executor.submit(blockingTask(started, release));

      await(started);
      var task =
          executor.submit(
              () -> {
                ran.set(true);
                return "unexpected";
              });

      await(rejected);
      task.cancel();
      assertThat(task.completionStage().toCompletableFuture()).isCancelled();
      assertThat(ran).isFalse();

      release.release();
      assertThat(blocker.completionStage()).succeedsWithin(Duration.ofSeconds(5));
      assertThat(ran).isFalse();
    }
  }

  @Test
  public void taskRetriesAfterRepeatedRejections() throws Exception {
    var started = new CountDownLatch(1);
    var release = new Semaphore(0);
    var rejectedTwice = new CountDownLatch(2);

    try (var executor =
        new VertxAsyncExec(vertx, AsyncConfiguration.builder().maxThreads(1).build()) {
          @Override
          void taskSubmissionRejectedHook(Cancelable<?> cancelable) {
            rejectedTwice.countDown();
          }
        }) {
      var blocker = executor.submit(blockingTask(started, release));

      await(started);
      var task = executor.submit(() -> "retried").completionStage().toCompletableFuture();

      await(rejectedTwice);
      assertThat(task).isNotDone();

      release.release();
      assertThat(blocker.completionStage()).succeedsWithin(Duration.ofSeconds(5));
      assertThat(task).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("retried");
    }
  }

  @Test
  public void closeCancelsTrackedTasksAndRejectsLaterSchedules() throws Exception {
    var started = new CountDownLatch(1);
    var release = new Semaphore(0);
    var rejected = new CountDownLatch(1);
    var ran = new AtomicBoolean();

    var executor =
        new VertxAsyncExec(vertx, AsyncConfiguration.builder().maxThreads(1).build()) {
          @Override
          void taskSubmissionRejectedHook(Cancelable<?> cancelable) {
            rejected.countDown();
          }
        };

    var blocker = executor.submit(blockingTask(started, release));
    await(started);
    var task =
        executor.submit(
            () -> {
              ran.set(true);
              return "unexpected";
            });
    await(rejected);

    executor.close();

    assertThat(task.completionStage().toCompletableFuture()).isCancelled();
    assertThat(ran).isFalse();
    assertThat(blocker.completionStage().toCompletableFuture()).isDone();
    assertThatThrownBy(() -> executor.submit(() -> "after-close"))
        .isInstanceOf(IllegalStateException.class);
  }

  private static Callable<Void> blockingTask(CountDownLatch started, Semaphore release) {
    return () -> {
      started.countDown();
      try {
        release.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return null;
    };
  }

  private static void await(CountDownLatch latch) throws InterruptedException {
    assertThat(latch.await(5_000, MILLISECONDS)).isTrue();
  }
}
