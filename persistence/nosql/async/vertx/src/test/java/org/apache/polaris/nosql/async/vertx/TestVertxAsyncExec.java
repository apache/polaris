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

import io.vertx.core.Vertx;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
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
  public void immediateTaskFailsWhenThreadPoolIsSaturated() throws Exception {
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
      var rejected = executor.submit(() -> "rejected").completionStage().toCompletableFuture();
      assertThat(rejected)
          .failsWithin(Duration.ofSeconds(5))
          .withThrowableThat()
          .isInstanceOf(ExecutionException.class)
          .havingCause()
          .isInstanceOf(RejectedExecutionException.class);

      release.release();
      assertThat(blocker.completionStage()).succeedsWithin(Duration.ofSeconds(5));
    }
  }
}
