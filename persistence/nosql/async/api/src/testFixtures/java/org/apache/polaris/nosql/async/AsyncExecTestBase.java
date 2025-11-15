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
package org.apache.polaris.nosql.async;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.assertj.core.api.CompletableFutureAssert;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jboss.weld.junit5.EnableWeld;
import org.jboss.weld.junit5.WeldInitiator;
import org.jboss.weld.junit5.WeldSetup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@SuppressWarnings("CdiInjectionPointsInspection")
@EnableWeld
@ExtendWith(SoftAssertionsExtension.class)
public abstract class AsyncExecTestBase {
  @InjectSoftAssertions protected SoftAssertions soft;

  @WeldSetup WeldInitiator weld = WeldInitiator.performDefaultDiscovery();

  @Inject protected AsyncExec executor;

  protected final Duration asyncTimeout = Duration.ofMinutes(5);

  @Inject AppScopedChecker appScopedChecker;

  @Test
  public void simpleTests() throws Exception {
    soft.assertThat(executor.submit(() -> "foo").completionStage())
        .succeedsWithin(asyncTimeout)
        .isEqualTo("foo");
    soft.assertThat(executor.schedule(() -> "foo", Duration.ZERO).completionStage())
        .succeedsWithin(asyncTimeout)
        .isEqualTo("foo");
    soft.assertThat(executor.schedule(() -> "foo", Duration.ofMillis(1)).completionStage())
        .succeedsWithin(asyncTimeout)
        .isEqualTo("foo");

    var done = new AtomicBoolean();
    soft.assertThat(executor.schedule(() -> done.set(true), Duration.ZERO).completionStage())
        .succeedsWithin(asyncTimeout);
    soft.assertThat(done.get()).isTrue();
    done.set(false);
    soft.assertThat(executor.schedule(() -> done.set(true), Duration.ofMillis(1)).completionStage())
        .succeedsWithin(asyncTimeout);
    soft.assertThat(done.get()).isTrue();

    // .cancel() after first execution
    for (var initialDelay : List.of(Duration.ZERO, Duration.ofMillis(1))) {
      var sem = new Semaphore(0);
      var cancelable = executor.schedulePeriodic(sem::release, initialDelay, Duration.ofMillis(1));
      soft.assertThat(sem.tryAcquire(asyncTimeout.toMillis(), MILLISECONDS))
          .describedAs("initialDelay %s", initialDelay)
          .isTrue();
      cancelable.cancel();
      cancelledAssert(soft.assertThat(cancelable.completionStage()));
    }

    // .cancel() before execution
    var cancelable = executor.schedulePeriodic(() -> {}, Duration.ofHours(12));
    cancelable.cancel();
    cancelledAssert(soft.assertThat(cancelable.completionStage()));

    cancelable = executor.schedule(() -> {}, Duration.ofHours(12));
    cancelable.cancel();
    cancelledAssert(soft.assertThat(cancelable.completionStage()));
  }

  private void cancelledAssert(CompletableFutureAssert<?> assertion) {
    assertion.satisfiesAnyOf(
        completionStage ->
            assertThat(completionStage)
                .failsWithin(asyncTimeout)
                .withThrowableThat()
                .isInstanceOf(CancellationException.class),
        completionStage -> assertThat(completionStage).succeedsWithin(asyncTimeout));
  }

  @Test
  public void applicationScopedInvocation() {
    var expect = AppScopedChecker.COUNTER.get();
    soft.assertThat(executor.submit(() -> appScopedChecker.getAndIncrement()).completionStage())
        .succeedsWithin(asyncTimeout)
        .isEqualTo(expect);
  }

  @Test
  public void submitMany() {
    int numTasks = 50;
    var sem = new Semaphore(0);
    var completableFutures =
        IntStream.range(0, numTasks)
            .mapToObj(
                i ->
                    executor.submit(
                        () -> {
                          soft.assertThat(sem.tryAcquire(asyncTimeout.toMillis(), MILLISECONDS))
                              .isTrue();
                          threadAssertion();
                          return "foo";
                        }))
            .map(Cancelable::completionStage)
            .map(CompletionStage::toCompletableFuture)
            .toList();

    soft.assertThat(completableFutures).noneMatch(CompletableFuture::isDone);

    sem.release(numTasks);

    soft.assertThat(completableFutures)
        .allSatisfy(cf -> assertThat(cf).succeedsWithin(asyncTimeout).isEqualTo("foo"));
  }

  @Test
  public void submitManyFailing() {
    int numTasks = 50;
    var sem = new Semaphore(0);
    var completableFutures =
        IntStream.range(0, numTasks)
            .mapToObj(
                i ->
                    executor.submit(
                        () -> {
                          soft.assertThat(sem.tryAcquire(asyncTimeout.toMillis(), MILLISECONDS))
                              .isTrue();
                          threadAssertion();
                          throw new RuntimeException("FAILED");
                        }))
            .map(Cancelable::completionStage)
            .map(CompletionStage::toCompletableFuture)
            .toList();

    soft.assertThat(completableFutures).noneMatch(CompletableFuture::isDone);

    sem.release(numTasks);

    soft.assertThat(completableFutures)
        .allSatisfy(
            cf ->
                assertThat(cf)
                    .failsWithin(asyncTimeout)
                    .withThrowableThat()
                    .isInstanceOf(ExecutionException.class)
                    .havingCause()
                    .isInstanceOf(RuntimeException.class)
                    .withMessage("FAILED"));
  }

  @Test
  public void scheduleMany() {
    var numTasks = 50;
    var sem = new Semaphore(0);
    var completableFutures =
        IntStream.range(0, numTasks)
            .mapToObj(
                i ->
                    executor.schedule(
                        () -> {
                          soft.assertThat(sem.tryAcquire(asyncTimeout.toMillis(), MILLISECONDS))
                              .isTrue();
                          threadAssertion();
                          return "foo";
                        },
                        Duration.ofMillis(1)))
            .map(Cancelable::completionStage)
            .map(CompletionStage::toCompletableFuture)
            .toList();

    soft.assertThat(completableFutures).noneMatch(CompletableFuture::isDone);

    sem.release(numTasks);

    soft.assertThat(completableFutures)
        .allSatisfy(cf -> assertThat(cf).succeedsWithin(asyncTimeout).isEqualTo("foo"));
  }

  @SuppressWarnings({"InnerClassMayBeStatic", "ClassCanBeStatic"})
  final class PeriodicPerTask {
    final Semaphore before;
    final Semaphore after;
    final AtomicInteger runs;
    volatile Cancelable<Void> c;
    volatile CompletableFuture<Void> f;

    PeriodicPerTask() {
      before = new Semaphore(0);
      after = new Semaphore(0);
      runs = new AtomicInteger(0);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {Integer.MAX_VALUE, 0, 1, 3, 5})
  public void periodic(int failAfter) {
    var dontFail = failAfter == Integer.MAX_VALUE;
    var numTasks = 50;
    var stop = new AtomicBoolean();
    var completableFutures =
        IntStream.range(0, numTasks)
            .mapToObj(
                i -> {
                  var task = new PeriodicPerTask();
                  task.c =
                      executor.schedulePeriodic(
                          () -> {
                            threadAssertion();
                            try {
                              soft.assertThat(
                                      task.before.tryAcquire(asyncTimeout.toMillis(), MILLISECONDS))
                                  .isTrue();
                            } catch (InterruptedException e) {
                              throw new RuntimeException(e);
                            }
                            try {
                              if (stop.get()) {
                                return;
                              }
                              var iter = task.runs.getAndIncrement();
                              if (iter == failAfter) {
                                throw new RuntimeException("FAILED");
                              }
                            } finally {
                              task.after.release(1);
                            }
                          },
                          Duration.ofMillis(1));
                  task.f = task.c.completionStage().toCompletableFuture();
                  return task;
                })
            .toList();

    var iterations = dontFail ? 10 : (failAfter + 1);
    for (int i = 0; i < iterations; i++) {
      soft.assertThat(completableFutures).noneMatch(c -> c.f.isDone());

      completableFutures.forEach(t -> t.before.release());

      assertThat(completableFutures)
          .describedAs("iteration %d", i)
          .allSatisfy(
              t -> assertThat(t.after.tryAcquire(asyncTimeout.toMillis(), MILLISECONDS)).isTrue());
    }

    stop.set(true);

    if (dontFail) {
      completableFutures.forEach(t -> t.c.cancel());
    }

    soft.assertThat(completableFutures).allMatch(t -> t.runs.get() == iterations);

    // Just in case the periodic tasks get called again, let those run
    completableFutures.forEach(s -> s.before.release(1000));

    soft.assertThat(completableFutures)
        .allSatisfy(
            t -> {
              if (dontFail) {
                cancelledAssert(assertThat(t.f));
              } else {
                assertThat(t.f)
                    .completesExceptionallyWithin(asyncTimeout)
                    .withThrowableThat()
                    .isInstanceOf(ExecutionException.class)
                    .havingCause()
                    .isInstanceOf(RuntimeException.class)
                    .withMessage("FAILED");
              }
            });

    soft.assertThat(completableFutures).allMatch(t -> t.f.isDone());
    if (dontFail) {
      soft.assertThat(completableFutures).allMatch(t -> t.f.isCancelled());
    } else {
      soft.assertThat(completableFutures).noneMatch(t -> t.f.isCancelled());
    }
    // cancellation is still an exceptional completion
    soft.assertThat(completableFutures).allMatch(t -> t.f.isCompletedExceptionally());
  }

  protected abstract void threadAssertion();
}
