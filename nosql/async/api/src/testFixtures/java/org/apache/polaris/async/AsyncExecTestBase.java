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
package org.apache.polaris.async;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jboss.weld.junit5.EnableWeld;
import org.jboss.weld.junit5.WeldInitiator;
import org.jboss.weld.junit5.WeldSetup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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

    var sem = new Semaphore(0);
    done.set(false);
    var cancelable = executor.schedulePeriodic(sem::release, Duration.ofMillis(1));
    soft.assertThat(sem.tryAcquire(asyncTimeout.toMillis(), MILLISECONDS)).isTrue();
    cancelable.cancel();
    soft.assertThat(cancelable.completionStage())
        .failsWithin(asyncTimeout)
        .withThrowableThat()
        .isInstanceOf(CancellationException.class);

    cancelable = executor.schedulePeriodic(sem::release, Duration.ZERO, Duration.ofMillis(1));
    soft.assertThat(sem.tryAcquire(asyncTimeout.toMillis(), MILLISECONDS)).isTrue();
    cancelable.cancel();
    soft.assertThat(cancelable.completionStage())
        .failsWithin(asyncTimeout)
        .withThrowableThat()
        .isInstanceOf(CancellationException.class);
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
    var completables =
        IntStream.range(0, numTasks)
            .mapToObj(
                i ->
                    executor.submit(
                        () -> {
                          soft.assertThat(sem.tryAcquire(asyncTimeout.toMillis(), MILLISECONDS))
                              .isTrue();
                          return "foo";
                        }))
            .map(Cancelable::completionStage)
            .map(CompletionStage::toCompletableFuture)
            .toList();

    soft.assertThat(completables).noneMatch(CompletableFuture::isDone);

    sem.release(numTasks);

    soft.assertThat(completables)
        .allSatisfy(cf -> assertThat(cf).succeedsWithin(asyncTimeout).isEqualTo("foo"));
  }

  @Test
  public void scheduleMany() {
    int numTasks = 50;
    var sem = new Semaphore(0);
    var completables =
        IntStream.range(0, numTasks)
            .mapToObj(
                i ->
                    executor.schedule(
                        () -> {
                          soft.assertThat(sem.tryAcquire(asyncTimeout.toMillis(), MILLISECONDS))
                              .isTrue();
                          return "foo";
                        },
                        Duration.ofMillis(5)))
            .map(Cancelable::completionStage)
            .map(CompletionStage::toCompletableFuture)
            .toList();

    soft.assertThat(completables).noneMatch(CompletableFuture::isDone);

    sem.release(numTasks);

    soft.assertThat(completables)
        .allSatisfy(cf -> assertThat(cf).succeedsWithin(asyncTimeout).isEqualTo("foo"));
  }
}
