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

import static java.util.concurrent.Executors.callable;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.concurrent.Callable;

/**
 * Abstraction for platform/environment-specific scheduler implementations for delayed and
 * optionally repeated executions.
 *
 * <p>Quarkus production systems use Vert.x, tests usually use Java executors.
 *
 * <p>Implementations, like Java executors or Vert.X, are usually {@link
 * ApplicationScoped @ApplicationScoped} in CDI environments.
 */
public interface AsyncExec {

  default <R> Cancelable<R> submit(Callable<R> callable) {
    return schedule(callable, Duration.ZERO);
  }

  /**
   * Asynchronously run the given {@linkplain Callable callable} after the provided {@linkplain
   * Duration delay}. If the delay is not positive, the function is scheduled for immediate
   * execution.
   *
   * @param callable the callable to execute
   * @param delay the execution delay, zero and negative values mean immediate scheduling
   * @param <R> return type of the callable propagated through the returned cancelable
   * @return the cancelable for the scheduled task
   */
  <R> Cancelable<R> schedule(Callable<R> callable, Duration delay);

  /**
   * This is a convenience function for {@link #schedule(Callable, Duration)} with a void result,
   * using a {@link Runnable}.
   */
  default Cancelable<Void> schedule(Runnable runnable, Duration delay) {
    return schedule(callable(runnable, null), delay);
  }

  /**
   * Schedules a runnable to be executed repeatedly using the given initial delay. This is
   * equivalent to calling {@link #schedulePeriodic(Runnable, Duration, Duration)} with the {@code
   * initialDelay} and {@code delay} having the same values.
   *
   * <p>There is intentionally no variant of {@code schedulePeriodic()} that takes a {@link Callable
   * Callable<R>} because there are multiple invocations of the runnable.
   */
  default Cancelable<Void> schedulePeriodic(Runnable runnable, Duration delay) {
    return schedulePeriodic(runnable, delay, delay);
  }

  /**
   * Schedules a runnable to be executed repeatedly, starting after the given initial delay.
   *
   * <p>There is intentionally no variant of {@code schedulePeriodic()} that takes a {@link Callable
   * Callable<R>} because there are multiple invocations of the runnable.
   *
   * @param runnable the runnable to execute
   * @param initialDelay initial delay, zero and negative values mean immediate scheduling
   * @param delay repetition delay, zero and negative cause an {@link IllegalArgumentException}
   * @return cancelable instance
   */
  Cancelable<Void> schedulePeriodic(Runnable runnable, Duration initialDelay, Duration delay);
}
