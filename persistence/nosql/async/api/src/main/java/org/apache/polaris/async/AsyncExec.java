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

import static java.util.concurrent.Executors.callable;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.concurrent.Callable;

/**
 * Abstraction for platform/environment specific scheduler implementations.
 *
 * <p>Quarkus production systems use Vert.x, tests usually use Java executors.
 *
 * <p>Implementations, like Java executors or Vert.X, are {@link
 * ApplicationScoped @ApplicationScoped}. There's also a CDI decorator to propagate the thread
 * context.
 */
public interface AsyncExec {

  default <R> Cancelable<R> submit(Callable<R> callable) {
    return schedule(callable, Duration.ZERO);
  }

  /**
   * Asynchronously run the given {@linkplain Callable callable} after the provided {@linkplain
   * Duration delay}. If the delay is not positive, the function is scheduled for immediate
   * execution.
   */
  <R> Cancelable<R> schedule(Callable<R> callable, Duration delay);

  default Cancelable<Void> schedule(Runnable runnable, Duration delay) {
    return schedule(callable(runnable, null), delay);
  }

  default Cancelable<Void> schedulePeriodic(Runnable runnable, Duration delay) {
    return schedulePeriodic(runnable, delay, delay);
  }

  Cancelable<Void> schedulePeriodic(Runnable callable, Duration initialDelay, Duration delay);
}
