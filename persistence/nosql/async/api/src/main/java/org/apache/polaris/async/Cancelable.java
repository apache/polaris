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

import java.util.concurrent.CompletionStage;

/**
 * Implementation agnostic interface for asynchronous delayed and optionally repeated executions.
 *
 * <p>Implementations may use JVM-local backends, like Java executors or Vert.X. Vert.X's API is not
 * based on Java's {@code (Completable)Future} or {@code CompletionStage}. The cancellation
 * semantics/guarantees are different for Vert.X and Java.
 *
 * @param <R>
 */
public interface Cancelable<R> {
  /**
   * Attempt to cancel the delayed execution of a callable. Already running callables are not
   * interrupted. A callable may still be invoked after calling this function, because of side
   * effects and race conditions.
   *
   * <p>After cancellation, the result of this instance's might be either in state "completed
   * exceptionally" ({@link java.util.concurrent.CancellationException}) or successfully completed.
   */
  void cancel();

  /**
   * Retrieve the {@link CompletionStage} associated with this {@link Cancelable} for the submitted
   * async and potentially periodic execution.
   */
  CompletionStage<R> completionStage();
}
