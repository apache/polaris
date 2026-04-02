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
package org.apache.polaris.service.task;

import jakarta.annotation.Nonnull;

/**
 * Extension point for propagating request-scoped context across the async task boundary.
 *
 * <p>Each implementation is responsible for a single piece of request-scoped context (e.g. realm
 * identity, authenticated principal, request ID). Implementations are CDI beans (typically
 * {@code @ApplicationScoped}). {@link TaskExecutorImpl} discovers all implementations via CDI
 * {@code Instance} injection, so adding a new propagation concern requires only a new bean — no
 * existing code needs to change.
 *
 * <p>Lifecycle:
 *
 * <ol>
 *   <li>{@link #capture()} is called on the request thread (active request scope). The
 *       implementation reads its relevant context and returns a {@link RestoreAction} that
 *       encapsulates the captured state and knows how to restore it.
 *   <li>The action is carried across the async boundary.
 *   <li>{@link RestoreAction#restore()} is called inside the task thread's new CDI request scope.
 *   <li>{@link RestoreAction#close()} is called after the task finishes, for optional cleanup (e.g.
 *       MDC restoration). The default implementation is a no-op.
 * </ol>
 */
public interface AsyncContextPropagator {

  /**
   * Captures relevant context from the current request scope.
   *
   * <p>The returned action may be restored multiple times across retries and different threads.
   * Implementations must ensure the captured state within the action is <strong>immutable</strong>
   * and <strong>thread-safe</strong>.
   *
   * @return an action that can restore the captured context, or {@link RestoreAction#NOOP} if no
   *     context is available to capture.
   */
  @Nonnull
  RestoreAction capture();

  /**
   * Encapsulates captured context and the logic to restore it on a task thread.
   *
   * <p>Implementations that need cleanup after task completion (e.g. MDC restoration) override
   * {@link #close()}. The default {@code close()} is a no-op, so propagators with no cleanup
   * requirement need not implement it.
   */
  interface RestoreAction extends AutoCloseable {

    /** Shared no-op instance for propagators that have nothing to capture. */
    RestoreAction NOOP = () -> {};

    /** Restores the captured context into the task thread's active request scope. */
    void restore();

    /** Optional cleanup after the task finishes. Default is a no-op. */
    @Override
    default void close() throws Exception {}
  }
}
