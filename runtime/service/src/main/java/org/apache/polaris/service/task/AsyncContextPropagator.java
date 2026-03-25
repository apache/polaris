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
import jakarta.annotation.Nullable;

/**
 * Extension point for propagating request-scoped context across the async task boundary.
 *
 * <p>Each implementation is responsible for a single piece of request-scoped context (e.g. realm
 * identity, authenticated principal, request ID). Implementations are CDI beans (typically {@code
 * @ApplicationScoped}). {@link TaskExecutorImpl} discovers all implementations via CDI {@code
 * Instance} injection, so adding a new propagation concern requires only a new bean — no existing
 * code needs to change.
 *
 * <p>Lifecycle:
 *
 * <ol>
 *   <li>{@link #capture()} is called on the request thread (active request scope). The
 *       implementation reads its relevant context and returns an opaque snapshot.
 *   <li>The snapshot is carried across the async boundary together with the propagator that created
 *       it.
 *   <li>{@link #restore(Object)} is called inside the task thread's new CDI request scope. The
 *       implementation re-establishes its context from the snapshot and returns an {@link
 *       AutoCloseable} for cleanup after the task finishes (e.g. MDC restoration).
 * </ol>
 */
public interface AsyncContextPropagator {

  /**
   * Captures relevant context from the current request scope.
   *
   * <p>The returned snapshot may be restored multiple times across retries and different threads.
   * Implementations must ensure the captured state is <strong>immutable</strong> and
   * <strong>thread-safe</strong>.
   *
   * @return an opaque snapshot that will be passed to {@link #restore(Object)} in the task thread,
   *     or {@code null} if no context is available to capture.
   */
  @Nullable
  Object capture();

  /**
   * Restores the captured context into the task thread's active request scope.
   *
   * @param capturedState the snapshot returned by {@link #capture()}, may be {@code null}.
   * @return an {@link AutoCloseable} that is closed after the task finishes. Implementations that
   *     need no cleanup must return a no-op ({@code () -> {}}). Must not return {@code null}.
   */
  @Nonnull
  AutoCloseable restore(@Nullable Object capturedState);
}
