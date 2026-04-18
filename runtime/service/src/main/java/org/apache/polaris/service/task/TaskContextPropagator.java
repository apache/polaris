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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.auth.ImmutablePolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.apache.polaris.service.context.catalog.RequestIdHolder;

/**
 * Captures request-scoped context on the calling thread and restores it inside a task thread's CDI
 * request scope.
 *
 * <p>This follows the same pattern as {@link org.apache.polaris.service.config.Bootstrapper}:
 * context is captured before async submission and explicitly restored after a new CDI request scope
 * is activated on the task thread.
 *
 * <p>{@link #capture()} reads the current realm, principal, and request ID. {@link
 * #restore(CapturedTaskContext)} writes them into the holders of the task thread's fresh request
 * scope.
 */
@ApplicationScoped
class TaskContextPropagator {

  private final RealmContextHolder realmContextHolder;
  private final PolarisPrincipalHolder polarisPrincipalHolder;
  private final RequestIdHolder requestIdHolder;
  private final PolarisPrincipal polarisPrincipal;

  @SuppressWarnings("unused") // Required by CDI
  protected TaskContextPropagator() {
    this(null, null, null, null);
  }

  @Inject
  TaskContextPropagator(
      RealmContextHolder realmContextHolder,
      PolarisPrincipalHolder polarisPrincipalHolder,
      RequestIdHolder requestIdHolder,
      PolarisPrincipal polarisPrincipal) {
    this.realmContextHolder = realmContextHolder;
    this.polarisPrincipalHolder = polarisPrincipalHolder;
    this.requestIdHolder = requestIdHolder;
    this.polarisPrincipal = polarisPrincipal;
  }

  /**
   * Captures the current request-scoped context for later propagation to a task thread.
   *
   * <p>Must be called on a thread with an active CDI request scope (e.g. an HTTP request thread or
   * a task thread that already has context restored). The principal is cloned into an immutable
   * snapshot so the captured context is independent of the originating scope's lifecycle.
   */
  CapturedTaskContext capture() {
    return new CapturedTaskContext(
        realmContextHolder.get(),
        ImmutablePolarisPrincipal.builder().from(polarisPrincipal).build(),
        requestIdHolder.get());
  }

  /**
   * Restores previously captured context into the current (task) thread's request scope.
   *
   * <p>Must be called inside an active CDI request scope on the task thread (e.g. inside a method
   * annotated with {@code @ActivateRequestContext}).
   */
  void restore(CapturedTaskContext context) {
    realmContextHolder.set(context.realmContext());
    polarisPrincipalHolder.set(context.principal());
    requestIdHolder.set(context.requestId());
  }

  /**
   * Immutable snapshot of request-scoped context captured for propagation across async boundaries.
   */
  record CapturedTaskContext(
      RealmContext realmContext, PolarisPrincipal principal, String requestId) {}
}
