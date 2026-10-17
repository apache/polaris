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
package org.apache.polaris.core.auth;

import java.util.List;
import org.jspecify.annotations.NonNull;

/** Interface for invoking authorization checks. */
public interface PolarisAuthorizer {
  /**
   * Resolve authorizer-specific inputs before authorization.
   *
   * <p>Implementations may resolve only the entities required for the request (for example, the
   * caller principal, principal roles, catalog roles, and requested targets) and store that state
   * in {@link AuthorizationState}.
   *
   * <p>This method should not perform authorization decisions directly.
   */
  void resolveAuthorizationInputs(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request);

  /**
   * Batch counterpart to {@link #resolveAuthorizationInputs(AuthorizationState,
   * AuthorizationRequest)}, invoked once before {@link #authorize(AuthorizationState,
   * AuthorizationBatchRequest)} for intents that share one {@link AuthorizationState}.
   *
   * <p>Implementations may narrow what they resolve, exactly as for the single-request method, and
   * must resolve the shared state at most once. {@link #authorize(AuthorizationState,
   * AuthorizationBatchRequest)} may subsequently be called with a subset of {@code batch}'s
   * intents.
   *
   * <p>The default implementation delegates once to the single-request method with every intent of
   * {@code batch}, in order, so any narrowing done there applies to the batch as well. Overriding
   * is optional and only needed for batch-specific preparation.
   */
  default void resolveAuthorizationInputs(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationBatchRequest batch) {
    if (batch.intents().isEmpty()) {
      return;
    }
    resolveAuthorizationInputs(
        authzState, new AuthorizationRequest(batch.principal(), batch.intents()));
  }

  /**
   * Core authorization entry point for the new SPI.
   *
   * <p>Implementations should rely on any required state in {@link AuthorizationState} and the
   * request captured by {@link AuthorizationRequest}.
   *
   * <p>When a request contains multiple intents, they form a single batch contract: implementations
   * must AND-combine the intents in that request and may short-circuit evaluation on the first
   * deny.
   */
  @NonNull AuthorizationDecision authorize(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request);

  /**
   * Batch authorization entry point.
   *
   * <p>The default implementation evaluates each intent independently by delegating to {@link
   * #authorize(AuthorizationState, AuthorizationRequest)} with a single-intent request.
   * Implementations may override this method for improved performance.
   *
   * <p>The returned list preserves the same size and order as {@code batch.intents()}. Each intent
   * is evaluated independently. Implementations must return a decision for every intent and must
   * not short-circuit across intents, even when an earlier intent is denied.
   *
   * <p>Note that this differs from the contract of a single {@link AuthorizationRequest}, where the
   * intents are AND-combined and evaluation may short-circuit on the first deny.
   */
  default @NonNull List<AuthorizationDecision> authorize(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationBatchRequest batch) {
    return batch.intents().stream()
        .map(
            intent ->
                authorize(authzState, new AuthorizationRequest(batch.principal(), List.of(intent))))
        .toList();
  }
}
