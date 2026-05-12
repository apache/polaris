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

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;

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
      @Nonnull AuthorizationState authzState,
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull AuthorizationRequest request);

  /**
   * Resolve authorizer-specific inputs for a batch of authorization requests that share one
   * principal.
   *
   * <p>Implementations must define their own batch pre-resolution behavior explicitly because
   * merged manifest registration is authorizer-specific.
   */
  void resolveAuthorizationInputs(
      @Nonnull AuthorizationState authzState,
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull List<AuthorizationRequest> requests);

  /**
   * Core authorization entry point for the new SPI.
   *
   * <p>Implementations should rely on any required state in {@link AuthorizationState} and the
   * intent captured by {@link AuthorizationRequest} (operation and target securables), together
   * with the explicit {@link PolarisPrincipal} argument.
   */
  @Nonnull
  AuthorizationDecision authorize(
      @Nonnull AuthorizationState authzState,
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull AuthorizationRequest request);

  /**
   * Core authorization entry point for a batch of requests that share one principal.
   *
   * <p>The default behavior preserves semantics by evaluating requests independently in order and
   * returning the first denial. Implementations may override this to batch homogeneous requests
   * into a single downstream authorization call.
   */
  @Nonnull
  default AuthorizationDecision authorize(
      @Nonnull AuthorizationState authzState,
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull List<AuthorizationRequest> requests) {
    Preconditions.checkArgument(
        !requests.isEmpty(), "Authorization request batch must contain at least one request");
    for (AuthorizationRequest request : requests) {
      AuthorizationDecision decision = authorize(authzState, polarisPrincipal, request);
      if (!decision.isAllowed()) {
        return decision;
      }
    }
    return AuthorizationDecision.allow();
  }

  /**
   * Convenience method that throws a {@link ForbiddenException} when authorization is denied.
   *
   * <p>Implementations should provide allow/deny decisions via {@link #authorize}.
   */
  default void authorizeOrThrow(
      @Nonnull AuthorizationState authzState,
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull AuthorizationRequest request) {
    AuthorizationDecision decision = authorize(authzState, polarisPrincipal, request);
    if (!decision.isAllowed()) {
      String message = decision.getMessage().orElse("Authorization denied");
      throw new ForbiddenException("%s", message);
    }
  }

  /**
   * Convenience method that throws when any request in the batch is denied.
   *
   * <p>The default behavior delegates to {@link #authorize(AuthorizationState, PolarisPrincipal,
   * List)}.
   */
  default void authorizeOrThrow(
      @Nonnull AuthorizationState authzState,
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull List<AuthorizationRequest> requests) {
    AuthorizationDecision decision = authorize(authzState, polarisPrincipal, requests);
    if (!decision.isAllowed()) {
      String message = decision.getMessage().orElse("Authorization denied");
      throw new ForbiddenException("%s", message);
    }
  }

  void authorizeOrThrow(
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull Set<PolarisBaseEntity> activatedEntities,
      @Nonnull PolarisAuthorizableOperation authzOp,
      @Nullable PolarisResolvedPathWrapper target,
      @Nullable PolarisResolvedPathWrapper secondary);

  void authorizeOrThrow(
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull Set<PolarisBaseEntity> activatedEntities,
      @Nonnull PolarisAuthorizableOperation authzOp,
      @Nullable List<PolarisResolvedPathWrapper> targets,
      @Nullable List<PolarisResolvedPathWrapper> secondaries);
}
