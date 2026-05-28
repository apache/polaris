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
import java.util.Set;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

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
   * Core authorization entry point for the new SPI.
   *
   * <p>Implementations should rely on any required state in {@link AuthorizationState} and the
   * intent captured by {@link AuthorizationRequest} (principal, operation, and target securables).
   */
  @NonNull AuthorizationDecision authorize(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request);

  /**
   * Convenience method that throws a {@link ForbiddenException} when authorization is denied.
   *
   * <p>Implementations should provide allow/deny decisions via {@link #authorize}.
   */
  default void authorizeOrThrow(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request) {
    AuthorizationDecision decision = authorize(authzState, request);
    if (!decision.isAllowed()) {
      String message = decision.getMessage().orElse("Authorization denied");
      throw new ForbiddenException("%s", message);
    }
  }

  void authorizeOrThrow(
      @NonNull PolarisPrincipal polarisPrincipal,
      @NonNull Set<PolarisBaseEntity> activatedEntities,
      @NonNull PolarisAuthorizableOperation authzOp,
      @Nullable PolarisResolvedPathWrapper target,
      @Nullable PolarisResolvedPathWrapper secondary);

  void authorizeOrThrow(
      @NonNull PolarisPrincipal polarisPrincipal,
      @NonNull Set<PolarisBaseEntity> activatedEntities,
      @NonNull PolarisAuthorizableOperation authzOp,
      @Nullable List<PolarisResolvedPathWrapper> targets,
      @Nullable List<PolarisResolvedPathWrapper> secondaries);
}
