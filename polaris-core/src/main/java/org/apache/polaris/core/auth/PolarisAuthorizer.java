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
      @Nonnull AuthorizationState authzState, @Nonnull AuthorizationRequest request);

  /**
   * Core authorization entry point for the new SPI.
   *
   * <p>Implementations should rely on any required state in {@link AuthorizationState} and the
   * intent captured by {@link AuthorizationRequest} (principal, operation, and target securables).
   */
  @Nonnull
  AuthorizationDecision authorizeDecision(
      @Nonnull AuthorizationState authzState, @Nonnull AuthorizationRequest request);

  /**
   * Convenience method that throws a {@link ForbiddenException} when authorization is denied.
   *
   * <p>Implementations should provide allow/deny decisions via {@link #authorizeDecision}.
   */
  default void authorizeOrThrow(
      @Nonnull AuthorizationState authzState, @Nonnull AuthorizationRequest request) {
    AuthorizationDecision decision = authorizeDecision(authzState, request);
    if (!decision.isAllowed()) {
      String message = decision.getMessageOrDefault("Authorization denied");
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
