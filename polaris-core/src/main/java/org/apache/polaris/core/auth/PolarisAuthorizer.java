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
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;

/** Interface for invoking authorization checks. */
public interface PolarisAuthorizer {

  /**
   * Whether the implementation expects Polaris principal roles to be present in the {@code
   * activatedEntities} parameters of the {@link #authorizeOrThrow(PolarisPrincipal, Set,
   * PolarisAuthorizableOperation, PolarisResolvedPathWrapper, PolarisResolvedPathWrapper)}
   * functions.
   *
   * <p>If {@code false}, call sites may choose to not pass principal roles.
   */
  default boolean requiresPrincipalRoles() {
    return true;
  }

  /**
   * Whether the implementation expects Polaris catalog roles to be present in the {@code
   * activatedEntities} parameters of the {@link #authorizeOrThrow(PolarisPrincipal, Set,
   * PolarisAuthorizableOperation, PolarisResolvedPathWrapper, PolarisResolvedPathWrapper)}
   * functions.
   *
   * <p>If {@code false}, call sites may choose to not pass catalog roles.
   */
  default boolean requiresCatalogRoles() {
    return true;
  }

  /**
   * Whether the implementation expects the {@link
   * org.apache.polaris.core.persistence.ResolvedPolarisEntity}s in the {@link
   * PolarisResolvedPathWrapper} instances of the {@code target} and {@code secondary} parameters to
   * contain grant records information.
   *
   * <p>If {@code false}, call sites may choose to not pass grant records.
   */
  default boolean requiresResolvedEntities() {
    return true;
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
