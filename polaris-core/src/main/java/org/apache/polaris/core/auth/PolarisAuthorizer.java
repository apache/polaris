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
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;

/** Interface for invoking authorization checks. */
public interface PolarisAuthorizer {
  /**
   * Pre-authorization hook for resolving authorizer-specific inputs.
   *
   * <p>Default implementation is a no-op to preserve legacy behavior.
   */
  default void preAuthorize(
      @Nonnull AuthorizationCallContext ctx, @Nonnull AuthorizationRequest request) {}

  /**
   * Core authorization entry point for the new SPI.
   *
   * <p>Default implementation delegates to legacy {@code authorizeOrThrow(...)} to preserve
   * behavior for existing authorizers.
   */
  default void authorize(
      @Nonnull AuthorizationCallContext ctx, @Nonnull AuthorizationRequest request) {
    authorizeInternal(ctx, request, true /* throwOnDeny */);
  }

  /**
   * Backwards-compatible external API that throws on deny for legacy call sites.
   *
   * <p>Default implementation delegates to shared authorization logic.
   */
  default void authorizeOrThrow(
      @Nonnull AuthorizationCallContext ctx, @Nonnull AuthorizationRequest request) {
    authorizeInternal(ctx, request, true /* throwOnDeny */);
  }

  /**
   * Shared authorization logic used by both new and legacy entry points.
   *
   * <p>Default implementation adapts intent inputs to the legacy RBAC SPI to preserve behavior.
   */
  private void authorizeInternal(
      @Nonnull AuthorizationCallContext ctx,
      @Nonnull AuthorizationRequest request,
      boolean throwOnDeny) {
    PolarisResolutionManifest manifest = ctx.getResolutionManifest();
    Set<PolarisBaseEntity> activatedEntities =
        manifest == null ? Set.of() : manifest.getAllActivatedCatalogRoleAndPrincipalRoles();
    List<PolarisResolvedPathWrapper> resolvedTargets =
        resolveSecurables(manifest, request.getTargets());
    List<PolarisResolvedPathWrapper> resolvedSecondaries =
        resolveSecurables(manifest, request.getSecondaries());
    if (throwOnDeny) {
      authorizeOrThrow(
          request.getPrincipal(),
          activatedEntities,
          request.getOperation(),
          resolvedTargets,
          resolvedSecondaries);
    }
  }

  private static List<PolarisResolvedPathWrapper> resolveSecurables(
      PolarisResolutionManifest manifest, List<PolarisSecurable> securables) {
    if (securables == null) {
      return null;
    }
    if (manifest == null) {
      return null;
    }
    List<PolarisResolvedPathWrapper> resolved = new java.util.ArrayList<>(securables.size());
    for (PolarisSecurable securable : securables) {
      PolarisEntityType type = securable.getEntityType();
      switch (type) {
        case ROOT:
          resolved.add(manifest.getResolvedRootContainerEntityAsPath());
          break;
        case CATALOG:
          if (manifest.hasTopLevelName(securable.getName(), type)) {
            resolved.add(manifest.getResolvedTopLevelEntity(securable.getName(), type));
          } else {
            resolved.add(manifest.getResolvedReferenceCatalogEntity());
          }
          break;
        case PRINCIPAL:
        case PRINCIPAL_ROLE:
          resolved.add(manifest.getResolvedTopLevelEntity(securable.getName(), type));
          break;
        default:
          resolved.add(manifest.getResolvedPath(securable, true));
          break;
      }
    }
    return resolved;
  }

  @Deprecated
  void authorizeOrThrow(
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull Set<PolarisBaseEntity> activatedEntities,
      @Nonnull PolarisAuthorizableOperation authzOp,
      @Nullable PolarisResolvedPathWrapper target,
      @Nullable PolarisResolvedPathWrapper secondary);

  @Deprecated
  void authorizeOrThrow(
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull Set<PolarisBaseEntity> activatedEntities,
      @Nonnull PolarisAuthorizableOperation authzOp,
      @Nullable List<PolarisResolvedPathWrapper> targets,
      @Nullable List<PolarisResolvedPathWrapper> secondaries);
}
