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
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;

/**
 * Interface for invoking authorization checks.
 *
 * <p>An implementations of this interface is selected at startup time. Implementations should
 * declare a constrictor with a single argument of type {@link PolarisConfigurationStore}.
 */
public interface PolarisAuthorizer {

  /**
   * Validates whether the requested operation is permitted based on the collection of entities
   * (including principals, roles, and catalog objects) that are affected by the operation.
   *
   * <p>"activated" entities, "targets" and "secondaries" are contained within the provided
   * manifest. The extra selector parameters merely define what sub-set of objects from the manifest
   * should be considered as "targets", etc.
   *
   * <p>The effective principal information is also provided in the manifest.
   *
   * @param manifest defines the input for authorization checks.
   * @param operation the operation being authorized.
   * @param activatedEntities selector for roles that should be considered by the authorization
   *     checks
   * @param targets target entities affected by the operation (e.g. tables)
   * @param secondaries secondary entities affected by the operation (e.g. grantee)
   */
  void authorizeOrThrow(
      @Nonnull PolarisResolutionManifest manifest,
      @Nonnull PolarisAuthorizableOperation operation,
      @Nonnull ActivatedEntitySelector activatedEntities,
      @Nonnull List<AuthEntitySelector> targets,
      @Nonnull List<AuthEntitySelector> secondaries);

  /**
   * Convenience redirect for {@link #authorizeOrThrow(PolarisResolutionManifest,
   * PolarisAuthorizableOperation, ActivatedEntitySelector, List, List)}
   */
  default void authorizeOrThrow(
      @Nonnull PolarisResolutionManifest manifest,
      @Nonnull PolarisAuthorizableOperation operation,
      @Nonnull ActivatedEntitySelector activatedEntities,
      @Nullable AuthEntitySelector target) {
    authorizeOrThrow(manifest, operation, activatedEntities, target, null);
  }

  /**
   * Convenience redirect for {@link #authorizeOrThrow(PolarisResolutionManifest,
   * PolarisAuthorizableOperation, ActivatedEntitySelector, List, List)}
   */
  default void authorizeOrThrow(
      @Nonnull PolarisResolutionManifest manifest,
      @Nonnull PolarisAuthorizableOperation operation,
      @Nonnull ActivatedEntitySelector activatedEntities,
      @Nullable AuthEntitySelector target,
      @Nullable AuthEntitySelector secondary) {
    authorizeOrThrow(
        manifest,
        operation,
        activatedEntities,
        target == null ? List.of() : List.of(target),
        secondary == null ? List.of() : List.of(secondary));
  }
}
