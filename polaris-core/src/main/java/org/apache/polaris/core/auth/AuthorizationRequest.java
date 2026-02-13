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
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Authorization request inputs for pre-authorization and core authorization.
 *
 * <p>This wrapper keeps authorization inputs together and conveys the intent to be authorized via
 * {@link PolarisSecurable} targets and secondaries.
 */
@PolarisImmutable
public interface AuthorizationRequest {
  static AuthorizationRequest of(
      @Nonnull PolarisPrincipal principal,
      @Nonnull PolarisAuthorizableOperation operation,
      @Nullable List<PolarisSecurable> targets,
      @Nullable List<PolarisSecurable> secondaries) {
    return ImmutableAuthorizationRequest.builder()
        .principal(principal)
        .operation(operation)
        .targets(targets)
        .secondaries(secondaries)
        .build();
  }

  /** Returns the principal requesting authorization. */
  @Nonnull
  PolarisPrincipal getPrincipal();

  /** Returns the operation being authorized. */
  @Nonnull
  PolarisAuthorizableOperation getOperation();

  /** Returns the primary target securables, if any. */
  @Nullable
  List<PolarisSecurable> getTargets();

  /** Returns the secondary securables, if any. */
  @Nullable
  List<PolarisSecurable> getSecondaries();

  /** Returns true if the request targets principal entities. */
  default boolean hasPrincipalTarget() {
    return hasSecurableType(PolarisEntityType.PRINCIPAL);
  }

  /** Returns true if the request targets principal role entities. */
  default boolean hasPrincipalRoleTarget() {
    return hasSecurableType(PolarisEntityType.PRINCIPAL_ROLE);
  }

  /** Returns true if the request targets catalog role entities. */
  default boolean hasCatalogRoleTarget() {
    return hasSecurableType(PolarisEntityType.CATALOG_ROLE);
  }

  private boolean hasSecurableType(PolarisEntityType... types) {
    if (getTargets() != null && containsType(getTargets(), types)) {
      return true;
    }
    return getSecondaries() != null && containsType(getSecondaries(), types);
  }

  private static boolean containsType(
      List<PolarisSecurable> securables, PolarisEntityType... types) {
    for (PolarisSecurable securable : securables) {
      PolarisEntityType entityType = securable.getEntityType();
      for (PolarisEntityType type : types) {
        if (entityType == type) {
          return true;
        }
      }
    }
    return false;
  }

}
