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
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Authorization request inputs for pre-authorization and core authorization.
 *
 * <p>This wrapper keeps authorization inputs together and conveys the intent to be authorized via
 * {@link AuthorizationTargetBinding} target bindings.
 */
@PolarisImmutable
public interface AuthorizationRequest {
  static AuthorizationRequest of(
      @Nonnull PolarisPrincipal principal,
      @Nonnull PolarisAuthorizableOperation operation,
      @Nonnull List<AuthorizationTargetBinding> targetBindings) {
    return ImmutableAuthorizationRequest.builder()
        .principal(principal)
        .operation(operation)
        .targetBindings(targetBindings)
        .build();
  }

  /** Returns the principal requesting authorization. */
  @Nonnull
  PolarisPrincipal getPrincipal();

  /** Returns the operation being authorized. */
  @Nonnull
  PolarisAuthorizableOperation getOperation();

  /** Returns the target/secondary target bindings. */
  @Nonnull
  List<AuthorizationTargetBinding> getTargetBindings();

  /**
   * Returns the primary target securables, if any.
   *
   * <p>Compatibility accessor derived from {@link #getTargetBindings()}.
   */
  @Nonnull
  default List<PolarisSecurable> getTargets() {
    return getTargetBindings().stream().map(AuthorizationTargetBinding::getTarget).toList();
  }

  /**
   * Returns secondary securables, if any.
   *
   * <p>Compatibility accessor derived from {@link #getTargetBindings()}.
   */
  @Nonnull
  default List<PolarisSecurable> getSecondaries() {
    List<PolarisSecurable> secondaries = new ArrayList<>();
    for (AuthorizationTargetBinding targetBinding : getTargetBindings()) {
      if (targetBinding.getSecondary() != null) {
        secondaries.add(targetBinding.getSecondary());
      }
    }
    return secondaries;
  }

  default boolean hasSecurableType(PolarisEntityType... types) {
    for (AuthorizationTargetBinding targetBinding : getTargetBindings()) {
      if (containsType(targetBinding.getTarget(), types)) {
        return true;
      }
      if (targetBinding.getSecondary() != null
          && containsType(targetBinding.getSecondary(), types)) {
        return true;
      }
    }
    return false;
  }

  static boolean containsType(PolarisSecurable securable, PolarisEntityType... types) {
    PolarisEntityType entityType = securable.getEntityType();
    for (PolarisEntityType type : types) {
      if (entityType == type) {
        return true;
      }
    }
    return false;
  }
}
