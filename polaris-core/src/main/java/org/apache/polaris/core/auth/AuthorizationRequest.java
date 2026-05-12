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

/**
 * Authorization request inputs for pre-authorization and core authorization.
 *
 * <p>This hierarchy makes the target shape explicit on the request itself while preserving the
 * normalized compatibility accessors used by current authorizer implementations.
 */
public sealed interface AuthorizationRequest
    permits UntargetedAuthorizationRequest,
        SingleTargetAuthorizationRequest,
        PairwiseTargetAuthorizationRequest {
  static AuthorizationRequest of(@Nonnull PolarisAuthorizableOperation operation) {
    return new UntargetedAuthorizationRequest(operation);
  }

  static AuthorizationRequest of(
      @Nonnull PolarisAuthorizableOperation operation, @Nonnull PolarisSecurable target) {
    return new SingleTargetAuthorizationRequest(operation, target);
  }

  static AuthorizationRequest of(
      @Nonnull PolarisAuthorizableOperation operation,
      @Nullable PolarisSecurable target,
      @Nullable PolarisSecurable secondary) {
    return new PairwiseTargetAuthorizationRequest(operation, target, secondary);
  }

  /** Returns the operation being authorized. */
  @Nonnull
  PolarisAuthorizableOperation getOperation();

  /** Returns the primary target securables, if any. */
  @Nonnull
  List<PolarisSecurable> getTargets();

  /** Returns secondary securables, if any. */
  @Nonnull
  List<PolarisSecurable> getSecondaries();

  default boolean hasSecurableType(PolarisEntityType... types) {
    for (PolarisSecurable target : getTargets()) {
      if (containsType(target, types)) {
        return true;
      }
    }
    for (PolarisSecurable secondary : getSecondaries()) {
      if (containsType(secondary, types)) {
        return true;
      }
    }
    return false;
  }

  static boolean containsType(PolarisSecurable securable, PolarisEntityType... types) {
    PolarisEntityType entityType = securable.getLeaf().entityType();
    for (PolarisEntityType type : types) {
      if (entityType == type) {
        return true;
      }
    }
    return false;
  }
}
