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
import org.apache.polaris.core.entity.PolarisEntityType;

/** Authorization intent describing an operation and its target resource shape. */
public sealed interface AuthorizationIntent
    permits TargetlessAuthorizationIntent,
        SingleTargetAuthorizationIntent,
        PairwiseTargetAuthorizationIntent {
  static AuthorizationIntent of(@Nonnull PolarisAuthorizableOperation operation) {
    return new TargetlessAuthorizationIntent(operation);
  }

  static AuthorizationIntent of(
      @Nonnull PolarisAuthorizableOperation operation, @Nonnull PolarisSecurable target) {
    return new SingleTargetAuthorizationIntent(operation, target);
  }

  static AuthorizationIntent of(
      @Nonnull PolarisAuthorizableOperation operation,
      @Nullable PolarisSecurable target,
      @Nullable PolarisSecurable secondary) {
    return new PairwiseTargetAuthorizationIntent(operation, target, secondary);
  }

  @Nonnull
  PolarisAuthorizableOperation getOperation();

  @Nullable
  PolarisSecurable getTarget();

  @Nullable
  PolarisSecurable getSecondary();

  default boolean hasSecurableType(PolarisEntityType... types) {
    if (getTarget() != null && containsType(getTarget(), types)) {
      return true;
    }
    if (getSecondary() != null && containsType(getSecondary(), types)) {
      return true;
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
