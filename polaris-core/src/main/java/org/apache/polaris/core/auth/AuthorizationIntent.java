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

import org.apache.polaris.core.entity.PolarisEntityType;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Authorization intent describing an operation and its target resource shape. */
public sealed interface AuthorizationIntent
    permits TargetlessAuthorizationIntent,
        SingleTargetAuthorizationIntent,
        PairwiseTargetAuthorizationIntent {
  static AuthorizationIntent of(@NonNull PolarisAuthorizableOperation operation) {
    return new TargetlessAuthorizationIntent(operation);
  }

  static AuthorizationIntent of(
      @NonNull PolarisAuthorizableOperation operation, @NonNull PolarisSecurable target) {
    return new SingleTargetAuthorizationIntent(operation, target);
  }

  static AuthorizationIntent of(
      @NonNull PolarisAuthorizableOperation operation,
      @Nullable PolarisSecurable target,
      @Nullable PolarisSecurable secondary) {
    return new PairwiseTargetAuthorizationIntent(operation, target, secondary);
  }

  @NonNull PolarisAuthorizableOperation getOperation();

  boolean hasSecurableType(PolarisEntityType... types);

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
