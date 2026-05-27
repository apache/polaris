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

import com.google.common.base.Preconditions;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Authorization intent for operations that may carry both a primary target and a related secondary
 * target.
 *
 * <p>The primary target may be omitted for legacy root-scoped flows that rely on an implicit root
 * primary plus an explicit secondary target.
 */
public record PairwiseTargetAuthorizationIntent(
    @NonNull PolarisAuthorizableOperation operation,
    @Nullable PolarisSecurable target,
    @Nullable PolarisSecurable secondary)
    implements AuthorizationIntent {
  public PairwiseTargetAuthorizationIntent {
    Preconditions.checkNotNull(operation, "operation must be non-null");
    Preconditions.checkState(
        target != null || secondary != null,
        "PairwiseTargetAuthorizationIntent must contain a target or secondary");
  }

  @Override
  public @NonNull PolarisAuthorizableOperation getOperation() {
    return operation;
  }

  @Override
  public boolean hasSecurableType(PolarisEntityType type) {
    return (target() != null && target().leafHasType(type))
        || (secondary() != null && secondary().leafHasType(type));
  }
}
