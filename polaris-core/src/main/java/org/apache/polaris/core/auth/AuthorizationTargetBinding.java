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
import org.apache.polaris.immutables.PolarisImmutable;

/** A resource binding containing a primary target and optional secondary. */
@PolarisImmutable
public interface AuthorizationTargetBinding {
  static AuthorizationTargetBinding of(
      @Nonnull PolarisSecurable target, @Nullable PolarisSecurable secondary) {
    return ImmutableAuthorizationTargetBinding.builder()
        .target(target)
        .secondary(secondary)
        .build();
  }

  /** Returns the primary target securable for the binding. */
  @Nonnull
  PolarisSecurable getTarget();

  /**
   * Returns the optional secondary securable associated with the target.
   *
   * <p>Secondaries are related resources needed to evaluate the authorization decision but are not
   * the direct object of the operation. Examples in current Polaris authorization flows include:
   *
   * <ul>
   *   <li>Table rename: the destination namespace (target is the source table).
   *   <li>Role grants: the grantee role/principal (target may be the role or the resource being
   *       granted on).
   *   <li>Policy attach/detach: the catalog/namespace/table being attached to (target is the
   *       policy).
   * </ul>
   */
  @Nullable
  PolarisSecurable getSecondary();
}
