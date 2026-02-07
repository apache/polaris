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

/**
 * Authorization request inputs for pre-authorization and core authorization.
 *
 * <p>This wrapper keeps authorization inputs together while preserving legacy semantics.
 */
public final class AuthorizationRequest {
  private final PolarisPrincipal principal;
  private final PolarisAuthorizableOperation operation;
  private final List<PolarisSecurable> targets;
  private final List<PolarisSecurable> secondaries;

  public AuthorizationRequest(
      @Nonnull PolarisPrincipal principal,
      @Nonnull PolarisAuthorizableOperation operation,
      @Nullable List<PolarisSecurable> targets,
      @Nullable List<PolarisSecurable> secondaries) {
    this.principal = principal;
    this.operation = operation;
    this.targets = targets;
    this.secondaries = secondaries;
  }

  public @Nonnull PolarisPrincipal getPrincipal() {
    return principal;
  }

  public @Nonnull PolarisAuthorizableOperation getOperation() {
    return operation;
  }

  public @Nullable List<PolarisSecurable> getTargets() {
    return targets;
  }

  public @Nullable List<PolarisSecurable> getSecondaries() {
    return secondaries;
  }
}
