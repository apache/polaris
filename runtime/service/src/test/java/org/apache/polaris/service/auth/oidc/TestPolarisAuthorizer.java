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
package org.apache.polaris.service.auth.oidc;

import java.util.Set;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.persistence.resolver.Resolvable;
import org.jspecify.annotations.NonNull;

/**
 * A minimal authorizer used only in tests. It mirrors the shape of a real external authorizer: it
 * resolves the manifest with minimal selections, ignores Polaris grants, and decides purely from
 * the principal identity.
 *
 * <p>By default, a principal whose name starts with {@value #DENY_PREFIX} or who has any role
 * starting with {@value #DENY_PREFIX} is denied; all others are allowed.
 */
public class TestPolarisAuthorizer implements PolarisAuthorizer {

  public static final String DENY_PREFIX = "denied";

  @Override
  public void resolveAuthorizationInputs(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request) {
    // nothing to resolve for this oidc authorizer, but the manifest must be in resolved state to
    // avoid errors in the authorization decision phase.
    authzState
        .getResolutionManifest()
        .resolveSelections(Set.of(Resolvable.REQUESTED_TOP_LEVEL_ENTITIES));
  }

  @Override
  @NonNull
  public AuthorizationDecision authorize(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request) {
    return isAllowed(request.principal())
        ? AuthorizationDecision.allow()
        : AuthorizationDecision.deny(
            "Test authorizer denied principal " + request.principal().getName());
  }

  protected boolean isAllowed(PolarisPrincipal principal) {
    return principal.getName() != null
        && !principal.getName().startsWith(DENY_PREFIX)
        && principal.getRoles().stream().noneMatch(role -> role.startsWith(DENY_PREFIX));
  }
}
