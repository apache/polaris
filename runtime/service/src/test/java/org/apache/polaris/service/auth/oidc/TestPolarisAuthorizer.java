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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.NonRBACResolutionSemantics;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
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

  private final Map<PolarisAuthorizableOperation, AuthorizationDecision> decisions =
      new ConcurrentHashMap<>();

  public void setDecision(PolarisAuthorizableOperation op, AuthorizationDecision decision) {
    decisions.put(op, decision);
  }

  public void reset() {
    decisions.clear();
  }

  @Override
  public void resolveAuthorizationInputs(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request) {
    NonRBACResolutionSemantics.resolveSelections(authzState, request);
    authzState.resolve();
  }

  @Override
  @NonNull
  public AuthorizationDecision authorize(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request) {
    if (!isAllowed(request.principal())) {
      return AuthorizationDecision.deny(
          "Test authorizer denied principal " + request.principal().getName());
    }

    return request.intents().stream()
        .map(intent -> decisions.get(intent.operation()))
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(AuthorizationDecision.ALLOW);
  }

  protected boolean isAllowed(PolarisPrincipal principal) {
    return principal.getName() != null
        && !principal.getName().startsWith(DENY_PREFIX)
        && principal.getRoles().stream().noneMatch(role -> role.startsWith(DENY_PREFIX));
  }
}
