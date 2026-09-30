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

package org.apache.polaris.service.it;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.BasicResolutionSemantics;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.jspecify.annotations.NonNull;

@ApplicationScoped
@Identifier("test")
public class ExampleNonRBACAuthorizer implements PolarisAuthorizer {
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
    BasicResolutionSemantics.resolveSelections(authzState, request);
    authzState.resolve();
  }

  @Override
  public @NonNull AuthorizationDecision authorize(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request) {
    return request.intents().stream()
        .map(intent -> decisions.get(intent.operation()))
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(AuthorizationDecision.ALLOW);
  }
}
