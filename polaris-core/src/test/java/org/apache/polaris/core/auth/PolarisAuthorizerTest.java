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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests the default batch {@link PolarisAuthorizer#authorize(AuthorizationState, List)}
 * implementation on the interface itself.
 *
 * <p>This default is the production code path for every authorizer that does not override it, which
 * today is all of them ({@link PolarisAuthorizerImpl} and the Ranger extension). Callers such as
 * the Iceberg catalog handler's LIST filtering depend on its contract: one decision per request, in
 * order, with no short-circuiting across requests.
 */
public class PolarisAuthorizerTest {

  private static final String DENY_MESSAGE = "denied by test authorizer";

  /**
   * Minimal authorizer that does <em>not</em> override the batch method, so the interface default
   * is what runs. It records every single-request delegation so tests can assert how many times,
   * and with what state, the default invoked it.
   */
  private static final class RecordingAuthorizer implements PolarisAuthorizer {

    private final Set<String> deniedLeafNames;
    private final List<AuthorizationRequest> seenRequests = new ArrayList<>();
    private final List<AuthorizationState> seenStates = new ArrayList<>();

    RecordingAuthorizer(Set<String> deniedLeafNames) {
      this.deniedLeafNames = deniedLeafNames;
    }

    @Override
    public void resolveAuthorizationInputs(
        AuthorizationState authzState, AuthorizationRequest request) {}

    @Override
    public AuthorizationDecision authorize(
        AuthorizationState authzState, AuthorizationRequest request) {
      seenRequests.add(request);
      seenStates.add(authzState);
      SingleTargetAuthorizationIntent intent =
          (SingleTargetAuthorizationIntent) request.intents().get(0);
      return deniedLeafNames.contains(intent.target().getLeaf().name())
          ? AuthorizationDecision.deny(DENY_MESSAGE)
          : AuthorizationDecision.allow();
    }

    @Override
    public void authorizeOrThrow(
        PolarisPrincipal polarisPrincipal,
        Set<PolarisBaseEntity> activatedEntities,
        PolarisAuthorizableOperation authzOp,
        @Nullable PolarisResolvedPathWrapper target,
        @Nullable PolarisResolvedPathWrapper secondary) {}

    @Override
    public void authorizeOrThrow(
        PolarisPrincipal polarisPrincipal,
        Set<PolarisBaseEntity> activatedEntities,
        PolarisAuthorizableOperation authzOp,
        @Nullable List<PolarisResolvedPathWrapper> targets,
        @Nullable List<PolarisResolvedPathWrapper> secondaries) {}
  }

  private static final PolarisPrincipal PRINCIPAL =
      PolarisPrincipal.of("alice", Map.of(), Set.of("role1"));

  private static AuthorizationRequest requestForTable(String tableName) {
    PolarisSecurable target =
        PolarisSecurable.of(
            new PathSegment(PolarisEntityType.CATALOG, "myCatalog"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns1"),
            new PathSegment(PolarisEntityType.TABLE_LIKE, tableName));
    return new AuthorizationRequest(
        PRINCIPAL,
        List.of(
            new SingleTargetAuthorizationIntent(PolarisAuthorizableOperation.LIST_TABLES, target)));
  }

  @Test
  void defaultBatchReturnsOneDecisionPerRequestInOrder() {
    RecordingAuthorizer authorizer = new RecordingAuthorizer(Set.of("table2"));
    AuthorizationState state = new AuthorizationState(mock(PolarisResolutionManifest.class));
    List<AuthorizationRequest> requests =
        List.of(requestForTable("table1"), requestForTable("table2"), requestForTable("table3"));

    List<AuthorizationDecision> decisions = authorizer.authorize(state, requests);

    // Size and order must mirror the input exactly: callers index decisions against candidates.
    assertThat(decisions).hasSameSizeAs(requests);
    assertThat(decisions.get(0).isAllowed()).isTrue();
    assertThat(decisions.get(1).isAllowed()).isFalse();
    assertThat(decisions.get(2).isAllowed()).isTrue();

    // The denial is the delegate's own decision, not a synthesized one.
    assertThat(decisions.get(1).getMessage()).contains(DENY_MESSAGE);
  }

  @Test
  void defaultBatchDoesNotShortCircuitWhenAnEarlierRequestIsDenied() {
    // The first request is denied. The contract requires every later request to still be
    // evaluated and to receive its own decision.
    RecordingAuthorizer authorizer = new RecordingAuthorizer(Set.of("table1"));
    AuthorizationState state = new AuthorizationState(mock(PolarisResolutionManifest.class));
    List<AuthorizationRequest> requests =
        List.of(requestForTable("table1"), requestForTable("table2"), requestForTable("table3"));

    List<AuthorizationDecision> decisions = authorizer.authorize(state, requests);

    assertThat(authorizer.seenRequests).hasSize(3);
    assertThat(decisions).hasSize(3);
    assertThat(decisions.get(0).isAllowed()).isFalse();
    assertThat(decisions.get(1).isAllowed()).isTrue();
    assertThat(decisions.get(2).isAllowed()).isTrue();
  }

  @Test
  void defaultBatchEvaluatesEveryRequestExactlyOnceAndInOrder() {
    RecordingAuthorizer authorizer = new RecordingAuthorizer(Set.of());
    AuthorizationState state = new AuthorizationState(mock(PolarisResolutionManifest.class));
    List<AuthorizationRequest> requests =
        List.of(requestForTable("table1"), requestForTable("table2"), requestForTable("table3"));

    authorizer.authorize(state, requests);

    assertThat(authorizer.seenRequests).containsExactlyElementsOf(requests);
    // The shared state is threaded through to every delegation unchanged.
    assertThat(authorizer.seenStates).hasSize(3).allMatch(seen -> seen == state);
  }

  @Test
  void defaultBatchDeniesEveryRequestWhenNoneAreAllowed() {
    RecordingAuthorizer authorizer = new RecordingAuthorizer(Set.of("table1", "table2"));
    AuthorizationState state = new AuthorizationState(mock(PolarisResolutionManifest.class));
    List<AuthorizationRequest> requests =
        List.of(requestForTable("table1"), requestForTable("table2"));

    List<AuthorizationDecision> decisions = authorizer.authorize(state, requests);

    assertThat(decisions).hasSize(2).noneMatch(AuthorizationDecision::isAllowed);
  }

  @Test
  void defaultBatchWithNoRequestsReturnsEmptyListWithoutDelegating() {
    RecordingAuthorizer authorizer = new RecordingAuthorizer(Set.of());
    AuthorizationState state = new AuthorizationState(mock(PolarisResolutionManifest.class));

    List<AuthorizationDecision> decisions = authorizer.authorize(state, List.of());

    assertThat(decisions).isEmpty();
    assertThat(authorizer.seenRequests).isEmpty();
  }

  @Test
  void defaultBatchAgreesWithSingleRequestAuthorize() {
    List<AuthorizationRequest> requests =
        List.of(requestForTable("table1"), requestForTable("table2"), requestForTable("table3"));

    List<Boolean> individual =
        requests.stream()
            .map(
                request ->
                    new RecordingAuthorizer(Set.of("table2"))
                        .authorize(
                            new AuthorizationState(mock(PolarisResolutionManifest.class)), request)
                        .isAllowed())
            .toList();

    List<Boolean> batched =
        new RecordingAuthorizer(Set.of("table2"))
                .authorize(new AuthorizationState(mock(PolarisResolutionManifest.class)), requests)
                .stream()
                .map(AuthorizationDecision::isAllowed)
                .toList();

    assertThat(batched).isEqualTo(individual);
  }
}
