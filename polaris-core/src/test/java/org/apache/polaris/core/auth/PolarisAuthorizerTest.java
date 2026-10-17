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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.polaris.core.collection.AttributeMap;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.junit.jupiter.api.Test;

/**
 * Tests the default batch {@link PolarisAuthorizer#authorize(AuthorizationState,
 * AuthorizationBatchRequest)} and {@link
 * PolarisAuthorizer#resolveAuthorizationInputs(AuthorizationState, AuthorizationBatchRequest)}
 * implementations on the interface itself.
 *
 * <p>This default is the production code path for every authorizer that does not override it, which
 * today is all of them ({@link PolarisAuthorizerImpl} and the Ranger extension). Callers such as
 * the Iceberg catalog handler's LIST filtering depend on its contract: one decision per intent, in
 * order, with no short-circuiting across intents.
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
    private final List<AuthorizationRequest> seenResolveRequests = new ArrayList<>();

    RecordingAuthorizer(Set<String> deniedLeafNames) {
      this.deniedLeafNames = deniedLeafNames;
    }

    @Override
    public void resolveAuthorizationInputs(
        AuthorizationState authzState, AuthorizationRequest request) {
      seenResolveRequests.add(request);
    }

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
  }

  private static final PolarisPrincipal PRINCIPAL =
      PolarisPrincipal.of("alice", AttributeMap.EMPTY, Set.of("role1"));

  private static AuthorizationIntent intentForTable(String tableName) {
    PolarisSecurable target =
        PolarisSecurable.of(
            new PathSegment(PolarisEntityType.CATALOG, "myCatalog"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns1"),
            new PathSegment(PolarisEntityType.TABLE_LIKE, tableName));
    return new SingleTargetAuthorizationIntent(PolarisAuthorizableOperation.LIST_TABLES, target);
  }

  private static AuthorizationBatchRequest batchForTables(String... tableNames) {
    return new AuthorizationBatchRequest(
        PRINCIPAL, Arrays.stream(tableNames).map(PolarisAuthorizerTest::intentForTable).toList());
  }

  @Test
  void defaultBatchReturnsOneDecisionPerIntentInOrder() {
    RecordingAuthorizer authorizer = new RecordingAuthorizer(Set.of("table2"));
    AuthorizationState state = new AuthorizationState(mock(PolarisResolutionManifest.class));
    AuthorizationBatchRequest batch = batchForTables("table1", "table2", "table3");

    List<AuthorizationDecision> decisions = authorizer.authorize(state, batch);

    // Size and order must mirror the input exactly: callers index decisions against candidates.
    assertThat(decisions).hasSameSizeAs(batch.intents());
    assertThat(decisions.get(0).isAllowed()).isTrue();
    assertThat(decisions.get(1).isAllowed()).isFalse();
    assertThat(decisions.get(2).isAllowed()).isTrue();

    // The denial is the delegate's own decision, not a synthesized one.
    assertThat(decisions.get(1).getMessage()).contains(DENY_MESSAGE);
  }

  @Test
  void defaultBatchDoesNotShortCircuitWhenAnEarlierIntentIsDenied() {
    // The first intent is denied. The contract requires every later intent to still be evaluated
    // and to receive its own decision.
    RecordingAuthorizer authorizer = new RecordingAuthorizer(Set.of("table1"));
    AuthorizationState state = new AuthorizationState(mock(PolarisResolutionManifest.class));

    List<AuthorizationDecision> decisions =
        authorizer.authorize(state, batchForTables("table1", "table2", "table3"));

    assertThat(authorizer.seenRequests).hasSize(3);
    assertThat(decisions).hasSize(3);
    assertThat(decisions.get(0).isAllowed()).isFalse();
    assertThat(decisions.get(1).isAllowed()).isTrue();
    assertThat(decisions.get(2).isAllowed()).isTrue();
  }

  @Test
  void defaultBatchEvaluatesEveryIntentExactlyOnceAndInOrder() {
    RecordingAuthorizer authorizer = new RecordingAuthorizer(Set.of());
    AuthorizationState state = new AuthorizationState(mock(PolarisResolutionManifest.class));
    AuthorizationBatchRequest batch = batchForTables("table1", "table2", "table3");

    authorizer.authorize(state, batch);

    // Each intent is delegated on its own, as a single-intent request for the batch's principal.
    assertThat(authorizer.seenRequests)
        .containsExactlyElementsOf(
            batch.intents().stream()
                .map(intent -> new AuthorizationRequest(PRINCIPAL, List.of(intent)))
                .toList());
    // The shared state is threaded through to every delegation unchanged.
    assertThat(authorizer.seenStates).hasSize(3).allMatch(seen -> seen == state);
  }

  @Test
  void defaultBatchDeniesEveryIntentWhenNoneAreAllowed() {
    RecordingAuthorizer authorizer = new RecordingAuthorizer(Set.of("table1", "table2"));
    AuthorizationState state = new AuthorizationState(mock(PolarisResolutionManifest.class));

    List<AuthorizationDecision> decisions =
        authorizer.authorize(state, batchForTables("table1", "table2"));

    assertThat(decisions).hasSize(2).noneMatch(AuthorizationDecision::isAllowed);
  }

  @Test
  void defaultBatchWithNoIntentsReturnsEmptyListWithoutDelegating() {
    RecordingAuthorizer authorizer = new RecordingAuthorizer(Set.of());
    AuthorizationState state = new AuthorizationState(mock(PolarisResolutionManifest.class));

    List<AuthorizationDecision> decisions = authorizer.authorize(state, batchForTables());

    assertThat(decisions).isEmpty();
    assertThat(authorizer.seenRequests).isEmpty();
  }

  @Test
  void defaultBatchAgreesWithSingleRequestAuthorize() {
    AuthorizationBatchRequest batch = batchForTables("table1", "table2", "table3");

    List<Boolean> individual =
        batch.intents().stream()
            .map(
                intent ->
                    new RecordingAuthorizer(Set.of("table2"))
                        .authorize(
                            new AuthorizationState(mock(PolarisResolutionManifest.class)),
                            new AuthorizationRequest(PRINCIPAL, List.of(intent)))
                        .isAllowed())
            .toList();

    List<Boolean> batched =
        new RecordingAuthorizer(Set.of("table2"))
                .authorize(new AuthorizationState(mock(PolarisResolutionManifest.class)), batch)
                .stream()
                .map(AuthorizationDecision::isAllowed)
                .toList();

    assertThat(batched).isEqualTo(individual);
  }

  @Test
  void defaultBatchResolveDelegatesOnceWithEveryIntentInOrder() {
    RecordingAuthorizer authorizer = new RecordingAuthorizer(Set.of());
    AuthorizationState state = new AuthorizationState(mock(PolarisResolutionManifest.class));
    AuthorizationBatchRequest batch = batchForTables("table1", "table2", "table3");

    authorizer.resolveAuthorizationInputs(state, batch);

    // One delegation, not one per intent: the shared state must be resolved at most once.
    assertThat(authorizer.seenResolveRequests).hasSize(1);
    AuthorizationRequest delegated = authorizer.seenResolveRequests.get(0);
    assertThat(delegated.principal()).isEqualTo(PRINCIPAL);
    assertThat(delegated.intents()).containsExactlyElementsOf(batch.intents());
  }

  @Test
  void defaultBatchResolveWithNoIntentsDoesNotDelegate() {
    RecordingAuthorizer authorizer = new RecordingAuthorizer(Set.of());
    AuthorizationState state = new AuthorizationState(mock(PolarisResolutionManifest.class));

    authorizer.resolveAuthorizationInputs(state, batchForTables());

    assertThat(authorizer.seenResolveRequests).isEmpty();
  }

  @Test
  void batchRequestCopiesIntentsDefensively() {
    List<AuthorizationIntent> intents = new ArrayList<>(List.of(intentForTable("table1")));
    AuthorizationBatchRequest batch = new AuthorizationBatchRequest(PRINCIPAL, intents);

    intents.add(intentForTable("table2"));

    assertThat(batch.intents()).containsExactly(intentForTable("table1"));
    assertThatThrownBy(() -> batch.intents().add(intentForTable("table3")))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
