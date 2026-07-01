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

import java.util.List;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

public class VisibilityFilterRequestTest {

  /**
   * Minimal concrete authorizer that does not override {@code filterByVisibility}, so the default
   * no-op implementation on the interface is exercised.
   */
  private static final class NoOpAuthorizer implements PolarisAuthorizer {
    @Override
    public void resolveAuthorizationInputs(
        AuthorizationState authzState, AuthorizationRequest request) {}

    @Override
    public AuthorizationDecision authorize(
        AuthorizationState authzState, AuthorizationRequest request) {
      return AuthorizationDecision.allow();
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

  private static final PolarisSecurable CATALOG_SECURABLE =
      PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "myCatalog"));

  private static final PolarisSecurable NS_SECURABLE =
      PolarisSecurable.of(
          new PathSegment(PolarisEntityType.CATALOG, "myCatalog"),
          new PathSegment(PolarisEntityType.NAMESPACE, "ns1"));

  private static final PolarisSecurable TABLE_SECURABLE_1 =
      PolarisSecurable.of(
          new PathSegment(PolarisEntityType.CATALOG, "myCatalog"),
          new PathSegment(PolarisEntityType.NAMESPACE, "ns1"),
          new PathSegment(PolarisEntityType.TABLE_LIKE, "table1"));

  private static final PolarisSecurable TABLE_SECURABLE_2 =
      PolarisSecurable.of(
          new PathSegment(PolarisEntityType.CATALOG, "myCatalog"),
          new PathSegment(PolarisEntityType.NAMESPACE, "ns1"),
          new PathSegment(PolarisEntityType.TABLE_LIKE, "table2"));

  @Test
  void recordExposesAllFields() {
    List<PolarisSecurable> candidates = List.of(TABLE_SECURABLE_1, TABLE_SECURABLE_2);

    VisibilityFilterRequest request =
        new VisibilityFilterRequest(
            PolarisAuthorizableOperation.LIST_TABLES, NS_SECURABLE, candidates);

    assertThat(request.listOperation()).isEqualTo(PolarisAuthorizableOperation.LIST_TABLES);
    assertThat(request.container()).isSameAs(NS_SECURABLE);
    assertThat(request.candidates()).isSameAs(candidates);
  }

  @Test
  void defaultFilterByVisibilityReturnsAllCandidatesUnchanged() {
    // The default no-op implementation on PolarisAuthorizer must return all candidates
    // unchanged, preserving backward compatibility for authorizers that do not override it.
    PolarisAuthorizer authorizer = new NoOpAuthorizer();
    List<PolarisSecurable> candidates = List.of(TABLE_SECURABLE_1, TABLE_SECURABLE_2);

    VisibilityFilterRequest request =
        new VisibilityFilterRequest(
            PolarisAuthorizableOperation.LIST_TABLES, NS_SECURABLE, candidates);

    List<PolarisSecurable> result =
        authorizer.filterByVisibility(mock(AuthorizationState.class), request);

    assertThat(result).isSameAs(candidates);
  }

  @Test
  void defaultFilterByVisibilityWithEmptyCandidatesReturnsEmpty() {
    PolarisAuthorizer authorizer = new NoOpAuthorizer();

    VisibilityFilterRequest request =
        new VisibilityFilterRequest(
            PolarisAuthorizableOperation.LIST_NAMESPACES, CATALOG_SECURABLE, List.of());

    List<PolarisSecurable> result =
        authorizer.filterByVisibility(mock(AuthorizationState.class), request);

    assertThat(result).isEmpty();
  }
}
