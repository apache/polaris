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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.junit.jupiter.api.Test;

public class AuthorizationRequestTest {

  @Test
  void hasSecurableTypeReturnsTrueForPrincipalTarget() {
    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisPrincipal.of("alice", Map.of(), Set.of("role")),
            AuthorizationIntent.of(
                PolarisAuthorizableOperation.LOAD_TABLE,
                PolarisSecurable.of(new PathSegment(PolarisEntityType.PRINCIPAL, "alice"))));

    assertThat(request.intents().get(0)).isInstanceOf(SingleTargetAuthorizationIntent.class);
    assertThat(request.hasSecurableType(PolarisEntityType.PRINCIPAL)).isTrue();
  }

  @Test
  void hasSecurableTypeReturnsTrueForPrincipalRoleSecondary() {
    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisPrincipal.of("alice", Map.of(), Set.of("role")),
            AuthorizationIntent.of(
                PolarisAuthorizableOperation.ASSIGN_PRINCIPAL_ROLE,
                PolarisSecurable.of(new PathSegment(PolarisEntityType.PRINCIPAL, "alice")),
                PolarisSecurable.of(
                    new PathSegment(PolarisEntityType.PRINCIPAL_ROLE, "analytics-admin"))));

    assertThat(request.intents().get(0)).isInstanceOf(PairwiseTargetAuthorizationIntent.class);
    assertThat(request.hasSecurableType(PolarisEntityType.PRINCIPAL_ROLE)).isTrue();
  }

  @Test
  void hasSecurableTypeReturnsTrueForCatalogRoleSecondary() {
    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisPrincipal.of("alice", Map.of(), Set.of("role")),
            AuthorizationIntent.of(
                PolarisAuthorizableOperation.ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE,
                PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "catalog")),
                PolarisSecurable.of(
                    new PathSegment(PolarisEntityType.CATALOG, "catalog"),
                    new PathSegment(PolarisEntityType.CATALOG_ROLE, "catalog-role"))));

    assertThat(request.hasSecurableType(PolarisEntityType.CATALOG_ROLE)).isTrue();
  }

  @Test
  void hasSecurableTypeReturnsFalseWhenTypeAbsent() {
    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisPrincipal.of("alice", Map.of(), Set.of("role")),
            AuthorizationIntent.of(
                PolarisAuthorizableOperation.LOAD_VIEW,
                PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "catalog"))));

    assertThat(request.hasSecurableType(PolarisEntityType.PRINCIPAL_ROLE)).isFalse();
  }

  @Test
  void allowsTargetlessIntent() {
    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisPrincipal.of("alice", Map.of(), Set.of("role")),
            AuthorizationIntent.of(PolarisAuthorizableOperation.LIST_CATALOGS));

    assertThat(request.intents().get(0)).isInstanceOf(TargetlessAuthorizationIntent.class);
    assertThat(request.intents().get(0).getTarget()).isNull();
    assertThat(request.intents().get(0).getSecondary()).isNull();
  }

  @Test
  void throwsWhenIntentFactoryHasNoTargetOrSecondary() {
    assertThatThrownBy(
            () -> AuthorizationIntent.of(PolarisAuthorizableOperation.GET_CATALOG, null, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "PairwiseTargetAuthorizationIntent must contain a target or secondary");
  }

  @Test
  void threeArgIntentFactoryAlwaysCreatesPairwiseIntent() {
    PolarisSecurable target =
        PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "catalog"));
    AuthorizationIntent intent =
        AuthorizationIntent.of(PolarisAuthorizableOperation.GET_CATALOG, target, null);

    assertThat(intent).isInstanceOf(PairwiseTargetAuthorizationIntent.class);
    assertThat(intent.getTarget()).isEqualTo(target);
    assertThat(intent.getSecondary()).isNull();
  }

  @Test
  void requestRequiresAtLeastOneIntent() {
    assertThatThrownBy(
            () ->
                AuthorizationRequest.of(
                    PolarisPrincipal.of("alice", Map.of(), Set.of("role")), List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must contain at least one intent");
  }

  @Test
  void throwsWhenSecurableDoesNotStartWithTopLevelEntity() {
    assertThatThrownBy(
            () -> PolarisSecurable.of(new PathSegment(PolarisEntityType.NAMESPACE, "ns")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("must start with a top-level entity");
  }
}
