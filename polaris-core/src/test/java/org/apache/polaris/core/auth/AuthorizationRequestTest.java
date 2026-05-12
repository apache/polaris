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

import org.apache.polaris.core.entity.PolarisEntityType;
import org.junit.jupiter.api.Test;

public class AuthorizationRequestTest {

  @Test
  void hasSecurableTypeReturnsTrueForPrincipalTarget() {
    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisAuthorizableOperation.LOAD_TABLE,
            PolarisSecurable.of(new PathSegment(PolarisEntityType.PRINCIPAL, "alice")));

    assertThat(request).isInstanceOf(SingleTargetAuthorizationRequest.class);
    assertThat(request.hasSecurableType(PolarisEntityType.PRINCIPAL)).isTrue();
  }

  @Test
  void hasSecurableTypeReturnsTrueForPrincipalRoleSecondary() {
    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisAuthorizableOperation.ASSIGN_PRINCIPAL_ROLE,
            PolarisSecurable.of(new PathSegment(PolarisEntityType.PRINCIPAL, "alice")),
            PolarisSecurable.of(
                new PathSegment(PolarisEntityType.PRINCIPAL_ROLE, "analytics-admin")));

    assertThat(request).isInstanceOf(PairwiseTargetAuthorizationRequest.class);
    assertThat(request.hasSecurableType(PolarisEntityType.PRINCIPAL_ROLE)).isTrue();
  }

  @Test
  void hasSecurableTypeReturnsTrueForCatalogRoleSecondary() {
    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisAuthorizableOperation.ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE,
            PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "catalog")),
            PolarisSecurable.of(
                new PathSegment(PolarisEntityType.CATALOG, "catalog"),
                new PathSegment(PolarisEntityType.CATALOG_ROLE, "catalog-role")));

    assertThat(request.hasSecurableType(PolarisEntityType.CATALOG_ROLE)).isTrue();
  }

  @Test
  void hasSecurableTypeReturnsFalseWhenTypeAbsent() {
    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisAuthorizableOperation.LOAD_VIEW,
            PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "catalog")));

    assertThat(request.hasSecurableType(PolarisEntityType.PRINCIPAL_ROLE)).isFalse();
  }

  @Test
  void allowsUntargetedRequest() {
    AuthorizationRequest request =
        AuthorizationRequest.of(PolarisAuthorizableOperation.LIST_CATALOGS);

    assertThat(request).isInstanceOf(UntargetedAuthorizationRequest.class);
    assertThat(request.getTargets()).isEmpty();
    assertThat(request.getSecondaries()).isEmpty();
  }

  @Test
  void throwsWhenTargetedFactoryHasNoTargetOrSecondary() {
    assertThatThrownBy(
            () -> AuthorizationRequest.of(PolarisAuthorizableOperation.GET_CATALOG, null, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "PairwiseTargetAuthorizationRequest must contain a target or secondary");
  }

  @Test
  void threeArgFactoryAlwaysCreatesPairwiseRequest() {
    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisAuthorizableOperation.GET_CATALOG,
            PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "catalog")),
            null);

    assertThat(request).isInstanceOf(PairwiseTargetAuthorizationRequest.class);
    assertThat(request.getTargets())
        .containsExactly(
            PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "catalog")));
    assertThat(request.getSecondaries()).isEmpty();
  }

  @Test
  void throwsWhenSecurableDoesNotStartWithTopLevelEntity() {
    assertThatThrownBy(
            () -> PolarisSecurable.of(new PathSegment(PolarisEntityType.NAMESPACE, "ns")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("must start with a top-level entity");
  }
}
