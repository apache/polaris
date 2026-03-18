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
            PolarisAuthorizableOperation.LOAD_TABLE,
            List.of(
                AuthorizationTargetBinding.of(
                    PolarisSecurable.of(
                        List.of(new PathSegment(PolarisEntityType.PRINCIPAL, "alice"))),
                    null)));

    assertThat(request.hasSecurableType(PolarisEntityType.PRINCIPAL)).isTrue();
  }

  @Test
  void hasSecurableTypeReturnsTrueForPrincipalRoleSecondary() {
    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisPrincipal.of("alice", Map.of(), Set.of("role")),
            PolarisAuthorizableOperation.ASSIGN_PRINCIPAL_ROLE,
            List.of(
                AuthorizationTargetBinding.of(
                    PolarisSecurable.of(
                        List.of(new PathSegment(PolarisEntityType.PRINCIPAL, "alice"))),
                    PolarisSecurable.of(
                        List.of(
                            new PathSegment(
                                PolarisEntityType.PRINCIPAL_ROLE, "analytics-admin"))))));

    assertThat(request.hasSecurableType(PolarisEntityType.PRINCIPAL_ROLE)).isTrue();
  }

  @Test
  void hasSecurableTypeReturnsTrueForCatalogRoleAcrossMultipleBindings() {
    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisPrincipal.of("alice", Map.of(), Set.of("role")),
            PolarisAuthorizableOperation.ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE,
            List.of(
                AuthorizationTargetBinding.of(
                    PolarisSecurable.of(
                        List.of(
                            new PathSegment(PolarisEntityType.CATALOG, "catalog"),
                            new PathSegment(PolarisEntityType.NAMESPACE, "ns"))),
                    null),
                AuthorizationTargetBinding.of(
                    PolarisSecurable.of(
                        List.of(new PathSegment(PolarisEntityType.CATALOG, "catalog"))),
                    PolarisSecurable.of(
                        List.of(
                            new PathSegment(PolarisEntityType.CATALOG, "catalog"),
                            new PathSegment(PolarisEntityType.CATALOG_ROLE, "catalog-role"))))));

    assertThat(request.hasSecurableType(PolarisEntityType.CATALOG_ROLE)).isTrue();
  }

  @Test
  void hasSecurableTypeReturnsFalseWhenTypeAbsent() {
    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisPrincipal.of("alice", Map.of(), Set.of("role")),
            PolarisAuthorizableOperation.LOAD_VIEW,
            List.of(
                AuthorizationTargetBinding.of(
                    PolarisSecurable.of(
                        List.of(new PathSegment(PolarisEntityType.CATALOG, "catalog"))),
                    null)));

    assertThat(request.hasSecurableType(PolarisEntityType.PRINCIPAL_ROLE)).isFalse();
  }

  @Test
  void throwsWhenSecurableDoesNotStartWithTopLevelEntity() {
    assertThatThrownBy(
            () -> PolarisSecurable.of(List.of(new PathSegment(PolarisEntityType.NAMESPACE, "ns"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("must start with a top-level entity");
  }
}
