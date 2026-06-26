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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.junit.jupiter.api.Test;

public class AuthorizationRequestTest {

  @Test
  void rootPrivilegeGrantIntentRejectsNullGrantee() {
    assertThatThrownBy(
            () ->
                new RootPrivilegeGrantAuthorizationIntent(
                    PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("grantee must be non-null");
  }

  @Test
  void requestRequiresAtLeastOneIntent() {
    assertThatThrownBy(
            () ->
                new AuthorizationRequest(
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
