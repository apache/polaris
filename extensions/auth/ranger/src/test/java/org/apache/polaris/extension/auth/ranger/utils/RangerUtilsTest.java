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

package org.apache.polaris.extension.auth.ranger.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributeNamespaces;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.ranger.authz.model.RangerUserInfo;
import org.junit.jupiter.api.Test;

public class RangerUtilsTest {

  @Test
  void toUserInfoIncludesDerivedNamespacedAttributes() {
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            "alice",
            Map.of(
                PolarisPrincipalAttributeNamespaces.USER_PREFIX + "department",
                "finance",
                PolarisPrincipalAttributeNamespaces.SYSTEM_CLIENT_ID,
                "client-id-123",
                PolarisPrincipalAttributeNamespaces.AUTH_PREFIX + "region",
                "us-west"),
            Set.of("admin"));

    RangerUserInfo userInfo = RangerUtils.toUserInfo(principal);

    assertThat(userInfo.getName()).isEqualTo("alice");
    assertThat(userInfo.getRoles()).containsExactly("admin");
    // Ranger 2.9.0's embedded plugin does not copy these attributes onto the access request
    // it evaluates, so this asserts the DTO contract only. An authorization-level test that
    // depends on polaris.user.* cannot pass until Ranger copies RangerUserInfo.attributes.
    assertThat(userInfo.getAttributes())
        .containsEntry(PolarisPrincipalAttributeNamespaces.USER_PREFIX + "department", "finance")
        .containsEntry(PolarisPrincipalAttributeNamespaces.SYSTEM_CLIENT_ID, "client-id-123")
        .containsEntry(PolarisPrincipalAttributeNamespaces.AUTH_PREFIX + "region", "us-west");
  }

  @Test
  void toUserInfoPreservesEmptyUserPropertyValues() {
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            "alice",
            Map.of(PolarisPrincipalAttributeNamespaces.USER_PREFIX + "department", ""),
            Set.of("admin"));

    RangerUserInfo userInfo = RangerUtils.toUserInfo(principal);

    assertThat(userInfo.getAttributes())
        .containsEntry(PolarisPrincipalAttributeNamespaces.USER_PREFIX + "department", "");
  }

  @Test
  void toUserInfoDoesNotWalkPrincipalEntity() {
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            "alice",
            Map.of(
                PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY,
                new PrincipalEntity.Builder()
                    .setName("alice")
                    .setProperties(Map.of("department", "finance"))
                    .setClientId("client-id-123")
                    .build()),
            Set.of("admin"));

    RangerUserInfo userInfo = RangerUtils.toUserInfo(principal);

    assertThat(userInfo.getAttributes()).isEmpty();
  }

  @Test
  void toUserInfoOmitsUnnamespacedAttributes() {
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            "alice",
            Map.of(
                "department", "finance", PolarisPrincipal.PRINCIPAL_ROLE_ALL_ATTRIBUTE_KEY, true),
            Set.of("admin"));

    RangerUserInfo userInfo = RangerUtils.toUserInfo(principal);

    assertThat(userInfo.getAttributes()).isEmpty();
  }
}
