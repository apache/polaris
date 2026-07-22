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
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.ranger.authz.model.RangerUserInfo;
import org.junit.jupiter.api.Test;

public class RangerUtilsTest {

  @Test
  void toUserInfoIncludesUserDefinedPrincipalProperties() {
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

    assertThat(userInfo.getName()).isEqualTo("alice");
    assertThat(userInfo.getRoles()).containsExactly("admin");
    assertThat(userInfo.getAttributes()).containsEntry("department", "finance");
    assertThat(userInfo.getAttributes())
        .containsEntry(PolarisEntityConstants.getClientIdPropertyName(), "client-id-123");
  }

  @Test
  void toUserInfoPrefersInternalPropertiesOnCollision() {
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            "alice",
            Map.of(
                PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY,
                new PrincipalEntity.Builder()
                    .setName("alice")
                    .setProperties(
                        Map.of(PolarisEntityConstants.getClientIdPropertyName(), "user-value"))
                    .setClientId("internal-value")
                    .build()),
            Set.of("admin"));

    RangerUserInfo userInfo = RangerUtils.toUserInfo(principal);

    assertThat(userInfo.getAttributes())
        .containsEntry(PolarisEntityConstants.getClientIdPropertyName(), "internal-value");
  }
}
