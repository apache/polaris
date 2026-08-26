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

import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.collection.AttributeMap;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class PolarisPrincipalAttributeNamespacesTest {

  @ParameterizedTest
  @CsvSource({
    "polaris.auth.department,true",
    "polaris.system.client_id,true",
    "polaris.user.department,true",
    "department,false",
    "client_id,false",
    "org.apache.polaris.core.auth.PRINCIPAL_ENTITY,false"
  })
  void isDerivedAttributeKey(String key, boolean expected) {
    assertThat(PolarisPrincipalAttributeNamespaces.isDerivedAttributeKey(key)).isEqualTo(expected);
  }

  @Test
  void derivedStringAttributesOmitsPrimaryAndNonStringValues() {
    PrincipalEntity entity = new PrincipalEntity.Builder().setName("eve").build();
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            "eve",
            ImmutableAttributeMap.builder()
                .put(PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, entity)
                .put(PolarisPrincipalAttributes.PRINCIPAL_ROLE_ALL_ATTRIBUTE_KEY, true)
                .put(
                    PolarisPrincipalAttributeNamespaces.stringKey(
                        PolarisPrincipalAttributeNamespaces.USER_PREFIX + "department"),
                    "finance")
                .put(
                    PolarisPrincipalAttributeNamespaces.stringKey(
                        PolarisPrincipalAttributeNamespaces.SYSTEM_CLIENT_ID),
                    "client-1")
                .put(
                    PolarisPrincipalAttributeNamespaces.stringKey(
                        PolarisPrincipalAttributeNamespaces.AUTH_PREFIX + "region"),
                    "us-west")
                .build(),
            Set.of("auditor"));

    assertThat(PolarisPrincipalAttributeNamespaces.derivedStringAttributes(principal))
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                PolarisPrincipalAttributeNamespaces.USER_PREFIX + "department",
                "finance",
                PolarisPrincipalAttributeNamespaces.SYSTEM_CLIENT_ID,
                "client-1",
                PolarisPrincipalAttributeNamespaces.AUTH_PREFIX + "region",
                "us-west"));
  }

  @Test
  void derivedStringAttributesEmptyWhenNoneProjected() {
    PolarisPrincipal principal = PolarisPrincipal.of("eve", AttributeMap.EMPTY, Set.of());
    assertThat(PolarisPrincipalAttributeNamespaces.derivedStringAttributes(principal)).isEmpty();
  }
}
