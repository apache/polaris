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
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.junit.jupiter.api.Test;

class PolarisPrincipalTest {

  private static final String CLIENT_ID_KEY = PolarisEntityConstants.getClientIdPropertyName();

  @Test
  void ofPrincipalEntityExposesUserDefinedProperties() {
    PrincipalEntity principalEntity =
        new PrincipalEntity.Builder()
            .setName("alice")
            .setClientId("client-123")
            .setProperties(Map.of("region", "northamerica", "department", "finance"))
            .build();

    PolarisPrincipal principal = PolarisPrincipal.of(principalEntity, Set.of("auditor"));

    assertThat(principal.getName()).isEqualTo("alice");
    assertThat(principal.getRoles()).containsExactly("auditor");
    assertThat(principal.getProperties())
        .containsEntry("region", "northamerica")
        .containsEntry("department", "finance")
        .containsEntry(CLIENT_ID_KEY, "client-123");
  }

  @Test
  void ofPrincipalEntityWithTokenExposesUserDefinedProperties() {
    PrincipalEntity principalEntity =
        new PrincipalEntity.Builder()
            .setName("bob")
            .setClientId("client-456")
            .setProperties(Map.of("team", "platform"))
            .build();

    PolarisPrincipal principal =
        PolarisPrincipal.of(principalEntity, Set.of("admin"), Optional.of("access-token"));

    assertThat(principal.getName()).isEqualTo("bob");
    assertThat(principal.getRoles()).containsExactly("admin");
    assertThat(principal.getToken()).contains("access-token");
    assertThat(principal.getProperties())
        .containsEntry("team", "platform")
        .containsEntry(CLIENT_ID_KEY, "client-456");
  }

  @Test
  void internalPropertiesTakePrecedenceOverUserDefinedOnCollision() {
    // Regression guard: if anyone later flips the merge order so that user-defined values win,
    // a caller could shadow a system-managed key such as client_id by setting a property of the
    // same name on the principal entity. This test pins the safe direction.
    PrincipalEntity principalEntity =
        new PrincipalEntity.Builder()
            .setName("eve")
            .setClientId("legitimate-client-id")
            .setProperties(Map.of(CLIENT_ID_KEY, "spoofed-client-id"))
            .build();

    PolarisPrincipal principal = PolarisPrincipal.of(principalEntity, Set.of());

    // System-managed value wins; user-supplied value is discarded.
    assertThat(principal.getProperties()).containsEntry(CLIENT_ID_KEY, "legitimate-client-id");
  }

  @Test
  void ofPrincipalEntityWithoutUserPropertiesStillExposesInternalProperties() {
    PrincipalEntity principalEntity =
        new PrincipalEntity.Builder().setName("svc").setClientId("client-789").build();

    PolarisPrincipal principal = PolarisPrincipal.of(principalEntity, Set.of());

    assertThat(principal.getProperties()).containsEntry(CLIENT_ID_KEY, "client-789");
  }
}
