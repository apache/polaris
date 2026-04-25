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
import org.apache.polaris.core.entity.PrincipalEntity;
import org.junit.jupiter.api.Test;

public class PolarisPrincipalTest {

  @Test
  public void ofWithRoles_includesUserDefinedProperties() {
    PrincipalEntity entity =
        new PrincipalEntity.Builder()
            .setName("alice")
            .setClientId("client-123")
            .setProperties(Map.of("region", "northamerica", "department", "finance"))
            .build();

    PolarisPrincipal principal = PolarisPrincipal.of(entity, Set.of("analyst"));

    assertThat(principal.getProperties()).containsEntry("region", "northamerica");
    assertThat(principal.getProperties()).containsEntry("department", "finance");
    assertThat(principal.getProperties()).containsKey("client_id");
  }

  @Test
  public void ofWithRolesAndToken_includesUserDefinedProperties() {
    PrincipalEntity entity =
        new PrincipalEntity.Builder()
            .setName("bob")
            .setClientId("client-456")
            .setProperties(Map.of("region", "europe"))
            .build();

    PolarisPrincipal principal =
        PolarisPrincipal.of(entity, Set.of("viewer"), Optional.of("tok-xyz"));

    assertThat(principal.getProperties()).containsEntry("region", "europe");
    assertThat(principal.getProperties()).containsKey("client_id");
    assertThat(principal.getToken()).contains("tok-xyz");
  }

  @Test
  public void internalPropertiesTakePrecedenceOnConflict() {
    PrincipalEntity entity =
        new PrincipalEntity.Builder()
            .setName("carol")
            .setClientId("real-client-id")
            .setProperties(Map.of("client_id", "spoofed-id", "region", "asia"))
            .build();

    PolarisPrincipal principal = PolarisPrincipal.of(entity, Set.of());

    assertThat(principal.getProperties()).containsEntry("client_id", "real-client-id");
    assertThat(principal.getProperties()).containsEntry("region", "asia");
  }
}
