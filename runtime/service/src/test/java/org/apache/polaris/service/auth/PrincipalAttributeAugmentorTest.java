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
package org.apache.polaris.service.auth;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import java.security.Principal;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributeNamespaces;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PrincipalAttributeAugmentorTest {

  private PrincipalAttributeAugmentor augmentor;

  @BeforeEach
  public void setup() {
    augmentor = new PrincipalAttributeAugmentor();
  }

  @Test
  public void testPriorityRunsAfterAuthenticatingAugmentor() {
    assertThat(augmentor.priority()).isLessThan(AuthenticatingAugmentor.PRIORITY);
  }

  @Test
  public void testAugmentAnonymousIdentity() {
    SecurityIdentity anonymous = QuarkusSecurityIdentity.builder().setAnonymous(true).build();
    assertThat(augmentor.augment(anonymous, null).await().indefinitely()).isSameAs(anonymous);
  }

  @Test
  public void testAugmentNonPolarisPrincipal() {
    Principal other = Mockito.mock(Principal.class);
    SecurityIdentity identity = QuarkusSecurityIdentity.builder().setPrincipal(other).build();
    assertThat(augmentor.augment(identity, Uni.createFrom()::item).await().indefinitely())
        .isSameAs(identity);
  }

  @Test
  public void testAugmentWithoutPrincipalEntity() {
    PolarisPrincipal principal = PolarisPrincipal.of("eve", Map.of(), Set.of("auditor"));
    SecurityIdentity identity = identityFor(principal);
    assertThat(augmentor.augment(identity, Uni.createFrom()::item).await().indefinitely())
        .isSameAs(identity);
  }

  @Test
  public void testDerivesUserAndSelectedSystemAttributes() {
    PrincipalEntity entity =
        new PrincipalEntity.Builder()
            .setName("eve")
            .setProperties(Map.of("department", "finance"))
            .setClientId("client-1")
            .setCredentialRotationRequiredState()
            .build();
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            "eve",
            Map.of(PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, entity),
            Set.of("auditor"));

    SecurityIdentity result =
        augmentor.augment(identityFor(principal), Uni.createFrom()::item).await().indefinitely();
    PolarisPrincipal enriched = (PolarisPrincipal) result.getPrincipal();

    assertThat(enriched.getName()).isEqualTo("eve");
    assertThat(enriched.getRoles()).containsExactly("auditor");
    assertThat(
            enriched.getAttribute(
                PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, PrincipalEntity.class))
        .contains(entity);
    assertThat(enriched.getAttributes())
        .containsEntry(PolarisPrincipalAttributeNamespaces.USER_PREFIX + "department", "finance")
        .containsEntry(PolarisPrincipalAttributeNamespaces.SYSTEM_CLIENT_ID, "client-1")
        .containsEntry(
            PolarisPrincipalAttributeNamespaces.SYSTEM_CREDENTIAL_ROTATION_REQUIRED, "true");
  }

  @Test
  public void testUserPropertyCannotShadowSystemClientId() {
    PrincipalEntity entity =
        new PrincipalEntity.Builder()
            .setName("eve")
            .setProperties(
                Map.of(PolarisEntityConstants.getClientIdPropertyName(), "user-client-id"))
            .setClientId("internal-client-id")
            .build();
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            "eve", Map.of(PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, entity), Set.of());

    PolarisPrincipal enriched =
        (PolarisPrincipal)
            augmentor
                .augment(identityFor(principal), Uni.createFrom()::item)
                .await()
                .indefinitely()
                .getPrincipal();

    assertThat(enriched.getAttributes())
        .containsEntry(PolarisPrincipalAttributeNamespaces.SYSTEM_CLIENT_ID, "internal-client-id")
        .containsEntry(
            PolarisPrincipalAttributeNamespaces.USER_PREFIX
                + PolarisEntityConstants.getClientIdPropertyName(),
            "user-client-id")
        .doesNotContainKey(PolarisEntityConstants.getClientIdPropertyName());
  }

  @Test
  public void testPreservesExistingAuthAttributes() {
    PrincipalEntity entity =
        new PrincipalEntity.Builder()
            .setName("eve")
            .setProperties(Map.of("department", "finance"))
            .build();
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            "eve",
            Map.of(
                PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY,
                entity,
                PolarisPrincipalAttributeNamespaces.AUTH_PREFIX + "department",
                "idp-finance"),
            Set.of());

    PolarisPrincipal enriched =
        (PolarisPrincipal)
            augmentor
                .augment(identityFor(principal), Uni.createFrom()::item)
                .await()
                .indefinitely()
                .getPrincipal();

    assertThat(enriched.getAttributes())
        .containsEntry(
            PolarisPrincipalAttributeNamespaces.AUTH_PREFIX + "department", "idp-finance")
        .containsEntry(PolarisPrincipalAttributeNamespaces.USER_PREFIX + "department", "finance");
  }

  @Test
  public void testPreservesEmptyAndWhitespaceUserPropertyValues() {
    PrincipalEntity entity =
        new PrincipalEntity.Builder()
            .setName("eve")
            .setProperties(Map.of("department", "", "team", "   ", "region", "us-west"))
            .build();
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            "eve", Map.of(PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, entity), Set.of());

    PolarisPrincipal enriched =
        (PolarisPrincipal)
            augmentor
                .augment(identityFor(principal), Uni.createFrom()::item)
                .await()
                .indefinitely()
                .getPrincipal();

    assertThat(enriched.getAttributes())
        .containsEntry(PolarisPrincipalAttributeNamespaces.USER_PREFIX + "department", "")
        .containsEntry(PolarisPrincipalAttributeNamespaces.USER_PREFIX + "team", "   ")
        .containsEntry(PolarisPrincipalAttributeNamespaces.USER_PREFIX + "region", "us-west");
  }

  @Test
  public void testDoesNotDumpUnselectedInternalProperties() {
    PrincipalEntity entity =
        new PrincipalEntity.Builder()
            .setName("eve")
            .setClientId("client-1")
            .addInternalProperty("unrelated_internal", "secret")
            .build();
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            "eve", Map.of(PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, entity), Set.of());

    PolarisPrincipal enriched =
        (PolarisPrincipal)
            augmentor
                .augment(identityFor(principal), Uni.createFrom()::item)
                .await()
                .indefinitely()
                .getPrincipal();

    assertThat(enriched.getAttributes())
        .containsEntry(PolarisPrincipalAttributeNamespaces.SYSTEM_CLIENT_ID, "client-1")
        .doesNotContainKey(PolarisPrincipalAttributeNamespaces.SYSTEM_PREFIX + "unrelated_internal")
        .doesNotContainKey("unrelated_internal");
  }

  private static SecurityIdentity identityFor(PolarisPrincipal principal) {
    return QuarkusSecurityIdentity.builder()
        .setAnonymous(false)
        .setPrincipal(principal)
        .addRoles(principal.getRoles())
        .build();
  }
}
