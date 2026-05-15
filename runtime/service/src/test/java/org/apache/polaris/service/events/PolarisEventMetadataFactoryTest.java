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
package org.apache.polaris.service.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.quarkus.security.identity.CurrentIdentityAssociation;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Instance;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import org.apache.polaris.core.auth.ImmutablePolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PolarisEventMetadataFactoryTest {

  private static final Instant FIXED_INSTANT = Instant.parse("2025-01-01T00:00:00Z");
  private static final String TEST_REALM = "test-realm";

  @Mock private CurrentIdentityAssociation currentIdentityAssociation;
  @Mock private Instance<RealmContext> realmContext;
  @Mock private RealmContext realmContextInstance;
  @Mock private SecurityIdentity securityIdentity;

  @InjectMocks private PolarisEventMetadataFactory factory;

  @BeforeEach
  void setUp() {
    factory.clock = Clock.fixed(FIXED_INSTANT, ZoneId.of("UTC"));
    when(realmContext.isResolvable()).thenReturn(true);
    when(realmContext.get()).thenReturn(realmContextInstance);
    when(realmContextInstance.getRealmIdentifier()).thenReturn(TEST_REALM);
  }

  @Test
  void createReturnsMetadataWithUserWhenIdentityResolved() {
    PolarisPrincipal principal = ImmutablePolarisPrincipal.builder().name("test-user").build();
    when(currentIdentityAssociation.getDeferredIdentity())
        .thenReturn(Uni.createFrom().item(securityIdentity));
    when(securityIdentity.isAnonymous()).thenReturn(false);
    when(securityIdentity.getPrincipal(PolarisPrincipal.class)).thenReturn(principal);

    PolarisEventMetadata metadata = factory.create();

    assertThat(metadata.realmId()).isEqualTo(TEST_REALM);
    assertThat(metadata.timestamp()).isEqualTo(FIXED_INSTANT);
    assertThat(metadata.user()).contains(principal);
  }

  @Test
  void createReturnsEmptyUserWhenIdentityIsAnonymous() {
    when(currentIdentityAssociation.getDeferredIdentity())
        .thenReturn(Uni.createFrom().item(securityIdentity));
    when(securityIdentity.isAnonymous()).thenReturn(true);

    PolarisEventMetadata metadata = factory.create();

    assertThat(metadata.user()).isEmpty();
  }

  @Test
  void createReturnsEmptyUserWhenIdentityNotYetResolved() {
    // Simulates getNow(null) returning null because the future hasn't completed
    when(currentIdentityAssociation.getDeferredIdentity())
        .thenReturn(Uni.createFrom().item(() -> null));

    PolarisEventMetadata metadata = factory.create();

    assertThat(metadata.user()).isEmpty();
  }

  @Test
  void createReturnsEmptyUserWhenDeferredIdentityFailsWithCompletionException() {
    // Simulates the scenario where triggering deferred identity resolution causes
    // the auth pipeline to fail (e.g., on unauthenticated endpoints like /oauth/tokens)
    when(currentIdentityAssociation.getDeferredIdentity())
        .thenReturn(
            Uni.createFrom().failure(new RuntimeException("Authentication pipeline failed")));

    PolarisEventMetadata metadata = factory.create();

    assertThat(metadata.user()).isEmpty();
    assertThat(metadata.realmId()).isEqualTo(TEST_REALM);
    assertThat(metadata.timestamp()).isEqualTo(FIXED_INSTANT);
  }
}
