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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.security.identity.CurrentIdentityAssociation;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Instance;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.context.RequestIdSupplier;
import org.junit.jupiter.api.Test;

class PolarisEventMetadataFactoryTest {

  @Test
  void testCreateReturnsNoRequestIdWhenSupplierReturnsNull() {
    PolarisEventMetadataFactory factory = new PolarisEventMetadataFactory();
    factory.clock = Clock.fixed(Instant.parse("2026-03-14T03:12:00Z"), ZoneOffset.UTC);

    CurrentIdentityAssociation currentIdentityAssociation = mock(CurrentIdentityAssociation.class);
    when(currentIdentityAssociation.getDeferredIdentity()).thenReturn(Uni.createFrom().nullItem());
    factory.currentIdentityAssociation = currentIdentityAssociation;

    @SuppressWarnings("unchecked")
    Instance<RealmContext> realmContext = mock(Instance.class);
    when(realmContext.isResolvable()).thenReturn(true);
    when(realmContext.get()).thenReturn(() -> "test-realm");
    factory.realmContext = realmContext;

    factory.requestIdSupplier = () -> null;

    PolarisEventMetadata metadata = factory.create();

    assertThat(metadata.requestId()).isEmpty();
  }

  @Test
  void testCreateUsesRequestIdSupplier() {
    PolarisEventMetadataFactory factory = new PolarisEventMetadataFactory();
    factory.clock = Clock.fixed(Instant.parse("2026-03-14T03:12:00Z"), ZoneOffset.UTC);

    CurrentIdentityAssociation currentIdentityAssociation = mock(CurrentIdentityAssociation.class);
    when(currentIdentityAssociation.getDeferredIdentity()).thenReturn(Uni.createFrom().nullItem());
    factory.currentIdentityAssociation = currentIdentityAssociation;

    @SuppressWarnings("unchecked")
    Instance<RealmContext> realmContext = mock(Instance.class);
    when(realmContext.isResolvable()).thenReturn(true);
    when(realmContext.get()).thenReturn(() -> "test-realm");
    factory.realmContext = realmContext;

    factory.requestIdSupplier = () -> "req-123";

    PolarisEventMetadata metadata = factory.create();

    assertThat(metadata.requestId()).contains("req-123");
    assertThat(metadata.realmId()).isEqualTo("test-realm");
    assertThat(metadata.timestamp()).isEqualTo(Instant.parse("2026-03-14T03:12:00Z"));
  }
}
