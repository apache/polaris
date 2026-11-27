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
package org.apache.polaris.extensions.events.webhook;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.collection.AttributeMap;
import org.apache.polaris.core.collection.MutableAttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadata;
import org.apache.polaris.service.events.PolarisEventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(value = WebhookReceiverTestResource.class, restrictToAnnotatedClass = true)
@TestProfile(WebhookEventListenerQuarkusNoSecretTest.Profile.class)
class WebhookEventListenerQuarkusNoSecretTest {

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.event-listener.types",
          "webhook",
          "polaris.event-listener.webhook.require-https",
          "false",
          "polaris.readiness.ignore-severe-issues",
          "true");
    }
  }

  @Inject
  @Identifier("webhook")
  WebhookEventListener listener;

  WebhookReceiverTestResource.Capture capture;

  @BeforeEach
  void clearCapture() {
    capture.clear();
  }

  @Test
  void productionRestClientOmitsSignatureWhenNoSecretConfigured() {
    listener.onEvent(testEvent());

    assertThat(capture.requests()).hasSize(1);
    assertThat(capture.requests().getFirst().signature()).isNull();
  }

  private static PolarisEvent testEvent() {
    return new PolarisEvent(
        PolarisEventType.AFTER_REFRESH_TABLE,
        PolarisEventMetadata.builder()
            .eventId(UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"))
            .timestamp(Instant.parse("2026-03-01T09:30:00Z"))
            .realmId("test-realm")
            .user(PolarisPrincipal.of("test-user", AttributeMap.EMPTY, Set.of("role1")))
            .build(),
        MutableAttributeMap.builder()
            .put(EventAttributes.CATALOG_NAME, "test_catalog")
            .put(EventAttributes.TABLE_IDENTIFIER, TableIdentifier.of("test_ns", "test_table"))
            .build());
  }
}
