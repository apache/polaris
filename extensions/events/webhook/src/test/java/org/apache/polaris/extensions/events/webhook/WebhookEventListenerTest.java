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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.quarkus.runtime.configuration.MemorySize;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.config.PolarisIcebergObjectMapperCustomizer;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadata;
import org.apache.polaris.service.events.PolarisEventType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class WebhookEventListenerTest {

  private static final String REALM = "test-realm";
  private static final String TEST_USER = "test-user";
  private static final String SECRET = "webhook-signing-secret";
  private static final PolarisPrincipal PRINCIPAL =
      PolarisPrincipal.of(TEST_USER, Map.of(), Set.of("role1", "role2"));
  private static final Clock CLOCK = Clock.systemUTC();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    new PolarisIcebergObjectMapperCustomizer(new MemorySize(BigInteger.valueOf(1024 * 1024)))
        .customize(OBJECT_MAPPER);
  }

  @Mock private WebhookEventListenerConfiguration config;

  private AutoCloseable mockitoContext;
  private HttpServer server;
  private List<RecordedRequest> recordedRequests;
  private AtomicBoolean failNext;
  private AtomicBoolean failAlways;

  private record RecordedRequest(
      String body, String signature, String eventType, String authorization) {}

  @BeforeEach
  void setUp() throws Exception {
    mockitoContext = MockitoAnnotations.openMocks(this);
    recordedRequests = new CopyOnWriteArrayList<>();
    failNext = new AtomicBoolean(false);
    failAlways = new AtomicBoolean(false);
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/hook",
        exchange -> {
          String body =
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          recordedRequests.add(
              new RecordedRequest(
                  body,
                  exchange.getRequestHeaders().getFirst(WebhookEventListener.SIGNATURE_HEADER),
                  exchange.getRequestHeaders().getFirst(WebhookEventListener.EVENT_TYPE_HEADER),
                  exchange.getRequestHeaders().getFirst("Authorization")));
          int status = failAlways.get() || failNext.getAndSet(false) ? 500 : 200;
          exchange.sendResponseHeaders(status, -1);
          exchange.close();
        });
    server.start();

    when(config.endpoint()).thenReturn(Optional.of(endpoint()));
    when(config.secret()).thenReturn(Optional.of(SECRET));
    when(config.headers()).thenReturn(Map.of());
    when(config.timeout()).thenReturn(Duration.ofSeconds(5));
    when(config.maxAttempts()).thenReturn(3);
    when(config.retryBackoff()).thenReturn(Duration.ofMillis(50));
  }

  @AfterEach
  void tearDown() throws Exception {
    if (server != null) {
      server.stop(0);
    }
    if (mockitoContext != null) {
      mockitoContext.close();
    }
  }

  private URI endpoint() {
    return URI.create("http://localhost:" + server.getAddress().getPort() + "/hook");
  }

  private WebhookEventListener createListener() {
    WebhookEventListener listener = new WebhookEventListener(config, OBJECT_MAPPER, CLOCK);
    listener.start();
    return listener;
  }

  private static PolarisEvent testEvent() {
    return new PolarisEvent(
        PolarisEventType.AFTER_REFRESH_TABLE,
        PolarisEventMetadata.builder().realmId(REALM).user(PRINCIPAL).build(),
        new EventAttributeMap()
            .put(EventAttributes.CATALOG_NAME, "test_catalog")
            .put(EventAttributes.TABLE_IDENTIFIER, TableIdentifier.of("test_ns", "test_table")));
  }

  @Test
  void deliversSignedJsonPayload() {
    WebhookEventListener listener = createListener();
    try {
      listener.onEvent(testEvent());

      Awaitility.await("webhook delivered")
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> assertThat(recordedRequests).hasSize(1));

      RecordedRequest request = recordedRequests.getFirst();
      assertThat(request.body())
          .contains(PolarisEventType.AFTER_REFRESH_TABLE.name())
          .contains(REALM)
          .contains(TEST_USER)
          .contains("test_ns.test_table");
      assertThat(request.eventType()).isEqualTo(PolarisEventType.AFTER_REFRESH_TABLE.name());
      assertThat(request.signature())
          .isEqualTo("sha256=" + WebhookEventListener.sign(request.body(), SECRET));
    } finally {
      listener.shutdown();
    }
  }

  @Test
  void omitsSignatureHeaderWhenNoSecretConfigured() {
    when(config.secret()).thenReturn(Optional.empty());
    WebhookEventListener listener = createListener();
    try {
      listener.onEvent(testEvent());

      Awaitility.await("webhook delivered")
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> assertThat(recordedRequests).hasSize(1));

      assertThat(recordedRequests.getFirst().signature()).isNull();
      assertThat(recordedRequests.getFirst().body()).contains(REALM);
    } finally {
      listener.shutdown();
    }
  }

  @Test
  void sendsConfiguredCustomHeaders() {
    when(config.headers()).thenReturn(Map.of("Authorization", "Splunk test-hec-token"));
    WebhookEventListener listener = createListener();
    try {
      listener.onEvent(testEvent());

      Awaitility.await("webhook delivered")
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> assertThat(recordedRequests).hasSize(1));

      assertThat(recordedRequests.getFirst().authorization()).isEqualTo("Splunk test-hec-token");
    } finally {
      listener.shutdown();
    }
  }

  @Test
  void failsFastWhenEndpointNotConfigured() {
    when(config.endpoint()).thenReturn(Optional.empty());
    WebhookEventListener listener = new WebhookEventListener(config, OBJECT_MAPPER, CLOCK);
    assertThatThrownBy(listener::start)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("polaris.event-listener.webhook.endpoint");
  }

  @Test
  void retriesFailedDeliveryUntilSuccess() {
    failNext.set(true);
    WebhookEventListener listener = createListener();
    try {
      listener.onEvent(testEvent());

      Awaitility.await("delivery retried and succeeded")
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> assertThat(recordedRequests).hasSize(2));
    } finally {
      listener.shutdown();
    }
  }

  @Test
  void givesUpAfterMaxAttempts() {
    when(config.maxAttempts()).thenReturn(2);
    failAlways.set(true);
    WebhookEventListener listener = createListener();
    try {
      listener.onEvent(testEvent());

      Awaitility.await("both attempts performed")
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> assertThat(recordedRequests).hasSize(2));

      // No further attempts beyond maxAttempts.
      assertThat(recordedRequests)
          .allSatisfy(r -> assertThat(r.body()).isEqualTo(recordedRequests.getLast().body()));
    } finally {
      listener.shutdown();
    }
  }
}
