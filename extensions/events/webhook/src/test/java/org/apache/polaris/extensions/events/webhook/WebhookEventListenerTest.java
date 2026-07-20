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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.runtime.configuration.MemorySize;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
  private static final UUID EVENT_ID = UUID.fromString("11111111-2222-3333-4444-555555555555");
  private static final Instant EVENT_TIME = Instant.parse("2026-01-15T12:00:00Z");
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
  private AtomicInteger statusToReturn;
  private SimpleMeterRegistry meterRegistry;

  private record RecordedRequest(
      String body, String signature, String eventType, String authorization) {}

  @BeforeEach
  void setUp() throws Exception {
    mockitoContext = MockitoAnnotations.openMocks(this);
    recordedRequests = new CopyOnWriteArrayList<>();
    failNext = new AtomicBoolean(false);
    failAlways = new AtomicBoolean(false);
    statusToReturn = new AtomicInteger(200);
    meterRegistry = new SimpleMeterRegistry();
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
          int status;
          if (failAlways.get()) {
            status = 500;
          } else if (failNext.getAndSet(false)) {
            status = 500;
          } else {
            status = statusToReturn.get();
          }
          exchange.sendResponseHeaders(status, -1);
          exchange.close();
        });
    server.start();

    when(config.endpoint()).thenReturn(Optional.of(endpoint()));
    when(config.secret()).thenReturn(Optional.of(SECRET));
    when(config.headers()).thenReturn(Map.of());
    when(config.timeout()).thenReturn(Duration.ofSeconds(5));
    when(config.maxAttempts()).thenReturn(3);
    when(config.retryBackoff()).thenReturn(Duration.ofMillis(20));
    when(config.maxRetryBackoff()).thenReturn(Duration.ofSeconds(5));
    when(config.maxConcurrent()).thenReturn(4);
    when(config.maxPending()).thenReturn(100);
    when(config.requireHttps()).thenReturn(false);
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
    WebhookEventListener listener =
        new WebhookEventListener(config, OBJECT_MAPPER, CLOCK, meterRegistry);
    listener.start();
    return listener;
  }

  private static PolarisEvent testEvent() {
    return new PolarisEvent(
        PolarisEventType.AFTER_REFRESH_TABLE,
        PolarisEventMetadata.builder()
            .eventId(EVENT_ID)
            .timestamp(EVENT_TIME)
            .realmId(REALM)
            .user(PRINCIPAL)
            .build(),
        new EventAttributeMap()
            .put(EventAttributes.CATALOG_NAME, "test_catalog")
            .put(EventAttributes.TABLE_IDENTIFIER, TableIdentifier.of("test_ns", "test_table")));
  }

  @Test
  void deliversSignedJsonPayloadWithEnvelopeFields() throws Exception {
    WebhookEventListener listener = createListener();
    try {
      listener.onEvent(testEvent());

      Awaitility.await("webhook delivered")
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> assertThat(recordedRequests).hasSize(1));

      RecordedRequest request = recordedRequests.getFirst();
      JsonNode json = OBJECT_MAPPER.readTree(request.body());
      assertThat(json.get("schema_version").asInt()).isEqualTo(WebhookEventListener.SCHEMA_VERSION);
      assertThat(json.get("event_id").asText()).isEqualTo(EVENT_ID.toString());
      assertThat(json.get("timestamp").asLong()).isEqualTo(EVENT_TIME.toEpochMilli());
      assertThat(json.get("delivery_timestamp").asLong()).isPositive();
      assertThat(json.get("event_type").asText())
          .isEqualTo(PolarisEventType.AFTER_REFRESH_TABLE.name());
      assertThat(json.get("realm_id").asText()).isEqualTo(REALM);
      assertThat(json.get("principal").asText()).isEqualTo(TEST_USER);
      assertThat(json.get("table_identifier").asText()).isEqualTo("test_ns.test_table");
      assertThat(request.eventType()).isEqualTo(PolarisEventType.AFTER_REFRESH_TABLE.name());
      assertThat(request.signature())
          .isEqualTo("sha256=" + WebhookEventListener.sign(request.body(), SECRET));
      assertThat(
              meterRegistry
                  .find("polaris.event.webhook.deliveries")
                  .tag("result", "success")
                  .counter()
                  .count())
          .isEqualTo(1.0);
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
    } finally {
      listener.shutdown();
    }
  }

  @Test
  void sendsConfiguredCustomHeadersButNotReservedOverrides() {
    when(config.headers())
        .thenReturn(
            Map.of(
                "Authorization",
                "Splunk test-hec-token",
                WebhookEventListener.SIGNATURE_HEADER,
                "sha256=forged",
                "Content-Type",
                "text/plain"));
    WebhookEventListener listener = createListener();
    try {
      listener.onEvent(testEvent());

      Awaitility.await("webhook delivered")
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> assertThat(recordedRequests).hasSize(1));

      RecordedRequest request = recordedRequests.getFirst();
      assertThat(request.authorization()).isEqualTo("Splunk test-hec-token");
      assertThat(request.signature())
          .isEqualTo("sha256=" + WebhookEventListener.sign(request.body(), SECRET));
      assertThat(request.signature()).doesNotContain("forged");
    } finally {
      listener.shutdown();
    }
  }

  @Test
  void failsFastWhenEndpointNotConfigured() {
    when(config.endpoint()).thenReturn(Optional.empty());
    WebhookEventListener listener =
        new WebhookEventListener(config, OBJECT_MAPPER, CLOCK, meterRegistry);
    assertThatThrownBy(listener::start)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("polaris.event-listener.webhook.endpoint");
  }

  @Test
  void failsFastWhenHttpsRequiredButHttpConfigured() {
    when(config.requireHttps()).thenReturn(true);
    WebhookEventListener listener =
        new WebhookEventListener(config, OBJECT_MAPPER, CLOCK, meterRegistry);
    assertThatThrownBy(listener::start)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("https");
  }

  @Test
  void retriesTransientFailuresUntilSuccess() {
    failNext.set(true);
    WebhookEventListener listener = createListener();
    try {
      listener.onEvent(testEvent());

      Awaitility.await("delivery retried and succeeded")
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> assertThat(recordedRequests).hasSize(2));
      assertThat(meterRegistry.find("polaris.event.webhook.retries").counter().count())
          .isEqualTo(1.0);
    } finally {
      listener.shutdown();
    }
  }

  @Test
  void doesNotRetryPermanentClientErrors() {
    statusToReturn.set(400);
    when(config.maxAttempts()).thenReturn(5);
    WebhookEventListener listener = createListener();
    try {
      listener.onEvent(testEvent());

      Awaitility.await("single attempt only")
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(() -> assertThat(recordedRequests).hasSize(1));
      // Give a short window to ensure no retry is scheduled.
      Awaitility.await().pollDelay(Duration.ofMillis(200)).until(() -> true);
      assertThat(recordedRequests).hasSize(1);
      assertThat(meterRegistry.find("polaris.event.webhook.retries").counter().count())
          .isEqualTo(0.0);
      assertThat(
              meterRegistry
                  .find("polaris.event.webhook.deliveries")
                  .tag("result", "failure")
                  .counter()
                  .count())
          .isEqualTo(1.0);
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
    } finally {
      listener.shutdown();
    }
  }

  @Test
  void dropsWhenPendingCapacityFull() {
    when(config.maxPending()).thenReturn(1);
    when(config.maxConcurrent()).thenReturn(1);
    // Hold the first delivery so the second is dropped for capacity.
    Object gate = new Object();
    AtomicBoolean firstEntered = new AtomicBoolean(false);
    server.removeContext("/hook");
    server.createContext(
        "/hook",
        exchange -> {
          firstEntered.set(true);
          synchronized (gate) {
            try {
              gate.wait(5000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          exchange.sendResponseHeaders(200, -1);
          exchange.close();
        });

    WebhookEventListener listener = createListener();
    try {
      listener.onEvent(testEvent());
      Awaitility.await("first delivery started")
          .atMost(Duration.ofSeconds(5))
          .untilTrue(firstEntered);

      listener.onEvent(testEvent());
      assertThat(meterRegistry.find("polaris.event.webhook.drops").counter().count())
          .isEqualTo(1.0);

      synchronized (gate) {
        gate.notifyAll();
      }
    } finally {
      synchronized (gate) {
        gate.notifyAll();
      }
      listener.shutdown();
    }
  }

  @Test
  void isTransientClassification() {
    assertThat(WebhookEventListener.isTransient(500, null)).isTrue();
    assertThat(WebhookEventListener.isTransient(429, null)).isTrue();
    assertThat(WebhookEventListener.isTransient(408, null)).isTrue();
    assertThat(WebhookEventListener.isTransient(400, null)).isFalse();
    assertThat(WebhookEventListener.isTransient(401, null)).isFalse();
    assertThat(WebhookEventListener.isTransient(404, null)).isFalse();
    assertThat(WebhookEventListener.isTransient(null, new RuntimeException("io"))).isTrue();
  }

  @Test
  void parseRetryAfterSeconds() {
    HttpHeaders headers = HttpHeaders.of(Map.of("Retry-After", List.of("3")), (a, b) -> true);
    assertThat(WebhookEventListener.parseRetryAfterMillis(headers)).contains(3000L);
  }

  @Test
  void validateEndpointHttps() {
    assertThatThrownBy(
            () ->
                WebhookEventListener.validateEndpoint(URI.create("http://example.com/hook"), true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("https");
    WebhookEventListener.validateEndpoint(URI.create("https://example.com/hook"), true);
    WebhookEventListener.validateEndpoint(URI.create("http://localhost/hook"), false);
  }

  @Test
  void reservedHeaderDetection() {
    assertThat(WebhookEventListener.isReservedHeader("X-Polaris-Signature-256")).isTrue();
    assertThat(WebhookEventListener.isReservedHeader("content-type")).isTrue();
    assertThat(WebhookEventListener.isReservedHeader("Authorization")).isFalse();
  }
}
