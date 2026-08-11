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

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
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
import org.apache.polaris.core.collection.MutableAttributeMap;
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
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class WebhookEventListenerTest {

  private static final String REALM = "test-realm";
  private static final String TEST_USER = "test-user";
  private static final String SECRET = "webhook-signing-secret";
  private static final UUID EVENT_ID = UUID.fromString("11111111-2222-3333-4444-555555555555");
  private static final Instant EVENT_TIME = Instant.parse("2026-01-15T12:00:00Z");
  private static final PolarisPrincipal PRINCIPAL =
      PolarisPrincipal.of(TEST_USER, Map.of(), Set.of("role1", "role2"));
  private static final Clock CLOCK = Clock.systemUTC();

  private static final JsonMapper OBJECT_MAPPER = JsonMapper.shared();

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
        new WebhookEventListener(config, CLOCK, meterRegistry) {
          @Override
          protected WebhookTransport createTransport() {
            return new JdkTransport(endpoint(), config.timeout(), config.headers());
          }
        };
    listener.start();
    return listener;
  }

  /**
   * JDK-HttpClient-based transport for tests: the Quarkus REST client used in production needs a
   * ServiceLoader-provided builder implementation that only exists inside a running Quarkus app.
   */
  private static final class JdkTransport implements WebhookEventListener.WebhookTransport {
    private final HttpClient client;
    private final URI endpoint;
    private final Duration timeout;
    private final Map<String, String> headers;

    private JdkTransport(URI endpoint, Duration timeout, Map<String, String> headers) {
      this.client =
          HttpClient.newBuilder()
              .connectTimeout(timeout)
              .followRedirects(HttpClient.Redirect.NEVER)
              .build();
      this.endpoint = endpoint;
      this.timeout = timeout;
      this.headers = headers;
    }

    @Override
    public Result post(String eventType, String signature, String payload) throws Exception {
      HttpRequest.Builder requestBuilder =
          HttpRequest.newBuilder(endpoint)
              .timeout(timeout)
              .header("Content-Type", "application/json")
              .header(WebhookEventListener.EVENT_TYPE_HEADER, eventType)
              .POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8));
      if (signature != null) {
        requestBuilder.header(WebhookEventListener.SIGNATURE_HEADER, signature);
      }
      headers.forEach(
          (name, value) -> {
            if (!WebhookEventListener.isReservedHeader(name)) {
              requestBuilder.header(name, value);
            }
          });
      HttpResponse<Void> response =
          client.send(requestBuilder.build(), HttpResponse.BodyHandlers.discarding());
      return new Result(
          response.statusCode(), response.headers().firstValue("Retry-After").orElse(null));
    }
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
        MutableAttributeMap.builder()
            .put(EventAttributes.CATALOG_NAME, "test_catalog")
            .put(EventAttributes.TABLE_IDENTIFIER, TableIdentifier.of("test_ns", "test_table"))
            .build());
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
      assertThat(json.get("specversion").asString()).isEqualTo(WebhookEventListener.SPEC_VERSION);
      assertThat(json.get("id").asString()).isEqualTo(EVENT_ID.toString());
      assertThat(json.get("time").asString()).isEqualTo(EVENT_TIME.toString());
      assertThat(json.get("deliverytime").asString()).isNotBlank();
      assertThat(json.get("source").asString()).isEqualTo(WebhookEventListener.EVENT_SOURCE);
      assertThat(json.get("type").asString())
          .isEqualTo(PolarisEventType.AFTER_REFRESH_TABLE.name());
      assertThat(json.get("realmid").asString()).isEqualTo(REALM);
      assertThat(json.get("principal").asString()).isEqualTo(TEST_USER);
      assertThat(json.get("activatedroles")).hasSize(2);
      assertThat(json.get("tableidentifier").asString()).isEqualTo("test_ns.test_table");
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
    WebhookEventListener listener = new WebhookEventListener(config, CLOCK, meterRegistry);
    assertThatThrownBy(listener::start)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("polaris.event-listener.webhook.endpoint");
  }

  @Test
  void failsFastWhenHttpsRequiredButHttpConfigured() {
    when(config.requireHttps()).thenReturn(true);
    WebhookEventListener listener = new WebhookEventListener(config, CLOCK, meterRegistry);
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
    assertThat(WebhookEventListener.parseRetryAfterMillis("3", CLOCK)).contains(3000L);
  }

  @Test
  void parseRetryAfterHttpDate() {
    Clock fixed = Clock.fixed(EVENT_TIME, ZoneOffset.UTC);
    assertThat(WebhookEventListener.parseRetryAfterMillis("Thu, 15 Jan 2026 12:00:30 GMT", fixed))
        .contains(30000L);
    // Dates in the past clamp to zero delay.
    assertThat(WebhookEventListener.parseRetryAfterMillis("Thu, 15 Jan 2026 11:59:00 GMT", fixed))
        .contains(0L);
    // Malformed values are ignored so computed backoff applies.
    assertThat(WebhookEventListener.parseRetryAfterMillis("not-a-date", fixed)).isEmpty();
    // Absent header is ignored as well.
    assertThat(WebhookEventListener.parseRetryAfterMillis(null, fixed)).isEmpty();
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
