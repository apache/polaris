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
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.collection.AttributeMap;
import org.apache.polaris.core.collection.MutableAttributeMap;
import org.apache.polaris.service.events.CloudEventsEventJsonMapper;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadata;
import org.apache.polaris.service.events.PolarisEventType;
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
      PolarisPrincipal.of(TEST_USER, AttributeMap.EMPTY, Set.of("role1", "role2"));

  private static final JsonMapper OBJECT_MAPPER = JsonMapper.shared();

  @Mock private WebhookEventListenerConfiguration config;

  private AutoCloseable mockitoContext;
  private HttpServer server;
  private List<RecordedRequest> recordedRequests;
  private AtomicInteger statusToReturn;
  private SimpleMeterRegistry meterRegistry;

  private record RecordedRequest(
      String body, String signature, String eventType, String authorization) {}

  @BeforeEach
  void setUp() throws Exception {
    mockitoContext = MockitoAnnotations.openMocks(this);
    recordedRequests = new CopyOnWriteArrayList<>();
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
          exchange.sendResponseHeaders(statusToReturn.get(), -1);
          exchange.close();
        });
    server.start();

    when(config.endpoint()).thenReturn(Optional.of(endpoint()));
    when(config.secret()).thenReturn(Optional.of(SECRET));
    when(config.headers()).thenReturn(Map.of());
    when(config.timeout()).thenReturn(Duration.ofSeconds(5));
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
        new WebhookEventListener(config, meterRegistry) {
          @Override
          WebhookTransport createTransport() {
            return new JdkTransport(endpoint(), config.timeout(), config.headers());
          }
        };
    listener.start();
    return listener;
  }

  /**
   * JDK-HttpClient-based transport for listener-behavior tests. Production {@code
   * createTransport()} is exercised by {@link WebhookEventListenerQuarkusTest} and {@code
   * WebhookEventListenerIT}. Status / Retry-After handling on {@code RestClientTransport} is
   * covered by {@link #restClientTransportPreservesStatusFromResponse()} and {@link
   * #restClientTransportPreservesStatusAndRetryAfterFromWebApplicationException()}.
   *
   * <p>Fault Tolerance retries apply through the CDI proxy in production. These tests construct the
   * listener directly, so each {@code onEvent} is a single POST.
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
      headers.forEach((name, value) -> {
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

      assertThat(recordedRequests).hasSize(1);
      RecordedRequest request = recordedRequests.getFirst();
      JsonNode json = OBJECT_MAPPER.readTree(request.body());
      assertThat(json.get("specversion").asString())
          .isEqualTo(CloudEventsEventJsonMapper.SPEC_VERSION);
      assertThat(json.get("id").asString()).isEqualTo(EVENT_ID.toString());
      assertThat(json.get("time").asString()).isEqualTo(EVENT_TIME.toString());
      assertThat(json.get("source").asString()).isEqualTo(CloudEventsEventJsonMapper.SOURCE);
      assertThat(json.get("type").asString())
          .isEqualTo(
              CloudEventsEventJsonMapper.cloudEventsType(PolarisEventType.AFTER_REFRESH_TABLE));
      assertThat(json.get("datacontenttype").asString()).isEqualTo("application/json");
      assertThat(json.has("deliverytime")).isFalse();
      assertThat(json.has("realmid")).isFalse();
      JsonNode data = json.get("data");
      assertThat(data.get("realm_id").asString()).isEqualTo(REALM);
      assertThat(data.get("principal").asString()).isEqualTo(TEST_USER);
      assertThat(data.get("activated_roles")).hasSize(2);
      assertThat(data.get("catalog_name").asString()).isEqualTo("test_catalog");
      assertThat(data.get("table_identifier").isArray()).isTrue();
      assertThat(data.get("event_type").asString())
          .isEqualTo(PolarisEventType.AFTER_REFRESH_TABLE.name());
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

      assertThat(recordedRequests).hasSize(1);
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

      assertThat(recordedRequests).hasSize(1);
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
    WebhookEventListener listener = new WebhookEventListener(config, meterRegistry);
    assertThatThrownBy(listener::start)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("polaris.event-listener.webhook.endpoint");
  }

  @Test
  void failsFastWhenHttpsRequiredButHttpConfigured() {
    when(config.requireHttps()).thenReturn(true);
    WebhookEventListener listener = new WebhookEventListener(config, meterRegistry);
    assertThatThrownBy(listener::start)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("https");
  }

  @Test
  void recordsFailureOnPermanentClientErrorWithoutRetry() {
    statusToReturn.set(400);
    WebhookEventListener listener = createListener();
    try {
      listener.onEvent(testEvent());

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
  void transientStatusThrowsRetryableException() {
    statusToReturn.set(500);
    WebhookEventListener listener = createListener();
    try {
      assertThatThrownBy(() -> listener.deliver("AFTER_REFRESH_TABLE", "{}"))
          .isInstanceOf(WebhookEventListener.TransientWebhookDeliveryException.class);
      assertThat(recordedRequests).hasSize(1);
      assertThat(meterRegistry.find("polaris.event.webhook.retries").counter().count())
          .isEqualTo(1.0);
    } finally {
      listener.shutdown();
    }
  }

  @Test
  void restClientTransportPreservesStatusFromResponse() throws Exception {
    WebhookHttpClient client = (eventType, signature, payload) -> Response.status(400).build();
    WebhookEventListener.WebhookTransport.Result result =
        WebhookEventListener.restClientTransport(client).post("T", null, "{}");
    assertThat(result.status()).isEqualTo(400);
    assertThat(result.retryAfter()).isNull();
    assertThat(WebhookEventListener.isTransient(result.status(), null)).isFalse();
  }

  @Test
  void restClientTransportPreservesStatusAndRetryAfterFromWebApplicationException()
      throws Exception {
    // Simulates the MP default mapper path that disableDefaultMapper(true) avoids in production.
    WebhookHttpClient client =
        (eventType, signature, payload) -> {
          throw new WebApplicationException(
              Response.status(429).header("Retry-After", "7").build());
        };
    WebhookEventListener.WebhookTransport.Result result =
        WebhookEventListener.restClientTransport(client).post("T", null, "{}");
    assertThat(result.status()).isEqualTo(429);
    assertThat(result.retryAfter()).isEqualTo("7");
    assertThat(WebhookEventListener.isTransient(result.status(), null)).isTrue();
  }

  @Test
  void isTransientClassification() {
    assertThat(WebhookEventListener.isTransient(500, null)).isTrue();
    assertThat(WebhookEventListener.isTransient(429, null)).isTrue();
    assertThat(WebhookEventListener.isTransient(408, null)).isTrue();
    assertThat(WebhookEventListener.isTransient(425, null)).isTrue();
    assertThat(WebhookEventListener.isTransient(400, null)).isFalse();
    assertThat(WebhookEventListener.isTransient(401, null)).isFalse();
    assertThat(WebhookEventListener.isTransient(404, null)).isFalse();
    assertThat(WebhookEventListener.isTransient(null, new RuntimeException("io"))).isTrue();
  }

  @Test
  void validateEndpointHttps() {
    assertThatThrownBy(() ->
            WebhookEventListener.validateEndpoint(URI.create("http://example.com/hook"), true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("https");
    WebhookEventListener.validateEndpoint(URI.create("https://example.com/hook"), true);
    WebhookEventListener.validateEndpoint(URI.create("http://localhost/hook"), false);
  }

  @Test
  void reservedHeaderDetection() {
    assertThat(WebhookEventListener.isReservedHeader("Polaris-Signature-256")).isTrue();
    assertThat(WebhookEventListener.isReservedHeader("Polaris-Event")).isTrue();
    assertThat(WebhookEventListener.isReservedHeader("content-type")).isTrue();
    assertThat(WebhookEventListener.isReservedHeader("Authorization")).isFalse();
  }
}
