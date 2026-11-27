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

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Listener checks that do not need HTTP. Live POSTs go through production {@code createTransport()}
 * in {@link WebhookEventListenerQuarkusTest} and {@code WebhookEventListenerIT}.
 */
class WebhookEventListenerTest {

  @Mock private WebhookEventListenerConfiguration config;

  private AutoCloseable mockitoContext;
  private SimpleMeterRegistry meterRegistry;

  @BeforeEach
  void setUp() {
    mockitoContext = MockitoAnnotations.openMocks(this);
    meterRegistry = new SimpleMeterRegistry();
    when(config.endpoint()).thenReturn(Optional.of(URI.create("http://127.0.0.1/hook")));
    when(config.secret()).thenReturn(Optional.empty());
    when(config.headers()).thenReturn(Map.of());
    when(config.timeout()).thenReturn(Duration.ofSeconds(5));
    when(config.requireHttps()).thenReturn(false);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (mockitoContext != null) {
      mockitoContext.close();
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
