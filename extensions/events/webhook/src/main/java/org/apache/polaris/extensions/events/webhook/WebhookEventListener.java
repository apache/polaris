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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delivers Polaris events to an HTTP(S) endpoint as JSON POST requests, optionally signed with
 * HMAC-SHA256. Failed deliveries (non-2xx responses or connection errors) are retried with
 * exponential backoff; events that exhaust all attempts are dropped and logged.
 */
@ApplicationScoped
@Identifier("webhook")
public class WebhookEventListener implements PolarisEventListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebhookEventListener.class);

  static final String SIGNATURE_HEADER = "X-Polaris-Signature-256";
  static final String EVENT_TYPE_HEADER = "X-Polaris-Event";

  private final URI endpoint;
  private final String secret;
  private final Map<String, String> headers;
  private final Duration timeout;
  private final int maxAttempts;
  private final Duration retryBackoff;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  private HttpClient client;
  private ScheduledExecutorService retryExecutor;

  @Inject
  public WebhookEventListener(
      WebhookEventListenerConfiguration config, ObjectMapper objectMapper, Clock clock) {
    this.endpoint = config.endpoint().orElse(null);
    this.secret = config.secret().orElse(null);
    this.headers = config.headers();
    this.timeout = config.timeout();
    this.maxAttempts = config.maxAttempts();
    this.retryBackoff = config.retryBackoff();
    this.objectMapper = objectMapper;
    this.clock = clock;
  }

  @PostConstruct
  void start() {
    if (endpoint == null) {
      throw new IllegalStateException(
          "The webhook event listener is enabled but polaris.event-listener.webhook.endpoint is"
              + " not set");
    }
    this.client = createHttpClient();
    this.retryExecutor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "polaris-webhook-event-listener-retry");
              thread.setDaemon(true);
              return thread;
            });
  }

  protected HttpClient createHttpClient() {
    return HttpClient.newBuilder().connectTimeout(timeout).build();
  }

  @PreDestroy
  void shutdown() {
    if (retryExecutor != null) {
      retryExecutor.shutdownNow();
      retryExecutor = null;
    }
  }

  @Override
  public void onEvent(PolarisEvent event) {
    String payload = toJson(event);
    if (payload == null) {
      return;
    }
    send(payload, event.type().name(), 1);
  }

  private void send(String payload, String eventType, int attempt) {
    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder(endpoint)
            .timeout(timeout)
            .header("Content-Type", "application/json")
            .header(EVENT_TYPE_HEADER, eventType);
    headers.forEach(requestBuilder::header);
    requestBuilder.POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8));
    if (secret != null) {
      requestBuilder.header(SIGNATURE_HEADER, "sha256=" + sign(payload, secret));
    }
    // The delivery future is intentionally not joined: retries are scheduled from the
    // completion callback and failures are only logged.
    var delivery =
        client
            .sendAsync(requestBuilder.build(), HttpResponse.BodyHandlers.discarding())
            .whenComplete(
                (response, error) -> {
                  Integer status = response != null ? response.statusCode() : null;
                  boolean succeeded = status != null && status >= 200 && status < 300;
                  if (succeeded) {
                    return;
                  }
                  if (attempt < maxAttempts) {
                    long delayMillis = retryBackoff.toMillis() << (attempt - 1);
                    LOGGER.debug(
                        "Webhook delivery to {} failed (attempt {}/{}, status: {}); retrying in {} ms",
                        endpoint,
                        attempt,
                        maxAttempts,
                        status,
                        delayMillis,
                        error);
                    var retry =
                        retryExecutor.schedule(
                            () -> send(payload, eventType, attempt + 1),
                            delayMillis,
                            TimeUnit.MILLISECONDS);
                  } else {
                    LOGGER.error(
                        "Webhook delivery to {} failed after {} attempt(s), dropping event"
                            + " (status: {})",
                        endpoint,
                        attempt,
                        status,
                        error);
                  }
                });
  }

  private String toJson(PolarisEvent event) {
    HashMap<String, Object> properties = new HashMap<>();
    properties.put("event_type", event.type().name());
    properties.put("timestamp", clock.millis());
    event
        .attributes()
        .get(EventAttributes.TABLE_IDENTIFIER)
        .map(TableIdentifier::toString)
        .ifPresent(id -> properties.put("table_identifier", id));
    properties.put("realm_id", event.metadata().realmId());
    event
        .metadata()
        .user()
        .ifPresent(
            p -> {
              properties.put("principal", p.getName());
              properties.put("activated_roles", p.getRoles());
            });
    event.metadata().requestId().ifPresent(id -> properties.put("request_id", id));
    try {
      return objectMapper.writeValueAsString(properties);
    } catch (JsonProcessingException e) {
      LOGGER.error("Error processing event into JSON string: ", e);
      LOGGER.debug("Failed to convert the following object into JSON string: {}", properties);
      return null;
    }
  }

  static String sign(String payload, String secret) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
      byte[] digest = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        hex.append(Character.forDigit((b >> 4) & 0xF, 16));
        hex.append(Character.forDigit(b & 0xF, 16));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new IllegalStateException("Failed to sign webhook payload", e);
    }
  }
}
