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

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.faulttolerance.api.ExponentialBackoff;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.polaris.service.events.CloudEventsEventJsonMapper;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.eclipse.microprofile.faulttolerance.Fallback;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;

/**
 * Delivers Polaris events to an HTTP(S) endpoint as JSON POSTs, optionally signed with HMAC-SHA256.
 *
 * <p>The JSON body is the shared {@link CloudEventsEventJsonMapper} envelope. Event time is the
 * CloudEvents {@code time} field from {@code PolarisEventMetadata.timestamp()}.
 *
 * <p>{@code onEvent} POSTs on the shared event-listener executor. Transient failures (network
 * errors, 408/425/429/5xx) retry immediately in that call via SmallRye Fault Tolerance; permanent
 * 4xx responses are not retried. Ordering of events for this listener follows the shared executor.
 * Delivery is best-effort and not durable across restarts.
 */
@ApplicationScoped
@Identifier("webhook")
public class WebhookEventListener implements PolarisEventListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebhookEventListener.class);

  static final String SIGNATURE_HEADER = "Polaris-Signature-256";
  static final String EVENT_TYPE_HEADER = "Polaris-Event";

  private static final CloudEventsEventJsonMapper EVENT_JSON_MAPPER =
      new CloudEventsEventJsonMapper();

  private static final Set<String> RESERVED_HEADERS =
      Set.of(
          "content-type",
          "content-length",
          "host",
          SIGNATURE_HEADER.toLowerCase(Locale.ROOT),
          EVENT_TYPE_HEADER.toLowerCase(Locale.ROOT));

  private final URI endpoint;
  private final String secret;
  private final Map<String, String> headers;
  private final java.time.Duration timeout;
  private final boolean requireHttps;
  private final MeterRegistry meterRegistry;

  private WebhookTransport transport;
  private WebhookEventListener self;

  private Counter successCounter;
  private Counter failureCounter;
  private Counter retryCounter;
  private Counter dropCounter;
  private Timer deliveryTimer;

  @Inject
  public WebhookEventListener(
      WebhookEventListenerConfiguration config, MeterRegistry meterRegistry) {
    this.endpoint = config.endpoint().orElse(null);
    this.secret = config.secret().orElse(null);
    this.headers = Map.copyOf(config.headers());
    this.timeout = config.timeout();
    this.requireHttps = config.requireHttps();
    this.meterRegistry = meterRegistry;
  }

  @Inject
  void setSelf(WebhookEventListener self) {
    this.self = self;
  }

  @PostConstruct
  void start() {
    if (endpoint == null) {
      throw new IllegalStateException(
          "The webhook event listener is enabled but polaris.event-listener.webhook.endpoint is"
              + " not set");
    }
    validateEndpoint(endpoint, requireHttps);
    this.transport = createTransport();
    registerMetrics();
  }

  private void registerMetrics() {
    successCounter =
        Counter.builder("polaris.event.webhook.deliveries")
            .tag("result", "success")
            .description("Successful webhook deliveries")
            .register(meterRegistry);
    failureCounter =
        Counter.builder("polaris.event.webhook.deliveries")
            .tag("result", "failure")
            .description("Failed webhook deliveries after retries exhausted or permanent errors")
            .register(meterRegistry);
    retryCounter =
        Counter.builder("polaris.event.webhook.retries")
            .description("Transient webhook delivery failures that will be retried")
            .register(meterRegistry);
    dropCounter =
        Counter.builder("polaris.event.webhook.drops")
            .description("Events dropped because the payload could not be encoded")
            .register(meterRegistry);
    deliveryTimer =
        Timer.builder("polaris.event.webhook.delivery")
            .description("Webhook HTTP attempt latency")
            .register(meterRegistry);
  }

  @VisibleForTesting
  static void validateEndpoint(URI endpoint, boolean requireHttps) {
    String scheme = endpoint.getScheme();
    if (scheme == null) {
      throw new IllegalStateException(
          "polaris.event-listener.webhook.endpoint must include a scheme (https://...)");
    }
    String lower = scheme.toLowerCase(Locale.ROOT);
    if (requireHttps && !"https".equals(lower)) {
      throw new IllegalStateException(
          "polaris.event-listener.webhook.endpoint must use https when require-https is true"
              + " (got scheme '"
              + scheme
              + "')");
    }
    if (!"https".equals(lower) && !"http".equals(lower)) {
      throw new IllegalStateException(
          "polaris.event-listener.webhook.endpoint must use http or https (got scheme '"
              + scheme
              + "')");
    }
  }

  /**
   * Minimal transport seam over the webhook endpoint. Production uses a Quarkus REST client; tests
   * can substitute a lightweight implementation.
   */
  interface WebhookTransport extends AutoCloseable {
    record Result(int status, String retryAfter) {}

    Result post(String eventType, String signature, String payload) throws Exception;

    @Override
    default void close() throws Exception {}
  }

  WebhookTransport createTransport() {
    // disableDefaultMapper is required: a ResponseExceptionMapper that returns null does not
    // suppress the MP default mapper, which throws on >=400 and drops status / Retry-After.
    WebhookHttpClient client =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(endpoint)
            .connectTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .readTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .followRedirects(false)
            .disableDefaultMapper(true)
            .register(new CustomHeadersFilter(headers))
            .build(WebhookHttpClient.class);
    return new RestClientTransport(client);
  }

  @VisibleForTesting
  static WebhookTransport restClientTransport(WebhookHttpClient client) {
    return new RestClientTransport(client);
  }

  private static final class RestClientTransport implements WebhookTransport {
    private final WebhookHttpClient client;

    private RestClientTransport(WebhookHttpClient client) {
      this.client = client;
    }

    @Override
    public Result post(String eventType, String signature, String payload) {
      try {
        try (Response response = client.post(eventType, signature, payload)) {
          return toResult(response);
        }
      } catch (WebApplicationException e) {
        // If the default mapper is still enabled, non-2xx arrive as WAE with the Response
        // attached. Prefer disableDefaultMapper(true); this keeps status / Retry-After usable.
        Response response = e.getResponse();
        if (response == null) {
          throw e;
        }
        try {
          return toResult(response);
        } finally {
          response.close();
        }
      }
    }

    private static Result toResult(Response response) {
      return new Result(response.getStatus(), response.getHeaderString("Retry-After"));
    }

    @Override
    public void close() throws Exception {
      ((AutoCloseable) client).close();
    }
  }

  /** Adds the configured custom headers to every request, skipping reserved header names. */
  private static final class CustomHeadersFilter implements ClientRequestFilter {
    private final Map<String, String> customHeaders;

    private CustomHeadersFilter(Map<String, String> customHeaders) {
      this.customHeaders = customHeaders;
    }

    @Override
    public void filter(ClientRequestContext requestContext) {
      customHeaders.forEach((name, value) -> {
        if (name != null && !isReservedHeader(name)) {
          requestContext.getHeaders().putSingle(name, value);
        } else if (name != null) {
          LOGGER.debug("Ignoring reserved custom webhook header '{}'", name);
        }
      });
    }
  }

  @PreDestroy
  void shutdown() {
    if (transport != null) {
      try {
        transport.close();
      } catch (Exception e) {
        LOGGER.debug("Failed to close webhook transport", e);
      }
      transport = null;
    }
  }

  @Override
  public void onEvent(PolarisEvent event) {
    String payload = toJson(event);
    if (payload == null) {
      dropCounter.increment();
      return;
    }
    WebhookEventListener target = self != null ? self : this;
    try {
      target.deliver(event.type().name(), payload);
    } catch (WebhookDeliveryException e) {
      onDeliveryError(event.type().name(), payload, e);
    }
  }

  /**
   * POSTs one event. Fault Tolerance retries transient failures immediately on the calling thread
   * (the shared event-listener executor). {@code maxRetries} is additional attempts after the first
   * try (three attempts total, matching the previous default).
   */
  @Retry(
      maxRetries = 2,
      delay = 1000,
      jitter = 200,
      retryOn = TransientWebhookDeliveryException.class,
      abortOn = PermanentWebhookDeliveryException.class)
  @ExponentialBackoff(maxDelay = 60_000)
  @Fallback(fallbackMethod = "onDeliveryError")
  protected void deliver(String eventType, String payload) {
    long startNanos = System.nanoTime();
    String signature = secret != null ? "sha256=" + sign(payload, secret) : null;
    try {
      WebhookTransport.Result result = transport.post(eventType, signature, payload);
      deliveryTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
      int status = result.status();
      if (status >= 200 && status < 300) {
        successCounter.increment();
        return;
      }
      throwForStatus(eventType, status);
    } catch (WebhookDeliveryException e) {
      throw e;
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      deliveryTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
      retryCounter.increment();
      LOGGER.debug("Webhook delivery to {} failed; retrying if attempts remain", endpoint, e);
      throw new TransientWebhookDeliveryException("Webhook delivery failed", e);
    }
  }

  private void throwForStatus(String eventType, int status) {
    if (isTransient(status, null)) {
      retryCounter.increment();
      LOGGER.debug(
          "Webhook delivery to {} failed (status {}); retrying if attempts remain",
          endpoint,
          status);
      throw new TransientWebhookDeliveryException(
          "Webhook delivery of " + eventType + " failed with status " + status);
    }
    throw new PermanentWebhookDeliveryException(
        "Webhook delivery of " + eventType + " failed with status " + status);
  }

  @SuppressWarnings("unused")
  protected void onDeliveryError(String eventType, String payload, Throwable error) {
    failureCounter.increment();
    LOGGER.error(
        "Webhook delivery to {} failed, dropping event type={}", endpoint, eventType, error);
  }

  @VisibleForTesting
  static boolean isTransient(Integer status, Throwable error) {
    if (error != null) {
      return true;
    }
    if (status == null) {
      return true;
    }
    if (status == 408 || status == 425 || status == 429) {
      return true;
    }
    return status >= 500 && status <= 599;
  }

  @VisibleForTesting
  static boolean isReservedHeader(String name) {
    return RESERVED_HEADERS.contains(name.toLowerCase(Locale.ROOT));
  }

  @VisibleForTesting
  String toJson(PolarisEvent event) {
    try {
      return EVENT_JSON_MAPPER.eventToJson(event);
    } catch (JacksonException e) {
      LOGGER.error("Error processing event into JSON string: ", e);
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

  @VisibleForTesting
  MeterRegistry meterRegistry() {
    return meterRegistry;
  }

  static class WebhookDeliveryException extends RuntimeException {
    WebhookDeliveryException(String message) {
      super(message);
    }

    WebhookDeliveryException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  static final class TransientWebhookDeliveryException extends WebhookDeliveryException {
    TransientWebhookDeliveryException(String message) {
      super(message);
    }

    TransientWebhookDeliveryException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  static final class PermanentWebhookDeliveryException extends WebhookDeliveryException {
    PermanentWebhookDeliveryException(String message) {
      super(message);
    }
  }
}
