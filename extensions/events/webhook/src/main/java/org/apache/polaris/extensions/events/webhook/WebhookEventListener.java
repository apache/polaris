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
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.eclipse.microprofile.rest.client.RestClientBuilder;
import org.eclipse.microprofile.rest.client.ext.ResponseExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Delivers Polaris events to an HTTP(S) endpoint as JSON POSTs, optionally signed with HMAC-SHA256.
 *
 * <p>Delivery is best-effort and bounded: concurrency and pending work are capped; retries use
 * exponential backoff with full jitter for transient failures only. Ordering is not guaranteed.
 * In-flight and pending work are not durable across restarts.
 */
@ApplicationScoped
@Identifier("webhook")
public class WebhookEventListener implements PolarisEventListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebhookEventListener.class);

  static final String SIGNATURE_HEADER = "X-Polaris-Signature-256";
  static final String EVENT_TYPE_HEADER = "X-Polaris-Event";

  /** CloudEvents spec version the payload envelope is shaped after. */
  static final String SPEC_VERSION = "1.0";

  /** CloudEvents {@code source} attribute identifying Polaris as the event producer. */
  static final String EVENT_SOURCE = "org.apache.polaris";

  private static final ObjectMapper MAPPER = JsonMapper.shared();

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
  private final Duration timeout;
  private final int maxAttempts;
  private final Duration retryBackoff;
  private final Duration maxRetryBackoff;
  private final int maxConcurrent;
  private final int maxPending;
  private final boolean requireHttps;
  private final Clock clock;
  private final MeterRegistry meterRegistry;
  private final SecureRandom random = new SecureRandom();

  private final AtomicInteger pending = new AtomicInteger();
  private final AtomicInteger inFlight = new AtomicInteger();

  private WebhookTransport transport;
  private ExecutorService deliveryExecutor;
  private ScheduledExecutorService retryExecutor;

  private Counter successCounter;
  private Counter failureCounter;
  private Counter retryCounter;
  private Counter dropCounter;
  private Timer deliveryTimer;

  @Inject
  public WebhookEventListener(
      WebhookEventListenerConfiguration config, Clock clock, MeterRegistry meterRegistry) {
    this.endpoint = config.endpoint().orElse(null);
    this.secret = config.secret().orElse(null);
    this.headers = Map.copyOf(config.headers());
    this.timeout = config.timeout();
    this.maxAttempts = config.maxAttempts();
    this.retryBackoff = config.retryBackoff();
    this.maxRetryBackoff = config.maxRetryBackoff();
    this.maxConcurrent = Math.max(1, config.maxConcurrent());
    this.maxPending = Math.max(1, config.maxPending());
    this.requireHttps = config.requireHttps();
    this.clock = clock;
    this.meterRegistry = meterRegistry;
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
    this.deliveryExecutor =
        Executors.newFixedThreadPool(
            maxConcurrent,
            runnable -> {
              Thread thread = new Thread(runnable, "polaris-webhook-event-listener-delivery");
              thread.setDaemon(true);
              return thread;
            });
    this.retryExecutor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "polaris-webhook-event-listener-retry");
              thread.setDaemon(true);
              return thread;
            });
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
            .description("Scheduled webhook delivery retries")
            .register(meterRegistry);
    dropCounter =
        Counter.builder("polaris.event.webhook.drops")
            .description("Events dropped (queue full or non-retryable failure)")
            .register(meterRegistry);
    deliveryTimer =
        Timer.builder("polaris.event.webhook.delivery")
            .description("Webhook HTTP attempt latency")
            .register(meterRegistry);
    Gauge.builder("polaris.event.webhook.pending", pending, AtomicInteger::get)
        .description("Events accepted but not yet finished (in flight or awaiting retry)")
        .register(meterRegistry);
    Gauge.builder("polaris.event.webhook.in_flight", inFlight, AtomicInteger::get)
        .description("Concurrent HTTP attempts in progress")
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

  protected WebhookTransport createTransport() {
    WebhookHttpClient client =
        RestClientBuilder.newBuilder()
            .baseUri(endpoint)
            .connectTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .readTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .register(new CustomHeadersFilter(headers))
            .register(new NoopResponseExceptionMapper())
            .build(WebhookHttpClient.class);
    return new RestClientTransport(client);
  }

  private static final class RestClientTransport implements WebhookTransport {
    private final WebhookHttpClient client;

    private RestClientTransport(WebhookHttpClient client) {
      this.client = client;
    }

    @Override
    public Result post(String eventType, String signature, String payload) {
      try (Response response = client.post(eventType, signature, payload)) {
        return new Result(response.getStatus(), response.getHeaderString("Retry-After"));
      }
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
      customHeaders.forEach(
          (name, value) -> {
            if (name != null && !isReservedHeader(name)) {
              requestContext.getHeaders().putSingle(name, value);
            } else if (name != null) {
              LOGGER.debug("Ignoring reserved custom webhook header '{}'", name);
            }
          });
    }
  }

  /**
   * Prevents the REST client from turning non-2xx responses into exceptions; the listener
   * classifies status codes itself for retry decisions.
   */
  private static final class NoopResponseExceptionMapper
      implements ResponseExceptionMapper<RuntimeException> {
    @Override
    public RuntimeException toThrowable(Response response) {
      return null;
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
    if (retryExecutor != null) {
      retryExecutor.shutdownNow();
      retryExecutor = null;
    }
    if (deliveryExecutor != null) {
      deliveryExecutor.shutdownNow();
      deliveryExecutor = null;
    }
  }

  @Override
  public void onEvent(PolarisEvent event) {
    if (!tryAccept()) {
      dropCounter.increment();
      LOGGER.warn(
          "Webhook pending capacity full ({}); dropping event type={}",
          maxPending,
          event.type().name());
      return;
    }
    String payload = toJson(event);
    if (payload == null) {
      releaseAccepted();
      dropCounter.increment();
      return;
    }
    scheduleDelivery(payload, event.type().name(), 1, /*delayMs*/ 0);
  }

  /** Try to reserve a pending slot for a new event. */
  private boolean tryAccept() {
    for (; ; ) {
      int current = pending.get();
      if (current >= maxPending) {
        return false;
      }
      if (pending.compareAndSet(current, current + 1)) {
        return true;
      }
    }
  }

  private void releaseAccepted() {
    pending.decrementAndGet();
  }

  private void scheduleDelivery(String payload, String eventType, int attempt, long delayMs) {
    ExecutorService delivery = deliveryExecutor;
    ScheduledExecutorService retry = retryExecutor;
    if (delivery == null || delivery.isShutdown()) {
      releaseAccepted();
      dropCounter.increment();
      return;
    }
    Runnable task = () -> deliverWithConcurrency(payload, eventType, attempt);
    if (delayMs <= 0) {
      try {
        delivery.execute(task);
      } catch (RuntimeException e) {
        releaseAccepted();
        dropCounter.increment();
      }
      return;
    }
    if (retry == null || retry.isShutdown()) {
      releaseAccepted();
      dropCounter.increment();
      return;
    }
    // Retries are best-effort; the Future is not joined (delivery completion is handled in-task).
    try {
      var unused =
          retry.schedule(
              () -> {
                ExecutorService d = deliveryExecutor;
                if (d == null || d.isShutdown()) {
                  releaseAccepted();
                  dropCounter.increment();
                  return;
                }
                try {
                  d.execute(task);
                } catch (RuntimeException e) {
                  releaseAccepted();
                  dropCounter.increment();
                }
              },
              delayMs,
              TimeUnit.MILLISECONDS);
    } catch (RuntimeException e) {
      releaseAccepted();
      dropCounter.increment();
    }
  }

  private void deliverWithConcurrency(String payload, String eventType, int attempt) {
    inFlight.incrementAndGet();
    long startNanos = System.nanoTime();
    String signature = secret != null ? "sha256=" + sign(payload, secret) : null;
    try {
      WebhookTransport.Result result = transport.post(eventType, signature, payload);
      deliveryTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
      int status = result.status();
      if (status >= 200 && status < 300) {
        successCounter.increment();
        releaseAccepted();
        return;
      }
      handleFailure(payload, eventType, attempt, status, result.retryAfter(), null);
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      deliveryTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
      handleFailure(payload, eventType, attempt, null, null, e);
    } finally {
      inFlight.decrementAndGet();
    }
  }

  private void handleFailure(
      String payload,
      String eventType,
      int attempt,
      Integer status,
      String retryAfter,
      Throwable error) {
    boolean transientFailure = isTransient(status, error);
    if (transientFailure && attempt < maxAttempts) {
      long delayMs = computeRetryDelayMillis(attempt, retryAfter);
      retryCounter.increment();
      LOGGER.debug(
          "Webhook delivery to {} failed (attempt {}/{}, status: {}); retrying in {} ms",
          endpoint,
          attempt,
          maxAttempts,
          status,
          delayMs,
          error);
      scheduleDelivery(payload, eventType, attempt + 1, delayMs);
      return;
    }
    failureCounter.increment();
    releaseAccepted();
    LOGGER.error(
        "Webhook delivery to {} failed after {} attempt(s), dropping event (status: {}, transient:"
            + " {})",
        endpoint,
        attempt,
        status,
        transientFailure,
        error);
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
  long computeRetryDelayMillis(int attempt, String retryAfter) {
    Optional<Long> retryAfterDelay = parseRetryAfterMillis(retryAfter, clock);
    if (retryAfterDelay.isPresent()) {
      return Math.min(retryAfterDelay.get(), maxRetryBackoff.toMillis());
    }
    long exp = retryBackoff.toMillis() << Math.min(attempt - 1, 16);
    long capped = Math.min(exp, maxRetryBackoff.toMillis());
    // Full jitter: random in [0, capped]
    if (capped <= 0) {
      return 0;
    }
    return (long) (random.nextDouble() * capped);
  }

  /**
   * Parses the {@code Retry-After} header value in either of its two standard forms: delta-seconds
   * or HTTP-date (RFC 9110). Returns the requested delay in milliseconds, clamped at zero for dates
   * in the past, or empty when the header is absent or malformed.
   */
  @VisibleForTesting
  static Optional<Long> parseRetryAfterMillis(String retryAfter, Clock clock) {
    if (retryAfter == null) {
      return Optional.empty();
    }
    String value = retryAfter.trim();
    try {
      long seconds = Long.parseLong(value);
      if (seconds < 0) {
        return Optional.empty();
      }
      return Optional.of(TimeUnit.SECONDS.toMillis(seconds));
    } catch (NumberFormatException e) {
      // Not delta-seconds; try HTTP-date below.
    }
    try {
      long delayMs =
          ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                  .toInstant()
                  .toEpochMilli()
              - clock.millis();
      return Optional.of(Math.max(delayMs, 0L));
    } catch (DateTimeParseException e) {
      return Optional.empty();
    }
  }

  @VisibleForTesting
  static boolean isReservedHeader(String name) {
    return RESERVED_HEADERS.contains(name.toLowerCase(Locale.ROOT));
  }

  @VisibleForTesting
  String toJson(PolarisEvent event) {
    // ObjectNode preserves insertion order: core CloudEvents attributes first, then extension
    // attributes (compliant names: lowercase alphanumerics).
    ObjectNode root = MAPPER.createObjectNode();
    root.put("specversion", SPEC_VERSION);
    root.put("id", event.metadata().eventId().toString());
    root.put("type", event.type().name());
    root.put("source", EVENT_SOURCE);
    // Original event time from metadata (not delivery time), RFC 3339 per CloudEvents.
    root.put("time", event.metadata().timestamp().toString());
    root.put("deliverytime", clock.instant().toString());
    root.put("realmid", event.metadata().realmId());
    event
        .attributes()
        .get(EventAttributes.TABLE_IDENTIFIER)
        .map(TableIdentifier::toString)
        .ifPresent(id -> root.put("tableidentifier", id));
    event
        .metadata()
        .user()
        .ifPresent(
            p -> {
              root.put("principal", p.getName());
              root.set("activatedroles", MAPPER.valueToTree(p.getRoles()));
            });
    event.metadata().requestId().ifPresent(id -> root.put("requestid", id));
    try {
      return MAPPER.writeValueAsString(root);
    } catch (JacksonException e) {
      LOGGER.error("Error processing event into JSON string: ", e);
      LOGGER.debug("Failed to convert the following object into JSON string: {}", root);
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
  int pendingCount() {
    return pending.get();
  }

  @VisibleForTesting
  MeterRegistry meterRegistry() {
    return meterRegistry;
  }
}
