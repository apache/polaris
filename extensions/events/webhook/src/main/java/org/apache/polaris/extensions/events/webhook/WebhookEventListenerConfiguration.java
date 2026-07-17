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

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import jakarta.enterprise.context.ApplicationScoped;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Configuration interface for the webhook event listener integration.
 *
 * <p>Deliveries are performed asynchronously and do not block the event executor; in-flight
 * requests to a slow or unreachable endpoint are bounded by {@link #timeout()}. Delivery is
 * best-effort: retries are kept in memory only, so pending retries are lost on shutdown, and an
 * event that exhausts all attempts is dropped (and logged). Deployments that need stronger delivery
 * guarantees should also enable the {@code persistence-in-memory-buffer} event listener. HTTP
 * redirects are not followed; the endpoint must respond directly.
 */
@StaticInitSafe
@ConfigMapping(prefix = "polaris.event-listener.webhook")
@ApplicationScoped
public interface WebhookEventListenerConfiguration {

  /**
   * Returns the HTTP(S) endpoint that webhook events are delivered to.
   *
   * <p>Each event is sent as an HTTP POST request with a JSON body. The endpoint must return a 2xx
   * status code for the delivery to be considered successful; any other status code (or a
   * connection error) triggers a retry. HTTPS is strongly recommended because the payload may
   * contain sensitive audit data.
   *
   * <p>This property is required when the {@code webhook} event listener is enabled.
   *
   * <p>Configuration property: {@code polaris.event-listener.webhook.endpoint}
   *
   * @return an optional URI of the webhook endpoint
   */
  @WithName("endpoint")
  Optional<URI> endpoint();

  /**
   * Returns the shared secret used to sign webhook payloads.
   *
   * <p>When set, each request carries an {@code X-Polaris-Signature-256} header containing the hex
   * encoded HMAC-SHA256 of the request body, prefixed with {@code sha256=}. Receivers should
   * recompute the HMAC over the raw request body and compare it against this header to verify
   * authenticity and integrity. If not set, no signature header is sent.
   *
   * <p>Configuration property: {@code polaris.event-listener.webhook.secret}
   *
   * @return an optional String containing the HMAC signing secret
   */
  @WithName("secret")
  Optional<String> secret();

  /**
   * Returns additional HTTP headers to send with every webhook request.
   *
   * <p>This is typically used to authenticate against the receiver, for example
   * `polaris.event-listener.webhook.headers."Authorization"=Bearer <token>` or
   * `polaris.event-listener.webhook.headers."Authorization"=Splunk <hec-token>`. Header values may
   * contain secrets; protect them like any other Polaris credential.
   *
   * <p>Configuration property: {@code polaris.event-listener.webhook.headers."<header-name>"}
   *
   * @return a map of header names to header values
   */
  @WithName("headers")
  Map<String, String> headers();

  /**
   * Returns the timeout for a single webhook delivery attempt.
   *
   * <p>If a delivery attempt does not complete within this duration, it is treated as failed and
   * the retry schedule applies.
   *
   * <p>Configuration property: {@code polaris.event-listener.webhook.timeout}
   *
   * @return the delivery attempt timeout
   */
  @WithName("timeout")
  @WithDefault("10s")
  Duration timeout();

  /**
   * Returns the maximum number of delivery attempts per event, including the initial attempt.
   *
   * <p>When all attempts fail, the event is dropped and an error is logged.
   *
   * <p>Configuration property: {@code polaris.event-listener.webhook.max-attempts}
   *
   * @return the maximum number of delivery attempts
   */
  @WithName("max-attempts")
  @WithDefault("3")
  int maxAttempts();

  /**
   * Returns the initial backoff between delivery attempts.
   *
   * <p>The backoff doubles after each failed attempt (exponential backoff).
   *
   * <p>Configuration property: {@code polaris.event-listener.webhook.retry-backoff}
   *
   * @return the initial retry backoff
   */
  @WithName("retry-backoff")
  @WithDefault("1s")
  Duration retryBackoff();
}
