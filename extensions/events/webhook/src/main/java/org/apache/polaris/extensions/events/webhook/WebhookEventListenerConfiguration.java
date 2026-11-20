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
 * Configuration for the webhook event listener.
 *
 * <p>Delivery is <strong>best-effort and in-process only</strong>: {@code onEvent} POSTs on the
 * shared event-listener executor. Transient failures retry immediately in that call; events that
 * exhaust retries are dropped. In-flight work is not durable and is lost on process restart. This
 * listener is not a durable audit spool and is not strengthened by enabling {@code
 * persistence-in-memory-buffer} (that buffer is a separate listener, not a webhook delivery queue).
 *
 * <p>HTTP redirects are not followed; the endpoint must respond directly. When {@code
 * require-https} is {@code true} (the default), only {@code https} endpoints are accepted.
 */
@StaticInitSafe
@ConfigMapping(prefix = "polaris.event-listener.webhook")
@ApplicationScoped
public interface WebhookEventListenerConfiguration {

  /**
   * HTTP(S) endpoint that receives webhook POSTs with a JSON body.
   *
   * <p>A 2xx response marks success. Transient failures (network errors, 408/425/429/5xx) are
   * retried immediately on the shared event-listener executor; permanent client errors (other 4xx)
   * are not retried.
   *
   * <p>Required when the {@code webhook} event listener is enabled.
   *
   * <p>Configuration property: {@code polaris.event-listener.webhook.endpoint}
   */
  @WithName("endpoint")
  Optional<URI> endpoint();

  /**
   * Shared secret for HMAC-SHA256 signing. When set, each request includes {@code
   * Polaris-Signature-256: sha256=<hex>} over the raw body. HMAC authenticates and protects
   * integrity; it does not encrypt the payload — use HTTPS for confidentiality.
   *
   * <p>Configuration property: {@code polaris.event-listener.webhook.secret}
   */
  @WithName("secret")
  Optional<String> secret();

  /**
   * Extra headers on every request (for example {@code Authorization}). Reserved headers ({@code
   * Content-Type}, {@code Polaris-Signature-256}, {@code Polaris-Event}, {@code Host}, {@code
   * Content-Length}) cannot be overridden. Header values may be secrets; protect them accordingly.
   *
   * <p>Configuration property: {@code polaris.event-listener.webhook.headers."<header-name>"}
   */
  @WithName("headers")
  Map<String, String> headers();

  /**
   * Timeout for a single HTTP attempt. Timed-out attempts count as transient failures.
   *
   * <p>Configuration property: {@code polaris.event-listener.webhook.timeout}
   */
  @WithName("timeout")
  @WithDefault("10s")
  Duration timeout();

  /**
   * When {@code true}, only {@code https} endpoints are allowed (recommended for production). Set
   * {@code false} only for local testing with plain HTTP.
   *
   * <p>Configuration property: {@code polaris.event-listener.webhook.require-https}
   */
  @WithName("require-https")
  @WithDefault("true")
  boolean requireHttps();
}
