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
package org.apache.polaris.service.idempotency;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.List;

@ConfigMapping(prefix = "polaris.idempotency")
public interface IdempotencyConfiguration {

  /**
   * Allowlist of endpoint scopes where idempotency is enforced.
   *
   * <p>This is an optional safety/rollout control: it limits idempotency to specific endpoint
   * prefixes and provides a stable {@link Scope#operationType()} for request binding.
   *
   * <p>If empty, the filter falls back to applying idempotency for Iceberg REST catalog mutating
   * endpoints only.
   */
  List<Scope> scopes();

  /** Enable HTTP idempotency at the request/response filter layer. */
  @WithDefault("false")
  boolean enabled();

  /** Request header name containing the client-provided idempotency key. */
  @WithDefault("Idempotency-Key")
  String keyHeader();

  /** Default TTL (seconds) for newly reserved keys. */
  @WithDefault("300")
  long ttlSeconds();

  /**
   * Additional grace (seconds) added to {@link #ttlSeconds()} when reserving keys.
   *
   * <p>This extends retention slightly to tolerate clock skew and queued retries while keeping the
   * advertised lifetime unchanged.
   */
  @WithDefault("0")
  long ttlGraceSeconds();

  /** Executor identifier to store alongside reservations (e.g. pod/instance id). */
  @WithDefault("http")
  String executorId();

  /**
   * Maximum time to wait for an in-progress idempotency key to finalize before returning a
   * retryable response.
   */
  @WithDefault("30")
  long inProgressWaitSeconds();

  /**
   * Lease TTL (seconds) for considering an in-progress owner "active" based on {@code heartbeatAt}.
   *
   * <p>If a duplicate observes {@code now - heartbeatAt > leaseTtlSeconds}, the owner is treated as
   * stale and the server should not wait indefinitely.
   */
  @WithDefault("25")
  long leaseTtlSeconds();

  /**
   * Response headers that are persisted and replayed (exact names).
   *
   * <p>Only the first header value is stored and replayed.
   */
  @WithDefault("Content-Type")
  List<String> responseHeaderAllowlist();

  /** Max number of response body characters to store for replay. */
  @WithDefault("8192")
  int maxResponseBodyChars();

  /** Enable periodic heartbeats while a request is in progress. */
  @WithDefault("false")
  boolean heartbeatEnabled();

  /** Heartbeat interval (seconds) while a request is in progress. */
  @WithDefault("5")
  long heartbeatIntervalSeconds();

  /** Enable periodic purge of expired idempotency records. */
  @WithDefault("false")
  boolean purgeEnabled();

  /** Purge interval (seconds). */
  @WithDefault("60")
  long purgeIntervalSeconds();

  /**
   * Purge records expired strictly before (now - grace).
   *
   * <p>Allows keeping just-expired keys around a bit longer to tolerate clock skew.
   */
  @WithDefault("0")
  long purgeGraceSeconds();

  interface Scope {
    /** HTTP method (e.g. POST). */
    String method();

    /** Request path prefix (no scheme/host; no leading slash required). */
    String pathPrefix();

    /**
     * Stable operation identifier used for the idempotency binding.
     *
     * <p>This should be stable across refactors and should not be derived from the HTTP method.
     */
    String operationType();
  }
}
