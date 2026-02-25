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
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Idempotency configuration.
 *
 * <p>Response replay persists and replays the full HTTP response entity for completed requests.
 * Responses are not truncated, because truncation would break clients. Any practical size limits
 * are therefore imposed by the chosen {@code IdempotencyStore} implementation / backing database.
 * If persistence fails (for example due to store limits), replay may be unavailable for that key.
 *
 * <h2>Single-node vs multi-node deployments</h2>
 *
 * <p>In a multi-node deployment (multiple Polaris replicas), idempotency records must be shared
 * across nodes via a durable store (for example JDBC + Postgres). Each node must use a different
 * executor id so ownership/heartbeats can be attributed correctly.
 *
 * <p>Heartbeats should be enabled in multi-node deployments to prevent long-running requests from
 * looking stale after routing changes, pod restarts, or transient pauses.
 *
 * <p>Purge is a best-effort background cleanup. In multi-node deployments, enabling purge on every
 * replica can create unnecessary contention; consider running purge in only one replica or via an
 * external scheduled job.
 */
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

  /**
   * Default TTL for newly reserved keys.
   *
   * <p>Examples: {@code PT5M}, {@code 300S}.
   */
  @WithDefault("PT5M")
  Duration ttlSeconds();

  /**
   * Additional grace added to {@link #ttlSeconds()} when reserving keys.
   *
   * <p>This extends retention slightly to tolerate clock skew and queued retries while keeping the
   * advertised lifetime unchanged.
   *
   * <p>Examples: {@code PT0S}, {@code 10S}.
   */
  @WithDefault("PT0S")
  Duration ttlGraceSeconds();

  /**
   * Executor identifier to store alongside reservations (e.g. pod/instance id).
   *
   * <p>If unset or blank, the service derives a best-effort identifier from environment/host info
   * (for example {@code $POD_NAME} / {@code $HOSTNAME} plus the process id).
   *
   * <p>In multi-node deployments, executor ids must be unique per replica.
   */
  Optional<String> executorId();

  /**
   * Maximum time to wait for an in-progress idempotency key to finalize before returning a
   * retryable response.
   */
  @WithDefault("PT30S")
  Duration inProgressWaitSeconds();

  /**
   * Lease TTL for considering an in-progress owner "active" based on {@code heartbeatAt}.
   *
   * <p>If a duplicate observes {@code now - heartbeatAt > leaseTtlSeconds}, the owner is treated as
   * stale and the server should not wait indefinitely.
   */
  @WithDefault("PT25S")
  Duration leaseTtlSeconds();

  /**
   * Response headers that are persisted and replayed (exact names).
   *
   * <p>Only the first header value is stored and replayed.
   */
  @WithDefault("Content-Type")
  List<String> responseHeaderAllowlist();

  /**
   * Enable periodic heartbeats while a request is in progress.
   *
   * <p>Recommended for multi-node deployments.
   */
  @WithDefault("false")
  boolean heartbeatEnabled();

  /**
   * Heartbeat interval while a request is in progress.
   *
   * <p>Examples: {@code PT5S}, {@code 1S}.
   *
   * <p>In multi-node deployments, this should be shorter than {@link #leaseTtlSeconds()}.
   */
  @WithDefault("PT5S")
  Duration heartbeatIntervalSeconds();

  /**
   * Enable periodic purge of expired idempotency records.
   *
   * <p>In multi-node deployments, enabling purge on all replicas may cause unnecessary contention.
   */
  @WithDefault("false")
  boolean purgeEnabled();

  /**
   * Purge interval.
   *
   * <p>Examples: {@code PT1M}, {@code 60S}.
   */
  @WithDefault("PT1M")
  Duration purgeIntervalSeconds();

  /**
   * Purge records expired strictly before (now - grace).
   *
   * <p>Allows keeping just-expired keys around a bit longer to tolerate clock skew.
   */
  @WithDefault("PT0S")
  Duration purgeGraceSeconds();

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
