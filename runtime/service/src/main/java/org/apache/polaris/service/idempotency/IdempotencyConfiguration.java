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
import java.util.Optional;

/**
 * Deploy-time configuration for handler-level idempotency.
 *
 * <p>Reservations are persisted via {@link
 * org.apache.polaris.core.persistence.IdempotencyPersistence}, which sits on the realm's {@link
 * org.apache.polaris.core.persistence.BasePersistence}.
 *
 * <p>All settings here are deployment-wide constants read from the Quarkus configuration tree. They
 * do not vary per-realm or per-catalog. Per-realm or per-catalog overrides can be introduced in a
 * follow-up if/when there is a concrete operator request for it.
 *
 * <h2>Single-node vs multi-node deployments</h2>
 *
 * <p>In a multi-node deployment idempotency records must be shared across nodes via a durable store
 * (for example JDBC + Postgres). Each node must use a different executor id so
 * ownership/cancel/heartbeats can be attributed correctly.
 *
 * <p>Purge is a best-effort background cleanup. In multi-node deployments, enabling purge on every
 * replica can create unnecessary contention; consider running purge in only one replica (via {@link
 * #purgeExecutorId()}) or via an external scheduled job.
 */
@ConfigMapping(prefix = "polaris.idempotency")
public interface IdempotencyConfiguration {

  /**
   * Whether handler-level idempotency is enabled. When {@code false} the handlers ignore the {@code
   * Idempotency-Key} header entirely and never read or write the idempotency store.
   */
  @WithDefault("false")
  boolean enabled();

  /** Request header name containing the client-provided idempotency key. */
  @WithDefault("Idempotency-Key")
  String keyHeader();

  /**
   * Executor identifier to store alongside reservations (e.g. pod / instance id).
   *
   * <p>If unset or blank, the service derives a best-effort identifier from environment / host info
   * (for example {@code $POD_NAME} / {@code $HOSTNAME} plus the process id).
   *
   * <p>In multi-node deployments, executor ids must be unique per replica.
   */
  Optional<String> executorId();

  /**
   * Optional executor id that is allowed to run purge.
   *
   * <p>When set, only the node whose resolved {@link #executorId()} matches this value will run the
   * purge timer.
   */
  Optional<String> purgeExecutorId();

  /**
   * Default TTL for newly reserved idempotency keys. After this duration the reservation may be
   * purged by the background maintenance task.
   */
  @WithDefault("PT5M")
  Duration ttl();

  /**
   * Additional grace added to {@link #ttl()} when reserving keys. Extends retention slightly to
   * tolerate clock skew and queued retries while keeping the advertised lifetime unchanged.
   */
  @WithDefault("PT0S")
  Duration ttlGrace();

  /**
   * Maximum time the handler waits while polling an in-progress reservation owned by another
   * executor before returning a retryable response. The poll loop runs synchronously on the calling
   * worker thread, so this value is the upper bound on how long a single duplicate request can pin
   * a worker. Set conservatively: a high value combined with concurrent duplicate traffic can
   * exhaust the worker pool. Operators are expected to deploy route-level rate limiting on
   * idempotent endpoints in addition to this cap.
   */
  @WithDefault("PT2S")
  Duration inProgressWait();

  /**
   * Lease TTL for considering an in-progress idempotency owner "active" based on heartbeatAt. If a
   * duplicate observes {@code (now - heartbeatAt > leaseTtl)}, the owner is treated as stale and
   * the server will not wait indefinitely.
   */
  @WithDefault("PT25S")
  Duration leaseTtl();

  /** Polling interval used while waiting for an in-progress duplicate. */
  @WithDefault("PT0.1S")
  Duration inProgressPollInterval();

  /**
   * Whether the background purge timer is enabled. In multi-node deployments, enabling purge on all
   * replicas may cause unnecessary contention; set {@link #purgeExecutorId()} to pin purge to a
   * single replica.
   */
  @WithDefault("false")
  boolean purgeEnabled();

  /**
   * Purge interval. Defaults to {@code P1D} since records expire on their own {@code ttl} and the
   * timer only controls how often dead rows are reclaimed; operators with very high reservation
   * churn can lower it. Examples: {@code P1D}, {@code PT1H}, {@code PT15M}.
   */
  @WithDefault("P1D")
  Duration purgeInterval();

  /** Purge records expired strictly before {@code (now - purgeGrace)}. */
  @WithDefault("PT0S")
  Duration purgeGrace();
}
