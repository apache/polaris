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
 * Idempotency configuration (handler-level design).
 *
 * <p>Idempotency is implemented inside the catalog handler, after authorization. The Iceberg REST
 * adapter reads the {@code Idempotency-Key} request header and forwards it into the wired handler
 * methods. Reservations are persisted via {@link
 * org.apache.polaris.core.persistence.IdempotencyStore}.
 *
 * <p>Endpoint scope is intentionally not configurable here: only a small set of handler methods are
 * wired today (initially {@code createTableDirect}). See the project's mailing list discussion for
 * the design rationale.
 *
 * <h2>Single-node vs multi-node deployments</h2>
 *
 * <p>In a multi-node deployment idempotency records must be shared across nodes via a durable store
 * (for example JDBC + Postgres). Each node must use a different executor id so
 * ownership/cancel/heartbeats can be attributed correctly.
 *
 * <p>Purge is a best-effort background cleanup. In multi-node deployments, enabling purge on every
 * replica can create unnecessary contention; consider running purge in only one replica or via an
 * external scheduled job.
 */
@ConfigMapping(prefix = "polaris.idempotency")
public interface IdempotencyConfiguration {

  /** Master switch for handler-level idempotency. */
  @WithDefault("false")
  boolean enabled();

  /** Request header name containing the client-provided idempotency key. */
  @WithDefault("Idempotency-Key")
  String keyHeader();

  /** Default TTL for newly reserved keys. Examples: {@code PT5M}, {@code PT300S}. */
  @WithDefault("PT5M")
  Duration ttl();

  /**
   * Additional grace added to {@link #ttl()} when reserving keys.
   *
   * <p>Extends retention slightly to tolerate clock skew and queued retries while keeping the
   * advertised lifetime unchanged.
   */
  @WithDefault("PT0S")
  Duration ttlGrace();

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
   * Maximum time the handler waits while polling an in-progress reservation owned by another
   * executor before returning a retryable response.
   */
  @WithDefault("PT30S")
  Duration inProgressWait();

  /** Polling interval used while waiting for an in-progress duplicate. */
  @WithDefault("PT0.1S")
  Duration inProgressPollInterval();

  /**
   * Lease TTL for considering an in-progress owner "active" based on {@code heartbeatAt}.
   *
   * <p>If a duplicate observes {@code now - heartbeatAt > leaseTtl}, the owner is treated as stale
   * and the server should not wait indefinitely.
   */
  @WithDefault("PT25S")
  Duration leaseTtl();

  /**
   * Enable periodic purge of expired idempotency records.
   *
   * <p>In multi-node deployments, enabling purge on all replicas may cause unnecessary contention.
   */
  @WithDefault("false")
  boolean purgeEnabled();

  /**
   * Optional executor id that is allowed to run purge.
   *
   * <p>When set, only the node whose resolved {@link #executorId()} matches this value will run the
   * purge timer.
   */
  Optional<String> purgeExecutorId();

  /** Purge interval. Examples: {@code PT1M}, {@code PT60S}. */
  @WithDefault("PT1M")
  Duration purgeInterval();

  /** Purge records expired strictly before {@code (now - purgeGrace)}. */
  @WithDefault("PT0S")
  Duration purgeGrace();
}
