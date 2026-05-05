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
 * Deploy-time / platform configuration for handler-level idempotency.
 *
 * <p>Reservations are persisted via {@link
 * org.apache.polaris.core.persistence.IdempotencyPersistence}, which sits on the realm's {@link
 * org.apache.polaris.core.persistence.BasePersistence}.
 *
 * <h2>What lives here vs. {@link org.apache.polaris.core.config.FeatureConfiguration}</h2>
 *
 * <p>The settings on this interface are deployment-wide constants that an operator typically sets
 * once per service installation (HTTP header name, executor identity, infrastructure timing knobs).
 * They do not vary per-realm or per-catalog and are read directly from the Quarkus configuration
 * tree.
 *
 * <p>Tenant-visible behaviour knobs (whether the feature is on, TTLs, the in-progress wait budget,
 * lease TTL, purge enable) live in {@link org.apache.polaris.core.config.FeatureConfiguration} as
 * {@code IDEMPOTENCY_*} entries so they can be overridden per-realm or per-catalog at runtime
 * through the standard configuration resolution path.
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

  /** Polling interval used while waiting for an in-progress duplicate. */
  @WithDefault("PT0.1S")
  Duration inProgressPollInterval();

  /**
   * Purge interval. Defaults to {@code PT1H} so per-pod wake-ups stay cheap; operators with very
   * high reservation churn can lower it. Examples: {@code PT1H}, {@code PT15M}.
   */
  @WithDefault("PT1H")
  Duration purgeInterval();

  /** Purge records expired strictly before {@code (now - purgeGrace)}. */
  @WithDefault("PT0S")
  Duration purgeGrace();
}
