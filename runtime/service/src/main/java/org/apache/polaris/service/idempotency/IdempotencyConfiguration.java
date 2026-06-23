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

/**
 * Deploy-time configuration for handler-level idempotency.
 *
 * <p>Polaris uses the single-transaction ("optimistic commit") model: an idempotency record is
 * inserted only after the originating operation has reached a terminal HTTP status, and duplicate
 * requests rebuild an equivalent response from authoritative catalog state. There is no in-progress
 * reservation, no executor lease, and no stored response body — so the configuration surface is
 * intentionally minimal.
 *
 * <p>All settings here are deployment-wide constants read from the Quarkus configuration tree. They
 * do not vary per-realm or per-catalog. Per-realm or per-catalog overrides can be introduced in a
 * follow-up if/when there is a concrete operator request for it.
 */
@ConfigMapping(prefix = "polaris.idempotency")
public interface IdempotencyConfiguration {

  /**
   * Whether handler-level idempotency is enabled. When {@code false} the handlers ignore the {@code
   * Idempotency-Key} header entirely and never read or write the idempotency store.
   */
  @WithDefault("false")
  boolean enabled();

  /**
   * The idempotency store backend to use when {@link #enabled()} is {@code true}. Must match an
   * {@link io.smallrye.common.annotation.Identifier} on a registered {@link
   * org.apache.polaris.core.persistence.IdempotencyStore} producer (e.g. {@code in-memory}, {@code
   * relational-jdbc}).
   */
  @WithDefault("in-memory")
  String type();

  /**
   * TTL for newly recorded idempotency keys. After this duration the record is expired and eligible
   * for reclamation. Reclamation of expired rows is handled separately (outside the request path).
   */
  @WithDefault("PT5M")
  Duration ttl();

  /**
   * Maximum number of times a create-table caller that lost the catalog race polls for the race
   * winner's idempotency record before giving up and surfacing the original conflict. The winner
   * records its outcome only after committing, so a brief, bounded poll lets the loser replay
   * instead of returning a 409.
   */
  @WithDefault("5")
  int concurrentReplayMaxAttempts();

  /**
   * Initial backoff between race-winner lookups; it doubles after each attempt. With the defaults
   * ({@code 5} attempts, {@code PT0.005S}) the total budget is ~155ms (5+10+20+40+80).
   */
  @WithDefault("PT0.005S")
  Duration concurrentReplayInitialBackoff();
}
