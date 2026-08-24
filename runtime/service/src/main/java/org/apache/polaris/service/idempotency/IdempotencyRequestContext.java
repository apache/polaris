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

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

/**
 * Request-scoped holder for a pending entity-property idempotency key. The REST adapter sets the
 * key before invoking the handler; {@code LocalIcebergCatalog} reads it when committing a create so
 * the key is stamped atomically with the new entity.
 *
 * <p>Although request-scoped, the same instance can be touched from more than one thread over a
 * request's lifetime (e.g. the {@code IdempotencyKeyFilter} vs. the REST service code), so the
 * pending key is held in an {@link AtomicReference} — mirroring {@code RealmContextHolder}.
 */
@RequestScoped
public class IdempotencyRequestContext {

  /**
   * Shared no-op context for code paths that build a catalog without request-scoped idempotency
   * (e.g. non-CDI construction and tests). {@link #isActive()} is always {@code false}, so callers
   * can hold a non-null context instead of null-checking.
   */
  public static final IdempotencyRequestContext DISABLED = new IdempotencyRequestContext();

  private final @Nullable IdempotencyConfiguration idempotencyConfiguration;

  // Key and expiry are always set/cleared together, so a single reference keeps reads consistent.
  private final AtomicReference<PendingKey> pending = new AtomicReference<>();

  @Inject
  public IdempotencyRequestContext(IdempotencyConfiguration idempotencyConfiguration) {
    this.idempotencyConfiguration = idempotencyConfiguration;
  }

  private IdempotencyRequestContext() {
    this.idempotencyConfiguration = null;
  }

  /**
   * Records {@code key} for the current request, computing expiry from {@link
   * IdempotencyConfiguration#ttl()}. No-op when {@code key} is {@code null} or on the {@link
   * #DISABLED} context.
   */
  public void setPendingKey(@Nullable UUID key) {
    if (key == null || idempotencyConfiguration == null) {
      return;
    }
    PendingKey value = new PendingKey(key, Instant.now().plus(idempotencyConfiguration.ttl()));
    if (!pending.compareAndSet(null, value)) {
      throw new IllegalStateException("Idempotency request context already set");
    }
  }

  /**
   * Whether entity-property idempotency applies to the current request: the feature is enabled and
   * a well-formed {@code Idempotency-Key} was supplied (and thus captured by {@code
   * IdempotencyKeyFilter}). When {@code true}, {@link #pendingKey()} is non-null.
   */
  public boolean isActive() {
    return pending.get() != null
        && idempotencyConfiguration != null
        && idempotencyConfiguration.enabled();
  }

  public @Nullable UUID pendingKey() {
    PendingKey current = pending.get();
    return current == null ? null : current.key();
  }

  public @Nullable Instant pendingExpiry() {
    PendingKey current = pending.get();
    return current == null ? null : current.expiry();
  }

  /**
   * Resets the pending state. Not needed in production, where each request gets a fresh {@link
   * RequestScoped} instance; provided so tests that drive multiple logical requests through a
   * shared instance can simulate that per-request lifecycle.
   */
  public void clearPending() {
    pending.set(null);
  }

  private record PendingKey(UUID key, Instant expiry) {}
}
