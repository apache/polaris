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
import org.jspecify.annotations.Nullable;

/**
 * Request-scoped holder for a pending entity-property idempotency key. The REST adapter sets the
 * key before invoking the handler; {@code LocalIcebergCatalog} reads it when committing a create so
 * the key is stamped atomically with the new entity.
 */
@RequestScoped
public class IdempotencyRequestContext {

  private final IdempotencyConfiguration idempotencyConfiguration;
  private @Nullable UUID pendingKey;
  private @Nullable Instant pendingExpiry;

  @Inject
  public IdempotencyRequestContext(IdempotencyConfiguration idempotencyConfiguration) {
    this.idempotencyConfiguration = idempotencyConfiguration;
  }

  /**
   * Records {@code key} for the current request, computing expiry from {@link
   * IdempotencyConfiguration#ttl()}. No-op when {@code key} is {@code null}.
   */
  public void setPendingKey(@Nullable UUID key) {
    if (pendingKey != null || pendingExpiry != null) {
      throw new IllegalStateException("Idempotency request context already set");
    }
    if (key == null) {
      return;
    }
    this.pendingKey = key;
    this.pendingExpiry = Instant.now().plus(idempotencyConfiguration.ttl());
  }

  /**
   * Whether entity-property idempotency applies to the current request: the feature is enabled and
   * a well-formed {@code Idempotency-Key} was supplied (and thus captured by {@code
   * IdempotencyKeyFilter}). When {@code true}, {@link #pendingKey()} is non-null.
   */
  public boolean isActive() {
    return pendingKey != null && idempotencyConfiguration.enabled();
  }

  public @Nullable UUID pendingKey() {
    return pendingKey;
  }

  public @Nullable Instant pendingExpiry() {
    return pendingExpiry;
  }

  /**
   * Resets the pending state. Not needed in production, where each request gets a fresh {@link
   * RequestScoped} instance; provided so tests that drive multiple logical requests through a
   * shared instance can simulate that per-request lifecycle.
   */
  public void clearPending() {
    pendingKey = null;
    pendingExpiry = null;
  }
}
