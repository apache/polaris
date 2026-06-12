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

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.apache.polaris.core.persistence.IdempotencyStore;

/**
 * Inert {@link IdempotencyStore} produced when idempotency is disabled. Handlers short-circuit on
 * {@link IdempotencyHandlerSupport#isEnabled()} and never touch the store, so every method here
 * fails fast: reaching one means a code path tried to use the store while the feature is off.
 */
final class NoOpIdempotencyStore implements IdempotencyStore {

  static final NoOpIdempotencyStore INSTANCE = new NoOpIdempotencyStore();

  private NoOpIdempotencyStore() {}

  @Override
  public Optional<IdempotencyRecord> load(UUID idempotencyKey) {
    throw disabled();
  }

  @Override
  public RecordResult recordIfAbsent(
      UUID idempotencyKey,
      String operationType,
      String bindingHash,
      int httpStatus,
      String metadataLocation,
      Instant createdAt,
      Instant expiresAt) {
    throw disabled();
  }

  @Override
  public int purgeExpired(Instant before) {
    throw disabled();
  }

  private static IllegalStateException disabled() {
    return new IllegalStateException(
        "Idempotency is disabled; no idempotency store should be used");
  }
}
