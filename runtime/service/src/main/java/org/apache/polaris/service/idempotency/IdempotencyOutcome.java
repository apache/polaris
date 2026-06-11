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

import java.util.UUID;
import org.apache.polaris.core.entity.IdempotencyRecord;

/**
 * The decision produced by {@link IdempotencyHandlerSupport#preflight} and {@link
 * IdempotencyHandlerSupport#recordOutcome}. It is the single value the handler branches on; it
 * replaces the former ad-hoc "is idempotency on?" boolean.
 *
 * <ul>
 *   <li>{@link Disabled} — idempotency is off for this request (feature disabled or no key). The
 *       handler runs the plain, non-idempotent path.
 *   <li>{@link Owned} — this caller owns the key: no prior record was found (pre-flight) or the
 *       record was just inserted (record). The handler performs the operation and then records the
 *       outcome. The binding (key, operation, and the principal/resource hashes) travels inside so
 *       the record and race-resolution steps need no re-hashing.
 *   <li>{@link Duplicate} — a prior success exists for this key with a matching binding. The
 *       handler rebuilds the response from authoritative catalog state.
 * </ul>
 */
public sealed interface IdempotencyOutcome
    permits IdempotencyOutcome.Disabled, IdempotencyOutcome.Owned, IdempotencyOutcome.Duplicate {

  static IdempotencyOutcome disabled() {
    return Disabled.INSTANCE;
  }

  static IdempotencyOutcome.Owned owned(
      UUID idempotencyKey,
      IdempotentOperation operation,
      String resourceHash,
      String principalHash) {
    return new Owned(idempotencyKey, operation, resourceHash, principalHash);
  }

  static IdempotencyOutcome duplicate(IdempotencyRecord existing) {
    return new Duplicate(existing);
  }

  /** Idempotency is not in effect for this request; proceed without recording. */
  final class Disabled implements IdempotencyOutcome {
    private static final Disabled INSTANCE = new Disabled();

    private Disabled() {}
  }

  /**
   * This caller may proceed and owns the key. Carries the binding so {@link
   * IdempotencyHandlerSupport#recordOutcome} and {@link
   * IdempotencyHandlerSupport#resolveConcurrentDuplicate} can act without recomputing hashes.
   */
  record Owned(
      UUID idempotencyKey, IdempotentOperation operation, String resourceHash, String principalHash)
      implements IdempotencyOutcome {}

  /** A prior success was recorded for this key. The handler must rebuild an equivalent response. */
  record Duplicate(IdempotencyRecord existing) implements IdempotencyOutcome {}
}
