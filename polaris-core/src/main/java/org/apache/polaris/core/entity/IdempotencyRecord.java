/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.polaris.core.entity;

import java.time.Instant;

/**
 * Immutable snapshot of an idempotency reservation and its finalization status.
 *
 * <p>This is the persistence-agnostic representation used by higher layers; storage backends map
 * their concrete schemas into this type.
 *
 * @param realmId Logical tenant / realm identifier.
 * @param idempotencyKey Client-provided idempotency key.
 * @param operationType Logical operation type (e.g. {@code "create-table"}).
 * @param normalizedResourceId Request-derived, fully-qualified identifier of the affected resource
 *     (see {@link #normalizedResourceId ()}).
 * @param principalHash Hash of the caller principal identity bound to this reservation. Compared on
 *     replay to prevent cross-principal cache hits.
 * @param httpStatus HTTP status code returned to the client once finalized; {@code null} while
 *     in-progress.
 * @param errorSubtype Optional error subtype/code when the operation failed.
 * @param responseSummary Minimal serialized representation of the response body for replay. Always
 *     {@code null} for credential-bearing mutations (which re-vend on replay rather than replaying
 *     a stored response).
 * @param finalizedAt Timestamp when the operation was finalized; {@code null} while in-progress.
 * @param createdAt Timestamp when the record was created.
 * @param updatedAt Timestamp when the record was last updated.
 * @param heartbeatAt Timestamp of the most recent heartbeat while in-progress; {@code null} if
 *     never heartbeated.
 * @param executorId Identifier of the executor that owns the in-progress reservation; {@code null}
 *     if not owned.
 * @param expiresAt Timestamp after which the reservation is considered expired and eligible for
 *     purging.
 */
public record IdempotencyRecord(
    String realmId,
    String idempotencyKey,
    String operationType,
    String normalizedResourceId,
    String principalHash,
    Integer httpStatus,
    String errorSubtype,
    String responseSummary,
    Instant createdAt,
    Instant updatedAt,
    Instant finalizedAt,
    Instant heartbeatAt,
    String executorId,
    Instant expiresAt) {

  public boolean isFinalized() {
    return httpStatus != null;
  }
}
