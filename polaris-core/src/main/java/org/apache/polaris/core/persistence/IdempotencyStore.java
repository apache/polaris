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
package org.apache.polaris.core.persistence;

import java.time.Instant;
import java.util.Optional;
import org.apache.polaris.idempotency.IdempotencyRecord;

/**
 * Abstraction for persisting and querying idempotency records.
 *
 * <p>An {@link IdempotencyStore} is responsible for:
 *
 * <ul>
 *   <li>Reserving an idempotency key for a particular operation and resource
 *   <li>Recording completion status and response metadata
 *   <li>Allowing callers to look up existing records to detect duplicates
 *   <li>Expiring and purging old reservations
 * </ul>
 *
 * <p>Implementations must be thread-safe if used concurrently.
 */
public interface IdempotencyStore {

  /** High-level outcome of attempting to reserve an idempotency key. */
  enum ReserveResultType {
    /** The caller successfully acquired ownership of the idempotency key. */
    OWNED,
    /** A reservation already exists for the key; the caller does not own it. */
    DUPLICATE
  }

  /**
   * Result of attempting to update the heartbeat for an in-progress idempotency record.
   *
   * <p>This allows callers to distinguish between different "no update" scenarios instead of
   * overloading them into a boolean.
   */
  enum HeartbeatResult {
    /** The heartbeat was successfully updated for the in-progress record owned by this executor. */
    UPDATED,

    /**
     * The idempotency record exists but has already been finalized with an HTTP status; there is no
     * longer an in-progress reservation to heartbeat.
     */
    FINALIZED,

    /**
     * No idempotency record exists for the specified realm and key. This can happen if the record
     * was never created or has already been purged.
     */
    NOT_FOUND,

    /**
     * An in-progress idempotency record exists for the key, but it is owned by a different
     * executor. The caller should stop heartbeating as it no longer owns the reservation.
     */
    LOST_OWNERSHIP
  }

  /**
   * Result of a {@link #reserve(String, String, String, String, Instant, String, Instant)} call,
   * including the outcome and, when applicable, the existing idempotency record.
   */
  final class ReserveResult {
    private final ReserveResultType type;
    private final Optional<IdempotencyRecord> existing;

    public ReserveResult(ReserveResultType type, Optional<IdempotencyRecord> existing) {
      this.type = type;
      this.existing = existing == null ? Optional.empty() : existing;
    }

    /**
     * Returns the outcome of the reservation attempt.
     *
     * @return the {@link ReserveResultType}
     */
    public ReserveResultType getType() {
      return type;
    }

    /**
     * Returns the existing idempotency record when {@link #getType()} is {@link
     * ReserveResultType#DUPLICATE}, otherwise {@link Optional#empty()}.
     *
     * @return the existing {@link IdempotencyRecord}, if present
     */
    public Optional<IdempotencyRecord> getExisting() {
      return existing;
    }
  }

  /**
   * Attempts to reserve an idempotency key for a given operation and resource.
   *
   * <p>If no record exists yet, the implementation should create a new reservation owned by {@code
   * executorId}. If a record already exists, the implementation should return {@link
   * ReserveResultType#DUPLICATE} along with the existing record.
   *
   * @param realmId logical tenant or realm identifier
   * @param idempotencyKey application-provided idempotency key
   * @param operationType logical operation name (e.g., {@code "commit-table"})
   * @param normalizedResourceId normalized identifier of the affected resource
   * @param expiresAt timestamp after which the reservation is considered expired
   * @param executorId identifier of the caller attempting the reservation
   * @param now timestamp representing the current time
   * @return {@link ReserveResult} describing whether the caller owns the reservation or hit a
   *     duplicate
   */
  ReserveResult reserve(
      String realmId,
      String idempotencyKey,
      String operationType,
      String normalizedResourceId,
      Instant expiresAt,
      String executorId,
      Instant now);

  /**
   * Loads an existing idempotency record for the given realm and key, if present.
   *
   * @param realmId logical tenant or realm identifier
   * @param idempotencyKey application-provided idempotency key
   * @return the corresponding {@link IdempotencyRecord}, if it exists
   */
  Optional<IdempotencyRecord> load(String realmId, String idempotencyKey);

  /**
   * Updates the heartbeat for an in-progress reservation to indicate that the executor is still
   * actively processing.
   *
   * @param realmId logical tenant or realm identifier
   * @param idempotencyKey application-provided idempotency key
   * @param executorId identifier of the executor that owns the reservation
   * @param now timestamp representing the current time
   * @return {@link HeartbeatResult} describing whether the heartbeat was updated or why it was not
   */
  HeartbeatResult updateHeartbeat(
      String realmId, String idempotencyKey, String executorId, Instant now);

  /**
   * Marks an idempotency record as finalized, recording HTTP status and response metadata.
   *
   * <p>Implementations should be tolerant of idempotent re-finalization attempts and typically
   * return {@code false} when a record was already finalized.
   *
   * @param realmId logical tenant or realm identifier
   * @param idempotencyKey application-provided idempotency key
   * @param httpStatus HTTP status code returned to the client, or {@code null} if not applicable
   * @param errorSubtype optional error subtype or code, if the operation failed
   * @param responseSummary short, serialized representation of the response body
   * @param responseHeaders serialized representation of response headers
   * @param finalizedAt timestamp when the operation completed
   * @return {@code true} if the record was transitioned to a finalized state, {@code false}
   *     otherwise
   */
  boolean finalizeRecord(
      String realmId,
      String idempotencyKey,
      Integer httpStatus,
      String errorSubtype,
      String responseSummary,
      String responseHeaders,
      Instant finalizedAt);

  /**
   * Purges records in a given realm whose expiration time is strictly before the given instant.
   *
   * @param realmId logical tenant or realm identifier
   * @param before cutoff instant; records expiring before this time may be removed
   * @return number of records that were purged
   */
  int purgeExpired(String realmId, Instant before);
}
