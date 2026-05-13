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

import com.google.common.annotations.Beta;
import java.time.Instant;
import java.util.Optional;
import org.apache.polaris.core.entity.IdempotencyRecord;

/**
 * SPI for persisting and querying idempotency records.
 *
 * <p>This interface lives at the same layer as {@link BasePersistence}, {@link
 * org.apache.polaris.core.policy.PolicyMappingPersistence}, and {@link
 * org.apache.polaris.core.persistence.metrics.MetricsPersistence}: a backend that wants to support
 * handler-level idempotency implements these methods on its existing persistence class. Callers
 * obtain it independently from {@link
 * org.apache.polaris.core.persistence.MetaStoreManagerFactory#getOrCreateIdempotencyPersistence(org.apache.polaris.core.context.RealmContext)},
 * so an admin is free to back it with a different storage technology than {@link BasePersistence}.
 *
 * <p>An {@link IdempotencyPersistence} is responsible for:
 *
 * <ul>
 *   <li>Reserving an idempotency key for a particular operation, resource, and caller principal
 *   <li>Recording completion status and (optionally) a minimal response summary
 *   <li>Allowing callers to look up existing records to detect duplicates
 *   <li>Cancelling an in-progress reservation if the executing call fails before finalization
 *   <li>Expiring and purging old reservations
 * </ul>
 *
 * <p>The handler-level idempotency design always runs after authorization, so the store does not
 * need to enforce identity itself; it only needs to persist the {@code principalHash} so the
 * handler can validate it on replay and reject cross-principal cache hits.
 *
 * <p>All methods have default implementations that throw {@link UnsupportedOperationException}.
 * Backends that do not support handler-level idempotency inherit the throwing defaults; the feature
 * is opt-in (disabled by default) so deployments that do not enable it never reach those methods.
 * Backends that support idempotency override every method.
 *
 * <p>Implementations must be thread-safe if used concurrently.
 */
@Beta
public interface IdempotencyPersistence {

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
   * Result of a {@link #reserve(String, String, String, String, String, Instant, String, Instant)}
   * call, including the outcome and, when applicable, the existing idempotency record.
   *
   * @param type outcome of the reservation attempt
   * @param existing existing idempotency record, when {@link #type ()} is {@link
   *     ReserveResultType#DUPLICATE}, otherwise {@link Optional#empty()}.
   */
  record ReserveResult(ReserveResultType type, Optional<IdempotencyRecord> existing) {}

  /**
   * Attempts to reserve an idempotency key for a given operation, resource, and caller.
   *
   * <p>If no record exists yet, the implementation should create a new reservation owned by {@code
   * executorId}. If a record already exists, the implementation should return {@link
   * ReserveResultType#DUPLICATE} along with the existing record. The caller is responsible for
   * comparing the existing record's {@code principalHash} and {@code normalizedResourceId} against
   * the current request and rejecting mismatches as conflicts.
   *
   * @param realmId logical tenant or realm identifier
   * @param idempotencyKey application-provided idempotency key
   * @param operationType logical operation name (e.g., {@code "create-table"})
   * @param normalizedResourceId normalized identifier of the affected resource
   * @param principalHash hash of the caller principal identity (e.g., {@code SHA256(name + ":" +
   *     realmId)}); persisted so replay can verify the same caller
   * @param expiresAt timestamp after which the reservation is considered expired
   * @param executorId identifier of the caller attempting the reservation
   * @param now timestamp representing the current time
   * @return {@link ReserveResult} describing whether the caller owns the reservation or hit a
   *     duplicate
   */
  default ReserveResult reserve(
      String realmId,
      String idempotencyKey,
      String operationType,
      String normalizedResourceId,
      String principalHash,
      Instant expiresAt,
      String executorId,
      Instant now) {
    throw new UnsupportedOperationException(
        "This persistence backend does not support handler-level idempotency");
  }

  /**
   * Loads an existing idempotency record for the given realm and key, if present.
   *
   * @param realmId logical tenant or realm identifier
   * @param idempotencyKey application-provided idempotency key
   * @return the corresponding {@link IdempotencyRecord}, if it exists
   */
  default Optional<IdempotencyRecord> loadIdempotencyRecord(String realmId, String idempotencyKey) {
    throw new UnsupportedOperationException(
        "This persistence backend does not support handler-level idempotency");
  }

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
  default HeartbeatResult updateHeartbeat(
      String realmId, String idempotencyKey, String executorId, Instant now) {
    throw new UnsupportedOperationException(
        "This persistence backend does not support handler-level idempotency");
  }

  /**
   * Cancels an in-progress reservation owned by {@code executorId}.
   *
   * <p>This is used by handler-level idempotency to release the key when the handler hits an error
   * before reaching finalization (for example, a 401/403 from the authorizer or a downstream IAM
   * failure during credential vending). After cancellation the next caller may acquire the same
   * key.
   *
   * <p>Implementations must not cancel a reservation that has already been finalized, and must not
   * cancel a reservation that is owned by a different executor.
   *
   * @return {@code true} if the reservation was removed, {@code false} otherwise
   */
  default boolean cancelInProgressReservation(
      String realmId, String idempotencyKey, String executorId) {
    throw new UnsupportedOperationException(
        "This persistence backend does not support handler-level idempotency");
  }

  /**
   * Marks an idempotency record as finalized, recording HTTP status and an optional response
   * summary.
   *
   * <p>For credential-bearing mutations the handler always passes {@code null} for {@code
   * responseSummary} since the response is rebuilt from authoritative state on replay rather than
   * served from the store.
   *
   * <p>Implementations should be tolerant of idempotent re-finalization attempts and typically
   * return {@code false} when a record was already finalized or is owned by a different executor.
   *
   * @param realmId logical tenant or realm identifier
   * @param idempotencyKey application-provided idempotency key
   * @param executorId identifier of the executor that owns the reservation
   * @param httpStatus HTTP status code returned to the client, or {@code null} if not applicable
   * @param errorSubtype optional error subtype or code, if the operation failed
   * @param responseSummary short, serialized representation of the response body; pass {@code null}
   *     for credential-bearing mutations
   * @param finalizedAt timestamp when the operation completed
   * @return {@code true} if the record was transitioned to a finalized state, {@code false}
   *     otherwise
   */
  default boolean finalizeRecord(
      String realmId,
      String idempotencyKey,
      String executorId,
      Integer httpStatus,
      String errorSubtype,
      String responseSummary,
      Instant finalizedAt) {
    throw new UnsupportedOperationException(
        "This persistence backend does not support handler-level idempotency");
  }

  /**
   * Purges records in a given realm whose expiration time is strictly before the given instant.
   *
   * @param realmId logical tenant or realm identifier
   * @param before cutoff instant; records expiring before this time may be removed
   * @return number of records that were purged
   */
  default int purgeExpired(String realmId, Instant before) {
    throw new UnsupportedOperationException(
        "This persistence backend does not support handler-level idempotency");
  }
}
