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
import java.util.UUID;
import org.apache.polaris.core.entity.IdempotencyRecord;

/**
 * SPI for persisting and looking up handler-level idempotency records.
 *
 * <p>Polaris uses an "optimistic commit" model: a record is inserted only <em>after</em> the
 * originating operation has reached a terminal HTTP status. The SPI exposes just two state
 * transitions:
 *
 * <ul>
 *   <li>{@link #load(UUID)} to detect a duplicate request before doing any work;
 *   <li>{@link #recordIfAbsent(UUID, String, String, int, String, Instant, Instant)} to atomically
 *       insert the record after the operation has finalized, returning {@link RecordResult#OWNED}
 *       when the caller wins the race and {@link RecordResult#DUPLICATE} (with the existing record)
 *       when another caller raced ahead.
 * </ul>
 *
 * <p>There is no in-progress / lease / heartbeat state in this design, and no response body is
 * stored — duplicate requests rebuild an equivalent response from authoritative catalog state.
 *
 * <p>The handler-level idempotency design always runs after authorization, so the store does not
 * need to enforce identity itself; it only needs to persist {@code bindingHash} (which folds in the
 * caller principal) so the handler can validate it on replay and reject reuse of the same key for a
 * different caller, operation, or resource.
 *
 * <p>A store instance is bound to a single realm at construction, so the realm is not part of any
 * method signature. Implementations must be thread-safe.
 */
@Beta
public interface IdempotencyStore {

  /** Outcome of an attempted record insertion. */
  enum RecordResultType {
    /** The caller successfully inserted a new idempotency record. */
    OWNED,
    /** A record already exists for the same key in this realm; the caller did not insert. */
    DUPLICATE
  }

  /**
   * Result of {@link #recordIfAbsent(UUID, String, String, int, String, Instant, Instant)},
   * including the outcome and, when {@link RecordResultType#DUPLICATE}, the existing record so the
   * caller can compare bindings without an extra round-trip.
   *
   * <p>Invariant: when {@link #type()} is {@link RecordResultType#DUPLICATE}, {@link #existing()}
   * is always present. Implementations that cannot reload the conflicting record must raise an
   * exception rather than return a {@code DUPLICATE} with an empty {@code existing}.
   */
  record RecordResult(RecordResultType type, Optional<IdempotencyRecord> existing) {}

  /**
   * Loads an existing record for the given key in this store's realm, if present.
   *
   * @param idempotencyKey application-provided idempotency key (a UUIDv7)
   */
  Optional<IdempotencyRecord> load(UUID idempotencyKey);

  /**
   * Atomically inserts an idempotency record if no record exists yet for {@code idempotencyKey} in
   * this store's realm; otherwise returns the existing record.
   *
   * <p>This is the only "write" path in the SPI. It is invoked after the originating operation has
   * succeeded, so {@code httpStatus} is always set.
   *
   * @param idempotencyKey application-provided idempotency key (a UUIDv7)
   * @param operationType logical operation name (e.g. {@code "create-table"}), persisted as a
   *     human-readable label only
   * @param bindingHash opaque hash over the full binding (caller principal, operation, and the
   *     request-derived resource identity); not a human-readable identifier, and not the request
   *     payload; persisted so replay can detect reuse of the same key for a different
   *     caller/operation/resource
   * @param httpStatus HTTP status code returned to the client
   * @param metadataLocation resource state pointer captured at record time (for tables, the
   *     metadata-file location); used on replay to detect the resource advancing beyond the
   *     originally-created state; may be {@code null}
   * @param createdAt timestamp representing when the record was created
   * @param expiresAt timestamp after which the record is eligible for purging
   */
  RecordResult recordIfAbsent(
      UUID idempotencyKey,
      String operationType,
      String bindingHash,
      int httpStatus,
      String metadataLocation,
      Instant createdAt,
      Instant expiresAt);

  /**
   * Purges records in this store's realm whose expiration time is strictly before the given
   * instant.
   *
   * @param before cutoff instant; records expiring before this time may be removed
   * @return number of records that were purged
   */
  int purgeExpired(Instant before);
}
