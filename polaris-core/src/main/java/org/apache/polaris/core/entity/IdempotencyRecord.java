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
import java.util.Map;

/**
 * Immutable snapshot of an idempotency reservation and its finalization status.
 *
 * <p>This is the persistence-agnostic representation used by higher layers; storage backends map
 * their concrete schemas into this type.
 *
 * @param realmId Logical tenant / realm identifier.
 * @param idempotencyKey Client-provided idempotency key.
 * @param operationType Logical operation type (e.g. {@code "commit-table"}).
 * @param normalizedResourceId Request-derived, fully-qualified identifier of the affected resource
 *     (see {@link #normalizedResourceId ()}).
 * @param httpStatus HTTP status code returned to the client once finalized; {@code null} while
 *     in-progress.
 * @param errorSubtype Optional error subtype/code when the operation failed.
 * @param responseSummary Minimal serialized representation of the response body for replay.
 * @param responseHeaders Allowlisted response headers captured for replay.
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
    Integer httpStatus,
    String errorSubtype,
    String responseSummary,
    Map<String, String> responseHeaders,
    Instant createdAt,
    Instant updatedAt,
    Instant finalizedAt,
    Instant heartbeatAt,
    String executorId,
    Instant expiresAt) {

  /**
   * Normalized identifier of the resource affected by the operation.
   *
   * <p>This should be derived from the request (for example, a canonicalized and fully-qualified
   * identifier like {@code "catalogs/<catalogId>/tables/ns.tbl"}), not from a generated internal
   * entity id.
   *
   * <p>The identifier must be stable even on failure (before any entities are created) and must be
   * scoped to avoid false conflicts (for example, include the catalog/warehouse identifier when
   * applicable).
   */
  @Override
  public String normalizedResourceId() {
    return normalizedResourceId;
  }

  /**
   * HTTP status code returned to the client for this idempotent operation.
   *
   * <p>Remains {@code null} while the record is {@code IN_PROGRESS} and is set only when the
   * operation reaches a terminal 2xx or 4xx state.
   */
  @Override
  public Integer httpStatus() {
    return httpStatus;
  }

  /**
   * Optional error subtype or code that provides additional detail when the operation failed.
   *
   * <p>Examples include {@code already_exists}, {@code namespace_not_empty}, or {@code
   * idempotency_replay_failed}.
   */
  @Override
  public String errorSubtype() {
    return errorSubtype;
  }

  /**
   * Minimal serialized representation of the response body used to reproduce an equivalent
   * response.
   *
   * <p>This is typically a compact JSON string that contains just enough information for the HTTP
   * layer to reconstruct the response for duplicate idempotent requests.
   */
  @Override
  public String responseSummary() {
    return responseSummary;
  }

  /**
   * Allowlisted snapshot of HTTP response headers to replay for duplicates.
   *
   * <p>How this map is persisted is store-specific.
   */
  @Override
  public Map<String, String> responseHeaders() {
    return responseHeaders;
  }

  /**
   * Timestamp indicating when the record was finalized.
   *
   * <p>Set at the same time as {@link #httpStatus ()} when the operation completes; {@code null}
   * while the record is still {@code IN_PROGRESS}.
   */
  @Override
  public Instant finalizedAt() {
    return finalizedAt;
  }

  /**
   * Timestamp of the most recent successful heartbeat while the operation is {@code IN_PROGRESS}.
   *
   * <p>This is updated by the owning executor to signal liveness and is used by reconciliation
   * logic to detect stuck or abandoned in-progress records.
   */
  @Override
  public Instant heartbeatAt() {
    return heartbeatAt;
  }

  /**
   * Identifier of the executor (for example pod or worker id) that currently owns the in-progress
   * reservation.
   */
  @Override
  public String executorId() {
    return executorId;
  }

  /** Timestamp after which the reservation is considered expired and eligible for purging. */
  @Override
  public Instant expiresAt() {
    return expiresAt;
  }

  public boolean isFinalized() {
    return httpStatus != null;
  }
}
