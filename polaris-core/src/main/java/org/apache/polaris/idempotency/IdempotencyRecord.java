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
package org.apache.polaris.idempotency;

import java.time.Instant;

public final class IdempotencyRecord {
  private final String realmId;
  private final String idempotencyKey;
  private final String operationType;
  private final String normalizedResourceId;

  private final Integer httpStatus;
  private final String errorSubtype;
  private final String responseSummary;
  private final String responseHeaders;
  private final Instant finalizedAt;

  private final Instant createdAt;
  private final Instant updatedAt;
  private final Instant heartbeatAt;
  private final String executorId;
  private final Instant expiresAt;

  public IdempotencyRecord(
      String realmId,
      String idempotencyKey,
      String operationType,
      String normalizedResourceId,
      Integer httpStatus,
      String errorSubtype,
      String responseSummary,
      String responseHeaders,
      Instant createdAt,
      Instant updatedAt,
      Instant finalizedAt,
      Instant heartbeatAt,
      String executorId,
      Instant expiresAt) {
    this.realmId = realmId;
    this.idempotencyKey = idempotencyKey;
    this.operationType = operationType;
    this.normalizedResourceId = normalizedResourceId;
    this.httpStatus = httpStatus;
    this.errorSubtype = errorSubtype;
    this.responseSummary = responseSummary;
    this.responseHeaders = responseHeaders;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
    this.finalizedAt = finalizedAt;
    this.heartbeatAt = heartbeatAt;
    this.executorId = executorId;
    this.expiresAt = expiresAt;
  }

  public String getRealmId() {
    return realmId;
  }

  public String getIdempotencyKey() {
    return idempotencyKey;
  }

  public String getOperationType() {
    return operationType;
  }

  public String getNormalizedResourceId() {
    return normalizedResourceId;
  }

  /**
   * HTTP status code returned to the client for this idempotent operation.
   *
   * <p>Remains {@code null} while the record is {@code IN_PROGRESS} and is set only when the
   * operation reaches a terminal 2xx or 4xx state.
   */
  public Integer getHttpStatus() {
    return httpStatus;
  }

  /**
   * Optional error subtype or code that provides additional detail when the operation failed.
   *
   * <p>Examples include {@code already_exists}, {@code namespace_not_empty}, or {@code
   * idempotency_replay_failed}.
   */
  public String getErrorSubtype() {
    return errorSubtype;
  }

  /**
   * Minimal serialized representation of the response body used to reproduce an equivalent
   * response.
   *
   * <p>This is typically a compact JSON string that contains just enough information for the HTTP
   * layer to reconstruct the response for duplicate idempotent requests.
   */
  public String getResponseSummary() {
    return responseSummary;
  }

  /**
   * Serialized representation of a small, whitelisted set of HTTP response headers.
   *
   * <p>Stored as a JSON string so that the HTTP layer can replay key headers (such as {@code
   * Content-Type}) when serving a duplicate idempotent request.
   */
  public String getResponseHeaders() {
    return responseHeaders;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public Instant getUpdatedAt() {
    return updatedAt;
  }

  /**
   * Timestamp indicating when the record was finalized.
   *
   * <p>Set at the same time as {@link #getHttpStatus()} when the operation completes; {@code null}
   * while the record is still {@code IN_PROGRESS}.
   */
  public Instant getFinalizedAt() {
    return finalizedAt;
  }

  /**
   * Timestamp of the most recent successful heartbeat while the operation is {@code IN_PROGRESS}.
   *
   * <p>This is updated by the owning executor to signal liveness and is used by reconciliation
   * logic to detect stuck or abandoned in-progress records.
   */
  public Instant getHeartbeatAt() {
    return heartbeatAt;
  }

  /**
   * Identifier of the executor (for example pod or worker id) that currently owns the in-progress
   * reservation.
   */
  public String getExecutorId() {
    return executorId;
  }

  /** Timestamp after which the reservation is considered expired and eligible for purging. */
  public Instant getExpiresAt() {
    return expiresAt;
  }

  public boolean isFinalized() {
    return httpStatus != null;
  }
}
