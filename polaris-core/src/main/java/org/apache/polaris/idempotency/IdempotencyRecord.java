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

  public Integer getHttpStatus() {
    return httpStatus;
  }

  public String getErrorSubtype() {
    return errorSubtype;
  }

  public String getResponseSummary() {
    return responseSummary;
  }

  public String getResponseHeaders() {
    return responseHeaders;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public Instant getUpdatedAt() {
    return updatedAt;
  }

  public Instant getFinalizedAt() {
    return finalizedAt;
  }

  public Instant getHeartbeatAt() {
    return heartbeatAt;
  }

  public String getExecutorId() {
    return executorId;
  }

  public Instant getExpiresAt() {
    return expiresAt;
  }

  public boolean isFinalized() {
    return httpStatus != null;
  }
}
