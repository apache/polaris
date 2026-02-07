/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.persistence.relational.jdbc.models;

import jakarta.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;

/**
 * JDBC model for {@link IdempotencyRecord} mirroring the {@code idempotency_records} table.
 *
 * <p>This follows the same pattern as {@link ModelEvent}, separating the storage representation
 * from the core domain model while still providing {@link Converter} helpers.
 *
 * <p>Note: {@code realm_id} is treated as an implicit column across relational-jdbc: callers can
 * filter on it in WHERE clauses even if it is not included in the projection list.
 */
public interface ModelIdempotencyRecord extends Converter<IdempotencyRecord> {

  String TABLE_NAME = "idempotency_records";

  // Logical tenant / realm identifier.
  String REALM_ID = "realm_id";
  // Client-provided idempotency key.
  String IDEMPOTENCY_KEY = "idempotency_key";
  // Logical operation type (e.g. commit-table).
  String OPERATION_TYPE = "operation_type";
  // Normalized identifier of the affected resource.
  String RESOURCE_ID = "resource_id";

  // Final HTTP status code once the operation is completed (null while in-progress).
  String HTTP_STATUS = "http_status";
  // Optional error subtype for failures.
  String ERROR_SUBTYPE = "error_subtype";
  // Short serialized representation of the response body.
  String RESPONSE_SUMMARY = "response_summary";
  // Serialized representation of response headers.
  String RESPONSE_HEADERS = "response_headers";
  // Timestamp when the operation was finalized (null while in-progress).
  String FINALIZED_AT = "finalized_at";

  // Timestamp when the record was created.
  String CREATED_AT = "created_at";
  // Timestamp when the record was last updated.
  String UPDATED_AT = "updated_at";
  // Timestamp for the last heartbeat update (null if no heartbeat recorded).
  String HEARTBEAT_AT = "heartbeat_at";
  // Identifier of the executor that owns the in-progress record (null if not owned).
  String EXECUTOR_ID = "executor_id";
  // Expiration timestamp after which the record can be considered stale/purgeable.
  String EXPIRES_AT = "expires_at";

  List<String> ALL_COLUMNS =
      List.of(
          IDEMPOTENCY_KEY,
          OPERATION_TYPE,
          RESOURCE_ID,
          HTTP_STATUS,
          ERROR_SUBTYPE,
          RESPONSE_SUMMARY,
          RESPONSE_HEADERS,
          FINALIZED_AT,
          CREATED_AT,
          UPDATED_AT,
          HEARTBEAT_AT,
          EXECUTOR_ID,
          EXPIRES_AT);

  String getRealmId();

  String getIdempotencyKey();

  String getOperationType();

  String getResourceId();

  @Nullable
  Integer getHttpStatus();

  @Nullable
  String getErrorSubtype();

  @Nullable
  String getResponseSummary();

  @Nullable
  String getResponseHeaders();

  @Nullable
  Instant getFinalizedAt();

  Instant getCreatedAt();

  Instant getUpdatedAt();

  @Nullable
  Instant getHeartbeatAt();

  @Nullable
  String getExecutorId();

  Instant getExpiresAt();

  @Override
  default IdempotencyRecord fromResultSet(ResultSet rs) throws SQLException {
    return fromRow(rs);
  }

  /** Convert the current ResultSet row into an {@link IdempotencyRecord}. */
  static IdempotencyRecord fromRow(ResultSet rs) throws SQLException {
    // Requires realm_id to be projected in the ResultSet.
    return fromRow(rs.getString(REALM_ID), rs);
  }

  /**
   * Convert the current ResultSet row into an {@link IdempotencyRecord}, using {@code realmId} from
   * call context (so callers can project only {@link #ALL_COLUMNS}).
   */
  static IdempotencyRecord fromRow(String realmId, ResultSet rs) throws SQLException {
    String idempotencyKey = rs.getString(IDEMPOTENCY_KEY);
    String operationType = rs.getString(OPERATION_TYPE);
    String resourceId = rs.getString(RESOURCE_ID);

    Integer httpStatus = (Integer) rs.getObject(HTTP_STATUS);
    String errorSubtype = rs.getString(ERROR_SUBTYPE);
    String responseSummary = rs.getString(RESPONSE_SUMMARY);
    String responseHeaders = rs.getString(RESPONSE_HEADERS);

    Instant createdAt = rs.getTimestamp(CREATED_AT).toInstant();
    Instant updatedAt = rs.getTimestamp(UPDATED_AT).toInstant();

    Timestamp finalizedTs = rs.getTimestamp(FINALIZED_AT);
    Instant finalizedAt = finalizedTs == null ? null : finalizedTs.toInstant();

    Timestamp heartbeatTs = rs.getTimestamp(HEARTBEAT_AT);
    Instant heartbeatAt = heartbeatTs == null ? null : heartbeatTs.toInstant();

    String executorId = rs.getString(EXECUTOR_ID);
    Instant expiresAt = rs.getTimestamp(EXPIRES_AT).toInstant();

    return new IdempotencyRecord(
        realmId,
        idempotencyKey,
        operationType,
        resourceId,
        httpStatus,
        errorSubtype,
        responseSummary,
        responseHeaders,
        createdAt,
        updatedAt,
        finalizedAt,
        heartbeatAt,
        executorId,
        expiresAt);
  }

  @Override
  default Map<String, Object> toMap(DatabaseType databaseType) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put(IDEMPOTENCY_KEY, getIdempotencyKey());
    map.put(OPERATION_TYPE, getOperationType());
    map.put(RESOURCE_ID, getResourceId());
    map.put(HTTP_STATUS, getHttpStatus());
    map.put(ERROR_SUBTYPE, getErrorSubtype());
    map.put(RESPONSE_SUMMARY, getResponseSummary());
    map.put(RESPONSE_HEADERS, getResponseHeaders());
    map.put(FINALIZED_AT, getFinalizedAt() == null ? null : Timestamp.from(getFinalizedAt()));
    map.put(CREATED_AT, Timestamp.from(getCreatedAt()));
    map.put(UPDATED_AT, Timestamp.from(getUpdatedAt()));
    map.put(HEARTBEAT_AT, getHeartbeatAt() == null ? null : Timestamp.from(getHeartbeatAt()));
    map.put(EXECUTOR_ID, getExecutorId());
    map.put(EXPIRES_AT, Timestamp.from(getExpiresAt()));
    return map;
  }
}
