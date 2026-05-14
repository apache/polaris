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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
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

  String REALM_ID = "realm_id";
  String IDEMPOTENCY_KEY = "idempotency_key";
  String OPERATION_TYPE = "operation_type";
  String RESOURCE_ID = "resource_id";

  // Hash of caller principal identity bound to the reservation.
  // Compared on replay to prevent cross-principal cache hits.
  String PRINCIPAL_HASH = "principal_hash";

  String HTTP_STATUS = "http_status";
  String ERROR_SUBTYPE = "error_subtype";

  String RESPONSE_SUMMARY = "response_summary";
  String FINALIZED_AT = "finalized_at";

  String CREATED_AT = "created_at";
  String UPDATED_AT = "updated_at";
  String HEARTBEAT_AT = "heartbeat_at";
  String EXECUTOR_ID = "executor_id";
  String EXPIRES_AT = "expires_at";

  List<String> ALL_COLUMNS =
      List.of(
          IDEMPOTENCY_KEY,
          OPERATION_TYPE,
          RESOURCE_ID,
          PRINCIPAL_HASH,
          HTTP_STATUS,
          ERROR_SUBTYPE,
          RESPONSE_SUMMARY,
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

  String getPrincipalHash();

  @Nullable
  Integer getHttpStatus();

  @Nullable
  String getErrorSubtype();

  @Nullable
  String getResponseSummary();

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
    String principalHash = rs.getString(PRINCIPAL_HASH);

    Integer httpStatus = (Integer) rs.getObject(HTTP_STATUS);
    String errorSubtype = rs.getString(ERROR_SUBTYPE);
    String responseSummary = rs.getString(RESPONSE_SUMMARY);

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
        principalHash,
        httpStatus,
        errorSubtype,
        responseSummary,
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
    map.put(PRINCIPAL_HASH, getPrincipalHash());
    map.put(HTTP_STATUS, getHttpStatus());
    map.put(ERROR_SUBTYPE, getErrorSubtype());
    map.put(RESPONSE_SUMMARY, getResponseSummary());
    map.put(FINALIZED_AT, getFinalizedAt() == null ? null : Timestamp.from(getFinalizedAt()));
    map.put(CREATED_AT, Timestamp.from(getCreatedAt()));
    map.put(UPDATED_AT, Timestamp.from(getUpdatedAt()));
    map.put(HEARTBEAT_AT, getHeartbeatAt() == null ? null : Timestamp.from(getHeartbeatAt()));
    map.put(EXECUTOR_ID, getExecutorId());
    map.put(EXPIRES_AT, Timestamp.from(getExpiresAt()));
    return map;
  }
}
