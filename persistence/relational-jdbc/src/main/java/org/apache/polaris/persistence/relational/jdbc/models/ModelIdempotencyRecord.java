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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.jspecify.annotations.Nullable;

/**
 * JDBC model for {@link IdempotencyRecord} mirroring the {@code idempotency_records} table.
 *
 * <p>The table holds finalized records only (no in-progress / lease state). A row is inserted at
 * most once per {@code (realm_id, idempotency_key)} pair and is never updated afterwards.
 *
 * <p>Note: {@code realm_id} is treated as an implicit column across relational-jdbc: callers can
 * filter on it in WHERE clauses even if it is not included in the projection list.
 */
public interface ModelIdempotencyRecord extends Converter<IdempotencyRecord> {

  String TABLE_NAME = "idempotency_records";

  String REALM_ID = "realm_id";
  String IDEMPOTENCY_KEY = "idempotency_key";
  String OPERATION_TYPE = "operation_type";
  String RESOURCE_HASH = "resource_hash";
  String PRINCIPAL_HASH = "principal_hash";
  String HTTP_STATUS = "http_status";
  String METADATA_LOCATION = "metadata_location";
  String CREATED_AT = "created_at";
  String EXPIRES_AT = "expires_at";

  List<String> ALL_COLUMNS =
      List.of(
          IDEMPOTENCY_KEY,
          OPERATION_TYPE,
          RESOURCE_HASH,
          PRINCIPAL_HASH,
          HTTP_STATUS,
          METADATA_LOCATION,
          CREATED_AT,
          EXPIRES_AT);

  String getRealmId();

  String getIdempotencyKey();

  String getOperationType();

  String getResourceHash();

  String getPrincipalHash();

  int getHttpStatus();

  @Nullable String getMetadataLocation();

  Instant getCreatedAt();

  Instant getExpiresAt();

  @Override
  default IdempotencyRecord fromResultSet(ResultSet rs) throws SQLException {
    return fromRow(rs.getString(REALM_ID), rs);
  }

  /**
   * Convert the current ResultSet row into an {@link IdempotencyRecord}, using {@code realmId} from
   * call context (so callers can project only {@link #ALL_COLUMNS}).
   */
  static IdempotencyRecord fromRow(String realmId, ResultSet rs) throws SQLException {
    UUID idempotencyKey = UUID.fromString(rs.getString(IDEMPOTENCY_KEY));
    String operationType = rs.getString(OPERATION_TYPE);
    String resourceHash = rs.getString(RESOURCE_HASH);
    String principalHash = rs.getString(PRINCIPAL_HASH);
    int httpStatus = rs.getInt(HTTP_STATUS);
    String metadataLocation = rs.getString(METADATA_LOCATION);
    Instant createdAt = rs.getTimestamp(CREATED_AT).toInstant();
    Instant expiresAt = rs.getTimestamp(EXPIRES_AT).toInstant();
    return new IdempotencyRecord(
        realmId,
        idempotencyKey,
        operationType,
        resourceHash,
        principalHash,
        httpStatus,
        metadataLocation,
        createdAt,
        expiresAt);
  }

  @Override
  default Map<String, Object> toMap(DatabaseType databaseType) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put(IDEMPOTENCY_KEY, getIdempotencyKey());
    map.put(OPERATION_TYPE, getOperationType());
    map.put(RESOURCE_HASH, getResourceHash());
    map.put(PRINCIPAL_HASH, getPrincipalHash());
    map.put(HTTP_STATUS, getHttpStatus());
    map.put(METADATA_LOCATION, getMetadataLocation());
    map.put(CREATED_AT, Timestamp.from(getCreatedAt()));
    map.put(EXPIRES_AT, Timestamp.from(getExpiresAt()));
    return map;
  }
}
