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
package org.apache.polaris.persistence.relational.jdbc.idempotency;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.apache.polaris.core.persistence.IdempotencyPersistenceException;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.apache.polaris.persistence.relational.jdbc.models.Converter;
import org.apache.polaris.persistence.relational.jdbc.models.ModelIdempotencyRecord;

public class RelationalJdbcIdempotencyStore implements IdempotencyStore {

  private final DatasourceOperations datasourceOperations;

  private static final ObjectMapper RESPONSE_HEADERS_MAPPER = new ObjectMapper();

  public RelationalJdbcIdempotencyStore(
      @Nonnull DataSource dataSource, @Nonnull RelationalJdbcConfiguration cfg)
      throws SQLException {
    this.datasourceOperations = new DatasourceOperations(dataSource, cfg);
  }

  @Override
  public ReserveResult reserve(
      String realmId,
      String idempotencyKey,
      String operationType,
      String normalizedResourceId,
      Instant expiresAt,
      String executorId,
      Instant now) {
    try {
      // Build insert values directly to avoid requiring an Immutables-generated model type.
      Map<String, Object> insertMap = new LinkedHashMap<>();
      insertMap.put(ModelIdempotencyRecord.IDEMPOTENCY_KEY, idempotencyKey);
      insertMap.put(ModelIdempotencyRecord.OPERATION_TYPE, operationType);
      insertMap.put(ModelIdempotencyRecord.RESOURCE_ID, normalizedResourceId);
      insertMap.put(ModelIdempotencyRecord.HTTP_STATUS, null);
      insertMap.put(ModelIdempotencyRecord.ERROR_SUBTYPE, null);
      insertMap.put(ModelIdempotencyRecord.RESPONSE_SUMMARY, null);
      insertMap.put(ModelIdempotencyRecord.RESPONSE_HEADERS, null);
      insertMap.put(ModelIdempotencyRecord.FINALIZED_AT, null);
      insertMap.put(ModelIdempotencyRecord.CREATED_AT, Timestamp.from(now));
      insertMap.put(ModelIdempotencyRecord.UPDATED_AT, Timestamp.from(now));
      insertMap.put(ModelIdempotencyRecord.HEARTBEAT_AT, Timestamp.from(now));
      insertMap.put(ModelIdempotencyRecord.EXECUTOR_ID, executorId);
      insertMap.put(ModelIdempotencyRecord.EXPIRES_AT, Timestamp.from(expiresAt));

      // Stream#toList returns an unmodifiable list and rejects nulls; these columns are nullable.
      List<Object> values = new ArrayList<>(insertMap.values());
      QueryGenerator.PreparedQuery insert =
          QueryGenerator.generateInsertQuery(
              ModelIdempotencyRecord.ALL_COLUMNS,
              ModelIdempotencyRecord.TABLE_NAME,
              values,
              realmId);
      datasourceOperations.executeUpdate(insert);
      return new ReserveResult(ReserveResultType.OWNED, Optional.empty());
    } catch (SQLException e) {
      if (datasourceOperations.isConstraintViolation(e)) {
        return new ReserveResult(ReserveResultType.DUPLICATE, load(realmId, idempotencyKey));
      }
      throw new IdempotencyPersistenceException("Failed to reserve idempotency key", e);
    }
  }

  @Override
  public Optional<IdempotencyRecord> load(String realmId, String idempotencyKey) {
    try {
      QueryGenerator.PreparedQuery query =
          QueryGenerator.generateSelectQuery(
              ModelIdempotencyRecord.ALL_COLUMNS,
              ModelIdempotencyRecord.TABLE_NAME,
              Map.of(
                  ModelIdempotencyRecord.REALM_ID,
                  realmId,
                  ModelIdempotencyRecord.IDEMPOTENCY_KEY,
                  idempotencyKey));
      List<IdempotencyRecord> results =
          datasourceOperations.executeSelect(
              query,
              new Converter<>() {
                @Override
                public IdempotencyRecord fromResultSet(ResultSet rs) throws SQLException {
                  return ModelIdempotencyRecord.fromRow(realmId, rs);
                }

                @Override
                public Map<String, Object> toMap(
                    org.apache.polaris.persistence.relational.jdbc.DatabaseType databaseType) {
                  throw new UnsupportedOperationException("Not used for SELECT conversion");
                }
              });
      if (results.isEmpty()) {
        return Optional.empty();
      }
      if (results.size() > 1) {
        throw new IllegalStateException(
            "More than one idempotency record found for realm/key: "
                + realmId
                + "/"
                + idempotencyKey);
      }
      return Optional.of(results.getFirst());
    } catch (SQLException e) {
      throw new IdempotencyPersistenceException("Failed to load idempotency record", e);
    }
  }

  @Override
  public HeartbeatResult updateHeartbeat(
      String realmId, String idempotencyKey, String executorId, Instant now) {
    Optional<IdempotencyRecord> existing = load(realmId, idempotencyKey);
    if (existing.isEmpty()) {
      return HeartbeatResult.NOT_FOUND;
    }

    IdempotencyRecord record = existing.get();
    if (record.httpStatus() != null) {
      return HeartbeatResult.FINALIZED;
    }
    if (record.executorId() == null || !record.executorId().equals(executorId)) {
      return HeartbeatResult.LOST_OWNERSHIP;
    }

    QueryGenerator.PreparedQuery update =
        QueryGenerator.generateUpdateQuery(
            ModelIdempotencyRecord.ALL_COLUMNS,
            ModelIdempotencyRecord.TABLE_NAME,
            Map.of(
                ModelIdempotencyRecord.HEARTBEAT_AT,
                Timestamp.from(now),
                ModelIdempotencyRecord.UPDATED_AT,
                Timestamp.from(now)),
            Map.of(
                ModelIdempotencyRecord.REALM_ID,
                realmId,
                ModelIdempotencyRecord.IDEMPOTENCY_KEY,
                idempotencyKey,
                ModelIdempotencyRecord.EXECUTOR_ID,
                executorId),
            Map.of(),
            Map.of(),
            Set.of(ModelIdempotencyRecord.HTTP_STATUS),
            Set.of());

    try {
      int updated = datasourceOperations.executeUpdate(update);
      if (updated > 0) {
        return HeartbeatResult.UPDATED;
      }
    } catch (SQLException e) {
      throw new IdempotencyPersistenceException("Failed to update idempotency heartbeat", e);
    }

    // Raced with finalize/ownership loss; re-check to return a meaningful result.
    Optional<IdempotencyRecord> after = load(realmId, idempotencyKey);
    if (after.isEmpty()) {
      return HeartbeatResult.NOT_FOUND;
    }
    if (after.get().httpStatus() != null) {
      return HeartbeatResult.FINALIZED;
    }
    return HeartbeatResult.LOST_OWNERSHIP;
  }

  @Override
  public boolean cancelInProgressReservation(
      String realmId, String idempotencyKey, String executorId) {
    try {
      QueryGenerator.PreparedQuery delete =
          QueryGenerator.generateDeleteQuery(
              ModelIdempotencyRecord.ALL_COLUMNS,
              ModelIdempotencyRecord.TABLE_NAME,
              Map.of(
                  ModelIdempotencyRecord.REALM_ID,
                  realmId,
                  ModelIdempotencyRecord.IDEMPOTENCY_KEY,
                  idempotencyKey,
                  ModelIdempotencyRecord.EXECUTOR_ID,
                  executorId),
              Map.of(),
              Map.of(),
              Set.of(ModelIdempotencyRecord.HTTP_STATUS),
              Set.of());
      return datasourceOperations.executeUpdate(delete) > 0;
    } catch (SQLException e) {
      throw new IdempotencyPersistenceException(
          "Failed to cancel in-progress idempotency reservation", e);
    }
  }

  @Override
  public boolean finalizeRecord(
      String realmId,
      String idempotencyKey,
      String executorId,
      Integer httpStatus,
      String errorSubtype,
      String responseSummary,
      Map<String, String> responseHeaders,
      Instant finalizedAt) {
    final String responseHeadersJson;
    if (responseHeaders == null || responseHeaders.isEmpty()) {
      responseHeadersJson = null;
    } else {
      try {
        responseHeadersJson = RESPONSE_HEADERS_MAPPER.writeValueAsString(responseHeaders);
      } catch (Exception e) {
        throw new IdempotencyPersistenceException(
            "Failed to serialize idempotency response headers", e);
      }
    }

    // Use ordered/set maps so we can include nullable values (Map.of disallows nulls).
    Map<String, Object> setClause = new LinkedHashMap<>();
    setClause.put(ModelIdempotencyRecord.HTTP_STATUS, httpStatus);
    setClause.put(ModelIdempotencyRecord.ERROR_SUBTYPE, errorSubtype);
    setClause.put(ModelIdempotencyRecord.RESPONSE_SUMMARY, responseSummary);
    setClause.put(ModelIdempotencyRecord.RESPONSE_HEADERS, responseHeadersJson);
    setClause.put(ModelIdempotencyRecord.FINALIZED_AT, Timestamp.from(finalizedAt));
    setClause.put(ModelIdempotencyRecord.UPDATED_AT, Timestamp.from(finalizedAt));

    Map<String, Object> whereEquals = new HashMap<>();
    whereEquals.put(ModelIdempotencyRecord.REALM_ID, realmId);
    whereEquals.put(ModelIdempotencyRecord.IDEMPOTENCY_KEY, idempotencyKey);
    whereEquals.put(ModelIdempotencyRecord.EXECUTOR_ID, executorId);

    QueryGenerator.PreparedQuery update =
        QueryGenerator.generateUpdateQuery(
            ModelIdempotencyRecord.ALL_COLUMNS,
            ModelIdempotencyRecord.TABLE_NAME,
            setClause,
            whereEquals,
            Map.of(),
            Map.of(),
            Set.of(ModelIdempotencyRecord.HTTP_STATUS),
            Set.of());

    try {
      return datasourceOperations.executeUpdate(update) > 0;
    } catch (SQLException e) {
      throw new IdempotencyPersistenceException("Failed to finalize idempotency record", e);
    }
  }

  @Override
  public int purgeExpired(String realmId, Instant before) {
    try {
      QueryGenerator.PreparedQuery delete =
          QueryGenerator.generateDeleteQuery(
              ModelIdempotencyRecord.ALL_COLUMNS,
              ModelIdempotencyRecord.TABLE_NAME,
              Map.of(ModelIdempotencyRecord.REALM_ID, realmId),
              Map.of(),
              Map.of(ModelIdempotencyRecord.EXPIRES_AT, Timestamp.from(before)),
              Set.of(),
              Set.of(ModelIdempotencyRecord.EXPIRES_AT));
      return datasourceOperations.executeUpdate(delete);
    } catch (SQLException e) {
      throw new IdempotencyPersistenceException("Failed to purge expired idempotency records", e);
    }
  }
}
