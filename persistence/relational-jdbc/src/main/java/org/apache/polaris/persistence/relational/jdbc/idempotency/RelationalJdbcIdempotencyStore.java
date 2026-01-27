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

import jakarta.annotation.Nonnull;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Arrays;
import javax.sql.DataSource;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.apache.polaris.persistence.relational.jdbc.models.ImmutableModelIdempotencyRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelIdempotencyRecord;

/** {@link IdempotencyStore} backed by the relational-jdbc schema. */
public class RelationalJdbcIdempotencyStore implements IdempotencyStore {

  private final DatasourceOperations datasourceOperations;

  public RelationalJdbcIdempotencyStore(
      @Nonnull DataSource dataSource, @Nonnull RelationalJdbcConfiguration cfg) throws SQLException {
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
      ModelIdempotencyRecord model =
          ImmutableModelIdempotencyRecord.builder()
              .realmId(realmId)
              .idempotencyKey(idempotencyKey)
              .operationType(operationType)
              .resourceId(normalizedResourceId)
              .createdAt(now)
              .updatedAt(now)
              .heartbeatAt(now)
              .executorId(executorId)
              .expiresAt(expiresAt)
              .build();

      List<Object> values = model.toMap(datasourceOperations.databaseType()).values().stream().toList();
      QueryGenerator.PreparedQuery insert =
          QueryGenerator.generateInsertQuery(
              ModelIdempotencyRecord.ALL_COLUMNS, ModelIdempotencyRecord.TABLE_NAME, values, realmId);
      datasourceOperations.executeUpdate(insert);
      return new ReserveResult(ReserveResultType.OWNED, Optional.empty());
    } catch (SQLException e) {
      if (datasourceOperations.isConstraintViolation(e)) {
        return new ReserveResult(ReserveResultType.DUPLICATE, load(realmId, idempotencyKey));
      }
      throw new RuntimeException("Failed to reserve idempotency key", e);
    }
  }

  @Override
  public Optional<IdempotencyRecord> load(String realmId, String idempotencyKey) {
    try {
      QueryGenerator.PreparedQuery query =
          QueryGenerator.generateSelectQuery(
              ModelIdempotencyRecord.SELECT_COLUMNS,
              ModelIdempotencyRecord.TABLE_NAME,
              Map.of(
                  ModelIdempotencyRecord.REALM_ID,
                  realmId,
                  ModelIdempotencyRecord.IDEMPOTENCY_KEY,
                  idempotencyKey));
      List<IdempotencyRecord> results =
          datasourceOperations.executeSelect(query, ModelIdempotencyRecord.CONVERTER);
      if (results.isEmpty()) {
        return Optional.empty();
      }
      if (results.size() > 1) {
        throw new IllegalStateException(
            "More than one idempotency record found for realm/key: " + realmId + "/" + idempotencyKey);
      }
      return Optional.of(results.getFirst());
    } catch (SQLException e) {
      throw new RuntimeException("Failed to load idempotency record", e);
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
    if (record.getHttpStatus() != null) {
      return HeartbeatResult.FINALIZED;
    }
    if (record.getExecutorId() == null || !record.getExecutorId().equals(executorId)) {
      return HeartbeatResult.LOST_OWNERSHIP;
    }

    String sql =
        "UPDATE "
            + QueryGenerator.fullyQualifiedTableName(ModelIdempotencyRecord.TABLE_NAME)
            + " SET heartbeat_at = ?, updated_at = ?"
            + " WHERE realm_id = ? AND idempotency_key = ? AND http_status IS NULL AND executor_id = ?";
    QueryGenerator.PreparedQuery update =
        new QueryGenerator.PreparedQuery(
            sql,
            List.of(
                Timestamp.from(now),
                Timestamp.from(now),
                realmId,
                idempotencyKey,
                executorId));

    try {
      int updated = datasourceOperations.executeUpdate(update);
      if (updated > 0) {
        return HeartbeatResult.UPDATED;
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to update idempotency heartbeat", e);
    }

    // Raced with finalize/ownership loss; re-check to return a meaningful result.
    Optional<IdempotencyRecord> after = load(realmId, idempotencyKey);
    if (after.isEmpty()) {
      return HeartbeatResult.NOT_FOUND;
    }
    if (after.get().getHttpStatus() != null) {
      return HeartbeatResult.FINALIZED;
    }
    return HeartbeatResult.LOST_OWNERSHIP;
  }

  @Override
  public boolean finalizeRecord(
      String realmId,
      String idempotencyKey,
      Integer httpStatus,
      String errorSubtype,
      String responseSummary,
      String responseHeaders,
      Instant finalizedAt) {
    String sql =
        "UPDATE "
            + QueryGenerator.fullyQualifiedTableName(ModelIdempotencyRecord.TABLE_NAME)
            + " SET http_status = ?, error_subtype = ?, response_summary = ?, response_headers = ?,"
            + " finalized_at = ?, updated_at = ?"
            + " WHERE realm_id = ? AND idempotency_key = ? AND http_status IS NULL";
    QueryGenerator.PreparedQuery update =
        new QueryGenerator.PreparedQuery(
            sql,
            Arrays.asList(
                httpStatus,
                errorSubtype,
                responseSummary,
                responseHeaders,
                Timestamp.from(finalizedAt),
                Timestamp.from(finalizedAt),
                realmId,
                idempotencyKey));

    try {
      return datasourceOperations.executeUpdate(update) > 0;
    } catch (SQLException e) {
      throw new RuntimeException("Failed to finalize idempotency record", e);
    }
  }

  @Override
  public int purgeExpired(String realmId, Instant before) {
    try {
      String sql =
          "DELETE FROM "
              + QueryGenerator.fullyQualifiedTableName(ModelIdempotencyRecord.TABLE_NAME)
              + " WHERE realm_id = ? AND expires_at IS NOT NULL AND expires_at < ?";
      QueryGenerator.PreparedQuery delete =
          new QueryGenerator.PreparedQuery(sql, List.of(realmId, Timestamp.from(before)));
      return datasourceOperations.executeUpdate(delete);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to purge expired idempotency records", e);
    }
  }
}
