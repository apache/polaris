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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.polaris.idempotency.IdempotencyRecord;
import org.apache.polaris.idempotency.IdempotencyStore;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.apache.polaris.persistence.relational.jdbc.models.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Postgres implementation of IdempotencyStore.
 */
public final class PostgresIdempotencyStore implements IdempotencyStore {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresIdempotencyStore.class);

  private static final String TABLE = "POLARIS_SCHEMA.idempotency_records";

  private final DatasourceOperations ops;

  public PostgresIdempotencyStore(@Nonnull DataSource dataSource,
                                  @Nonnull RelationalJdbcConfiguration cfg)
      throws SQLException {
    this.ops = new DatasourceOperations(dataSource, cfg);
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
    String sql =
        "INSERT INTO "
            + TABLE
            + " (realm_id, idempotency_key, operation_type, resource_id,"
            + " http_status, error_subtype, response_summary, response_headers, finalized_at,"
            + " created_at, updated_at, heartbeat_at, executor_id, expires_at)"
            + " VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, ?, ?)"
            + " ON CONFLICT (realm_id, idempotency_key) DO NOTHING";
    List<Object> params =
        List.of(
            realmId,
            idempotencyKey,
            operationType,
            normalizedResourceId,
            Timestamp.from(now),
            Timestamp.from(now),
            Timestamp.from(now),
            executorId,
            Timestamp.from(expiresAt));
    try {
      int updated = ops.executeUpdate(new QueryGenerator.PreparedQuery(sql, params));
      if (updated == 1) {
        return new ReserveResult(ReserveResultType.OWNED, Optional.empty());
      } else {
        // Load existing to return to caller
        return new ReserveResult(ReserveResultType.DUPLICATE, load(realmId, idempotencyKey));
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to reserve idempotency key", e);
    }
  }

  @Override
  public Optional<IdempotencyRecord> load(String realmId, String idempotencyKey) {
    String sql =
        "SELECT realm_id, idempotency_key, operation_type, resource_id, http_status, error_subtype,"
            + " response_summary, response_headers, created_at, updated_at, finalized_at, heartbeat_at,"
            + " executor_id, expires_at"
            + " FROM "
            + TABLE
            + " WHERE realm_id = ? AND idempotency_key = ?";
    try {
      final IdempotencyRecord[] holder = new IdempotencyRecord[1];
      ops.executeSelectOverStream(
          new QueryGenerator.PreparedQuery(sql, List.of(realmId, idempotencyKey)),
          new Converter<IdempotencyRecord>() {
            @Override
            public IdempotencyRecord fromResultSet(ResultSet rs) throws SQLException {
              return convert(rs);
            }

            @Override
            public Map<String, Object> toMap(
               DatabaseType databaseType) {
              return Map.of();
            }
          },
          stream -> stream.findFirst().ifPresent(r -> holder[0] = r));
      return Optional.ofNullable(holder[0]);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to load idempotency record", e);
    }
  }

  @Override
  public boolean updateHeartbeat(
      String realmId, String idempotencyKey, String executorId, Instant now) {
    String sql =
        "UPDATE "
            + TABLE
            + " SET heartbeat_at = ?, updated_at = ?"
            + " WHERE realm_id = ? AND idempotency_key = ?"
            + " AND http_status IS NULL"
            + " AND (executor_id IS NULL OR executor_id = ?)";
    try {
      int rows =
          ops.executeUpdate(
              new QueryGenerator.PreparedQuery(
                  sql,
                  List.of(
                      Timestamp.from(now),
                      Timestamp.from(now),
                      realmId,
                      idempotencyKey,
                      executorId)));
      return rows > 0;
    } catch (SQLException e) {
      throw new RuntimeException("Failed to update heartbeat", e);
    }
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
            + TABLE
            + " SET http_status = ?, error_subtype = ?, response_summary = ?, response_headers = ?,"
            + " finalized_at = ?, updated_at = ?"
            + " WHERE realm_id = ? AND idempotency_key = ? AND http_status IS NULL";
    try {
      int rows =
          ops.executeUpdate(
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
                      idempotencyKey)));
      return rows > 0;
    } catch (SQLException e) {
      throw new RuntimeException("Failed to finalize idempotency record", e);
    }
  }

  @Override
  public int purgeExpired(Instant before) {
    String sql = "DELETE FROM " + TABLE + " WHERE expires_at < ?";
    try {
      return ops.executeUpdate(new QueryGenerator.PreparedQuery(sql, List.of(Timestamp.from(before))));
    } catch (SQLException e) {
      throw new RuntimeException("Failed to purge expired idempotency records", e);
    }
  }

  private static IdempotencyRecord convert(ResultSet rs) {
    try {
      String realmId = rs.getString("realm_id");
      String idempotencyKey = rs.getString("idempotency_key");
      String operationType = rs.getString("operation_type");
      String resourceId = rs.getString("resource_id");
      Integer httpStatus = (Integer) rs.getObject("http_status");
      String errorSubtype = rs.getString("error_subtype");
      String responseSummary = rs.getString("response_summary");
      String responseHeaders = rs.getString("response_headers");
      Instant createdAt = rs.getTimestamp("created_at").toInstant();
      Instant updatedAt = rs.getTimestamp("updated_at").toInstant();
      Timestamp fts = rs.getTimestamp("finalized_at");
      Instant finalizedAt = fts == null ? null : fts.toInstant();
      Timestamp hb = rs.getTimestamp("heartbeat_at");
      Instant heartbeatAt = hb == null ? null : hb.toInstant();
      String executorId = rs.getString("executor_id");
      Instant expiresAt = rs.getTimestamp("expires_at").toInstant();
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
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}


