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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.apache.polaris.core.persistence.IdempotencyPersistenceException;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator;
import org.apache.polaris.persistence.relational.jdbc.models.Converter;
import org.apache.polaris.persistence.relational.jdbc.models.ModelIdempotencyRecord;
import org.jspecify.annotations.NonNull;

/**
 * JDBC-backed {@link IdempotencyStore}.
 *
 * <p>Implements the "optimistic commit" model: a row is inserted only after the originating
 * operation has finalized. Race conditions between concurrent retries are detected via the table's
 * {@code (realm_id, idempotency_key)} primary key — a duplicate INSERT surfaces as a constraint
 * violation, which we translate into a {@link RecordResultType#DUPLICATE} along with the existing
 * row.
 *
 * <p>Following the {@code JdbcBasePersistenceImpl} pattern, an instance is bound to a single realm
 * at construction; realm scoping is then applied to every query via the {@code realm_id} column.
 */
public class RelationalJdbcIdempotencyStore implements IdempotencyStore {

  private final DatasourceOperations datasourceOperations;
  private final String realmId;

  public RelationalJdbcIdempotencyStore(
      @NonNull DatasourceOperations datasourceOperations, @NonNull String realmId) {
    this.datasourceOperations = datasourceOperations;
    this.realmId = realmId;
  }

  @Override
  public Optional<IdempotencyRecord> load(String idempotencyKey) {
    try {
      QueryGenerator.PreparedQuery query =
          QueryGenerator.generateSelectQuery(
              ModelIdempotencyRecord.ALL_COLUMNS,
              ModelIdempotencyRecord.TABLE_NAME,
              Map.of(
                  ModelIdempotencyRecord.REALM_ID, realmId,
                  ModelIdempotencyRecord.IDEMPOTENCY_KEY, idempotencyKey));
      List<IdempotencyRecord> results =
          datasourceOperations.executeSelect(
              query,
              new Converter<>() {
                @Override
                public IdempotencyRecord fromResultSet(ResultSet rs) throws SQLException {
                  return ModelIdempotencyRecord.fromRow(realmId, rs);
                }

                @Override
                public Map<String, Object> toMap(DatabaseType databaseType) {
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
  public RecordResult recordIfAbsent(
      String idempotencyKey,
      String operationType,
      String resourceHash,
      String principalHash,
      int httpStatus,
      String metadataLocation,
      Instant createdAt,
      Instant expiresAt) {
    try {
      Map<String, Object> insertMap = new LinkedHashMap<>();
      insertMap.put(ModelIdempotencyRecord.IDEMPOTENCY_KEY, idempotencyKey);
      insertMap.put(ModelIdempotencyRecord.OPERATION_TYPE, operationType);
      insertMap.put(ModelIdempotencyRecord.RESOURCE_HASH, resourceHash);
      insertMap.put(ModelIdempotencyRecord.PRINCIPAL_HASH, principalHash);
      insertMap.put(ModelIdempotencyRecord.HTTP_STATUS, httpStatus);
      insertMap.put(ModelIdempotencyRecord.METADATA_LOCATION, metadataLocation);
      insertMap.put(ModelIdempotencyRecord.CREATED_AT, Timestamp.from(createdAt));
      insertMap.put(ModelIdempotencyRecord.EXPIRES_AT, Timestamp.from(expiresAt));

      List<Object> values = insertMap.values().stream().toList();
      QueryGenerator.PreparedQuery insert =
          QueryGenerator.generateInsertQuery(
              ModelIdempotencyRecord.ALL_COLUMNS,
              ModelIdempotencyRecord.TABLE_NAME,
              values,
              realmId);
      datasourceOperations.executeUpdate(insert);
      return new RecordResult(RecordResultType.OWNED, Optional.empty());
    } catch (SQLException e) {
      if (datasourceOperations.isUniquenessConstraintViolation(e)) {
        Optional<IdempotencyRecord> existing = load(idempotencyKey);
        if (existing.isEmpty()) {
          // The insert lost the race on the (realm_id, idempotency_key) constraint, yet the winning
          // row is no longer visible (e.g. purged or rolled back between the conflict and this
          // reload). Surface a persistence error rather than a DUPLICATE without a record, which
          // the
          // handler layer treats as an invariant violation.
          throw new IdempotencyPersistenceException(
              "Insert for realm/key "
                  + realmId
                  + "/"
                  + idempotencyKey
                  + " conflicted on the unique constraint but the existing record could not be"
                  + " reloaded");
        }
        return new RecordResult(RecordResultType.DUPLICATE, existing);
      }
      throw new IdempotencyPersistenceException("Failed to record idempotency entry", e);
    }
  }

  @Override
  public int purgeExpired(Instant before) {
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
