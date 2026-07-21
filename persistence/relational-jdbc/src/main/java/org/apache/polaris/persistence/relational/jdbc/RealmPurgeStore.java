/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.apache.polaris.persistence.relational.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator.PreparedQuery;
import org.apache.polaris.persistence.relational.jdbc.models.Converter;

/**
 * DAO for the {@code realm_purge} queue of realms whose time-series data is pending async batched
 * deletion. Cross-node coordination is via the {@link #claim} conditional update plus a lease.
 */
public class RealmPurgeStore {

  private static final String TABLE = "realm_purge";

  private final DatasourceOperations datasourceOperations;

  public RealmPurgeStore(DatasourceOperations datasourceOperations) {
    this.datasourceOperations = datasourceOperations;
  }

  private PreparedQuery enqueueQuery(String realmId, long requestedAtMs) {
    // ON CONFLICT DO NOTHING keeps enqueue idempotent (PostgreSQL, CockroachDB, H2).
    String sql =
        "INSERT INTO "
            + QueryGenerator.getFullyQualifiedTableName(TABLE)
            + " (realm_id, requested_at, status) VALUES (?, ?, 'pending') ON CONFLICT DO NOTHING";
    return new PreparedQuery(sql, List.of(realmId, requestedAtMs));
  }

  /** Records {@code realmId} as pending purge; a no-op if it is already queued. */
  public void enqueue(String realmId, long requestedAtMs) {
    try {
      datasourceOperations.executeUpdate(enqueueQuery(realmId, requestedAtMs));
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to enqueue realm %s for purge due to %s", realmId, e.getMessage()),
          e);
    }
  }

  /**
   * Enqueues on an existing transaction {@code connection}, committing atomically with the caller.
   */
  public void enqueueInTransaction(Connection connection, String realmId, long requestedAtMs)
      throws SQLException {
    datasourceOperations.execute(connection, enqueueQuery(realmId, requestedAtMs));
  }

  /**
   * Returns realms available to drain: {@code pending}, or {@code in_progress} with an expired
   * lease.
   */
  public List<String> listClaimable(long staleClaimBeforeMs) {
    String sql =
        "SELECT realm_id FROM "
            + QueryGenerator.getFullyQualifiedTableName(TABLE)
            + " WHERE status = 'pending' OR (status = 'in_progress' AND claimed_at < ?)";
    try {
      return datasourceOperations.executeSelect(
          new PreparedQuery(sql, List.of(staleClaimBeforeMs)), new RealmIdConverter());
    } catch (SQLException e) {
      throw new RuntimeException(
          "Failed to list claimable realm purges due to " + e.getMessage(), e);
    }
  }

  /**
   * Atomically claims {@code realmId} for {@code node}, succeeding only if it is pending or its
   * previous claim has expired — one active drainer per realm without leader election.
   */
  public boolean claim(String realmId, String node, long nowMs, long staleClaimBeforeMs) {
    String sql =
        "UPDATE "
            + QueryGenerator.getFullyQualifiedTableName(TABLE)
            + " SET status = 'in_progress', claimed_by = ?, claimed_at = ?"
            + " WHERE realm_id = ? AND (status = 'pending' OR claimed_at < ?)";
    try {
      return datasourceOperations.executeUpdate(
              new PreparedQuery(sql, List.of(node, nowMs, realmId, staleClaimBeforeMs)))
          > 0;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to claim realm %s for purge due to %s", realmId, e.getMessage()),
          e);
    }
  }

  /** Removes the marker once a realm has been fully drained. */
  public void complete(String realmId) {
    String sql =
        "DELETE FROM " + QueryGenerator.getFullyQualifiedTableName(TABLE) + " WHERE realm_id = ?";
    try {
      datasourceOperations.executeUpdate(new PreparedQuery(sql, List.of(realmId)));
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to complete purge of realm %s due to %s", realmId, e.getMessage()),
          e);
    }
  }

  private static final class RealmIdConverter implements Converter<String> {
    @Override
    public String fromResultSet(ResultSet rs) throws SQLException {
      return rs.getString("realm_id");
    }

    @Override
    public Map<String, Object> toMap(DatabaseType databaseType) {
      throw new UnsupportedOperationException();
    }
  }
}
