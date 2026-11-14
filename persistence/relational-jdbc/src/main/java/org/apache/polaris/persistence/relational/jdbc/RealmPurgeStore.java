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
import java.util.Optional;
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

  /**
   * Queues {@code realmId} on an existing transaction {@code connection}, committing atomically
   * with the caller.
   *
   * <p>A realm already queued has its {@code requested_at} carried forward and its claim released,
   * because the data to delete now extends to this purge: the same realm name may have been
   * bootstrapped and written to since the earlier one, and a drain still running against the
   * earlier cutoff would leave that data behind for good. Carrying the timestamp forward never
   * moves it backwards, so the queued cutoff always covers every purge seen so far.
   */
  public void enqueueInTransaction(Connection connection, String realmId, long requestedAtMs)
      throws SQLException {
    String bump =
        "UPDATE "
            + TABLE
            + " SET requested_at = ?, status = 'pending', claimed_by = NULL, claimed_at = NULL"
            + " WHERE realm_id = ? AND requested_at < ?";
    int bumped =
        datasourceOperations.execute(
            connection, new PreparedQuery(bump, List.of(requestedAtMs, realmId, requestedAtMs)));
    if (bumped > 0) {
      return;
    }
    // Either the realm is not queued yet, or it is queued with a cutoff that already covers this
    // purge. ON CONFLICT DO NOTHING settles the second case (PostgreSQL, CockroachDB, H2).
    String insert =
        "INSERT INTO "
            + TABLE
            + " (realm_id, requested_at, status) VALUES (?, ?, 'pending') ON CONFLICT DO NOTHING";
    datasourceOperations.execute(
        connection, new PreparedQuery(insert, List.of(realmId, requestedAtMs)));
  }

  /**
   * Returns realms available to drain: {@code pending}, or {@code in_progress} with an expired
   * lease.
   */
  public List<PendingPurge> listClaimable(long staleClaimBeforeMs) {
    String sql =
        "SELECT realm_id, requested_at FROM "
            + TABLE
            + " WHERE status = 'pending' OR (status = 'in_progress' AND claimed_at < ?)";
    try {
      return datasourceOperations.executeSelect(
          new PreparedQuery(sql, List.of(staleClaimBeforeMs)), new PendingPurgeConverter());
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
            + TABLE
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

  /**
   * Extends {@code node}'s claim on {@code realmId}, returning {@code false} once another node has
   * taken it over, which is the drainer's signal to stop deleting.
   */
  public boolean renew(String realmId, String node, long nowMs) {
    String sql =
        "UPDATE "
            + TABLE
            + " SET claimed_at = ?"
            + " WHERE realm_id = ? AND claimed_by = ? AND status = 'in_progress'";
    try {
      return datasourceOperations.executeUpdate(
              new PreparedQuery(sql, List.of(nowMs, realmId, node)))
          > 0;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to renew the claim on realm %s for purge due to %s", realmId, e.getMessage()),
          e);
    }
  }

  /** The node holding the claim on {@code realmId}, so a drain can name whoever took it over. */
  public Optional<String> claimedBy(String realmId) {
    String sql = "SELECT claimed_by FROM " + TABLE + " WHERE realm_id = ?";
    try {
      return datasourceOperations
          .executeSelect(new PreparedQuery(sql, List.of(realmId)), new ClaimedByConverter())
          .stream()
          .flatMap(Optional::stream)
          .findFirst();
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to read the claim on realm %s due to %s", realmId, e.getMessage()),
          e);
    }
  }

  /**
   * Removes the marker once a realm has been fully drained, only while {@code node} still holds the
   * claim: a node whose lease expired mid-drain must not retire a realm another node is draining.
   *
   * @return whether the marker was removed
   */
  public boolean complete(String realmId, String node) {
    String sql = "DELETE FROM " + TABLE + " WHERE realm_id = ? AND claimed_by = ?";
    try {
      return datasourceOperations.executeUpdate(new PreparedQuery(sql, List.of(realmId, node))) > 0;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to complete purge of realm %s due to %s", realmId, e.getMessage()),
          e);
    }
  }

  /**
   * A queued realm purge. {@code requestedAtMs} bounds the data it covers: anything written at or
   * after it belongs to a realm of the same name bootstrapped later.
   */
  public record PendingPurge(String realmId, long requestedAtMs) {}

  private static final class ClaimedByConverter implements Converter<Optional<String>> {
    @Override
    public Optional<String> fromResultSet(ResultSet rs) throws SQLException {
      return Optional.ofNullable(rs.getString("claimed_by"));
    }

    @Override
    public Map<String, Object> toMap(DatabaseType databaseType) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class PendingPurgeConverter implements Converter<PendingPurge> {
    @Override
    public PendingPurge fromResultSet(ResultSet rs) throws SQLException {
      return new PendingPurge(rs.getString("realm_id"), rs.getLong("requested_at"));
    }

    @Override
    public Map<String, Object> toMap(DatabaseType databaseType) {
      throw new UnsupportedOperationException();
    }
  }
}
