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

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator.PreparedQuery;
import org.apache.polaris.persistence.relational.jdbc.RealmPurgeStore.PendingPurge;
import org.apache.polaris.persistence.relational.jdbc.models.Converter;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEvent;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.Test;

class RealmPurgeTest {

  private static final String SCAN_METRICS_TABLE = RealmDataPurgerProducer.SCAN_METRICS_REPORT;
  private static final String COMMIT_METRICS_TABLE = RealmDataPurgerProducer.COMMIT_METRICS_REPORT;
  private static final List<String> TIME_SERIES_TABLES =
      List.of(ModelEvent.TABLE_NAME, SCAN_METRICS_TABLE, COMMIT_METRICS_TABLE);

  // Rows written without an explicit timestamp predate every purge requested by these tests.
  private static final long DEFAULT_ROW_TIMESTAMP = 1_000L;
  private static final long PURGE_REQUESTED_AT = 100_000L;
  private static final long NOW = 500_000L;

  private DatasourceOperations newH2(String label) throws IOException, SQLException {
    JdbcConnectionPool ds =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:" + label + "_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
            "sa",
            "");
    DatasourceOperations ops = new DatasourceOperations(ds, new TestJdbcConfiguration());
    try (InputStream script = DatabaseType.H2.openInitScriptResource()) {
      ops.executeScript(script);
    }
    return ops;
  }

  private void writeEvents(DatasourceOperations ops, String realmId, int count) {
    writeEvents(ops, realmId, count, DEFAULT_ROW_TIMESTAMP);
  }

  private void writeEvents(DatasourceOperations ops, String realmId, int count, long timestampMs) {
    JdbcBasePersistenceImpl impl =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(), ops, RANDOM_SECRETS, realmId);
    for (int i = 0; i < count; i++) {
      impl.writeEvents(
          List.of(
              new EventEntity(
                  "catalog-1",
                  UUID.randomUUID().toString(),
                  "req-" + i,
                  "TEST_EVENT",
                  timestampMs,
                  "principal-1",
                  EventEntity.ResourceType.CATALOG,
                  "resource-1")));
    }
  }

  // Metrics report tables live in the relational-jdbc schema, but their write API moved to the
  // metrics extension (#5068), so rows are inserted directly (only the row's presence matters
  // here).
  private void writeMetrics(DatasourceOperations ops, String realmId, int count)
      throws SQLException {
    writeMetrics(ops, realmId, count, DEFAULT_ROW_TIMESTAMP);
  }

  private void writeMetrics(DatasourceOperations ops, String realmId, int count, long timestampMs)
      throws SQLException {
    for (int i = 0; i < count; i++) {
      ops.executeUpdate(
          new PreparedQuery(
              "INSERT INTO "
                  + SCAN_METRICS_TABLE
                  + " (report_id, realm_id, catalog_id, table_id, timestamp_ms) VALUES (?, ?, ?, ?, ?)",
              List.of(UUID.randomUUID().toString(), realmId, 1L, 2L, timestampMs)));
      ops.executeUpdate(
          new PreparedQuery(
              "INSERT INTO "
                  + COMMIT_METRICS_TABLE
                  + " (report_id, realm_id, catalog_id, table_id, timestamp_ms, snapshot_id, operation)"
                  + " VALUES (?, ?, ?, ?, ?, ?, ?)",
              List.of(UUID.randomUUID().toString(), realmId, 1L, 2L, timestampMs, 3L, "append")));
    }
  }

  private long countRows(DatasourceOperations ops, String table, String realmId)
      throws SQLException {
    String sql = "SELECT COUNT(*) AS c FROM " + table + " WHERE realm_id = ?";
    return ops.executeSelect(new PreparedQuery(sql, List.of(realmId)), new CountConverter()).get(0);
  }

  // Markers are only ever created inside the caller's transaction, as deleteAll does, so tests
  // seed them through that same path.
  private static void enqueue(DatasourceOperations ops, String realmId, long requestedAtMs)
      throws SQLException {
    RealmPurgeStore store = new RealmPurgeStore(ops);
    ops.runWithinTransaction(connection -> {
      store.enqueueInTransaction(connection, realmId, requestedAtMs);
      return true;
    });
  }

  private static Clock fixedClock() {
    return Clock.fixed(Instant.ofEpochMilli(NOW), ZoneOffset.UTC);
  }

  private static List<String> claimableRealms(RealmPurgeStore store, long staleClaimBeforeMs) {
    return store.listClaimable(staleClaimBeforeMs).stream().map(PendingPurge::realmId).toList();
  }

  // The standard set of purgers, mirroring RealmDataPurgerProducer (which supplies them via CDI in
  // production).
  private RealmPurgeService newService(DatasourceOperations ops) {
    return newService(ops, fixedClock());
  }

  private RealmPurgeService newService(DatasourceOperations ops, Clock clock) {
    return new RealmPurgeService(
        new RealmPurgeStore(ops),
        List.of(
            new TableRealmDataPurger(ops, ModelEvent.TABLE_NAME, "event_id"),
            new TableRealmDataPurger(ops, SCAN_METRICS_TABLE, "report_id"),
            new TableRealmDataPurger(ops, COMMIT_METRICS_TABLE, "report_id")),
        clock);
  }

  @Test
  void markerStoreEnqueueClaimComplete() throws IOException, SQLException {
    DatasourceOperations ops = newH2("marker");
    RealmPurgeStore store = new RealmPurgeStore(ops);

    enqueue(ops, "R1", 1000L);
    enqueue(ops, "R1", 2000L); // re-queued: the cutoff moves forward to the later purge
    assertThat(store.listClaimable(Long.MAX_VALUE)).containsExactly(new PendingPurge("R1", 2000L));

    // First claim wins; a second claim within the same lease window loses.
    assertThat(store.claim("R1", "nodeA", 5000L, 4000L)).isTrue();
    assertThat(store.claim("R1", "nodeB", 5001L, 4000L)).isFalse();

    // Not claimable while the claim is fresh (claimed_at=5000 is not < 4000)...
    assertThat(claimableRealms(store, 4000L)).isEmpty();
    // ...but reclaimable once the lease is considered expired (claimed_at=5000 < 6000).
    assertThat(claimableRealms(store, 6000L)).containsExactly("R1");
    assertThat(store.claim("R1", "nodeB", 7000L, 6000L)).isTrue();

    // The node that lost the claim can neither extend it nor retire the realm under its new owner.
    assertThat(store.renew("R1", "nodeA", 7500L)).isFalse();
    assertThat(store.complete("R1", "nodeA")).isFalse();
    assertThat(claimableRealms(store, Long.MAX_VALUE)).containsExactly("R1");

    assertThat(store.renew("R1", "nodeB", 7500L)).isTrue();
    assertThat(store.complete("R1", "nodeB")).isTrue();
    assertThat(claimableRealms(store, Long.MAX_VALUE)).isEmpty();
  }

  @Test
  void purgeBatchDrainsOnlyTargetRealmInBatches() throws IOException, SQLException {
    DatasourceOperations ops = newH2("purge");
    writeEvents(ops, "R1", 25);
    writeEvents(ops, "R2", 7);

    TableRealmDataPurger purger = new TableRealmDataPurger(ops, ModelEvent.TABLE_NAME, "event_id");

    long total = 0;
    int batches = 0;
    long deleted;
    while ((deleted = purger.purgeBatch("R1", PURGE_REQUESTED_AT, 10)) > 0) {
      assertThat(deleted).isLessThanOrEqualTo(10);
      total += deleted;
      batches++;
    }

    assertThat(total).isEqualTo(25);
    assertThat(batches).isEqualTo(3); // 10 + 10 + 5
    assertThat(countRows(ops, ModelEvent.TABLE_NAME, "R1")).isZero();
    assertThat(countRows(ops, ModelEvent.TABLE_NAME, "R2")).isEqualTo(7); // other realm untouched
  }

  @Test
  void drainOnceRemovesTimeSeriesDataAndClearsMarker() throws IOException, SQLException {
    DatasourceOperations ops = newH2("drain");
    writeEvents(ops, "R1", 23);
    writeMetrics(ops, "R1", 4);
    writeEvents(ops, "R2", 5);

    RealmPurgeService service = newService(ops);
    enqueue(ops, "R1", PURGE_REQUESTED_AT);

    assertThat(countRows(ops, ModelEvent.TABLE_NAME, "R1")).isEqualTo(23);
    assertThat(countRows(ops, SCAN_METRICS_TABLE, "R1")).isEqualTo(4);
    assertThat(countRows(ops, COMMIT_METRICS_TABLE, "R1")).isEqualTo(4);

    int drained = service.drainOnce("nodeA", 10, 60_000L, List.of(), line -> {});

    assertThat(drained).isEqualTo(1);
    assertThat(countRows(ops, ModelEvent.TABLE_NAME, "R1")).isZero();
    assertThat(countRows(ops, SCAN_METRICS_TABLE, "R1")).isZero();
    assertThat(countRows(ops, COMMIT_METRICS_TABLE, "R1")).isZero();
    assertThat(countRows(ops, ModelEvent.TABLE_NAME, "R2")).isEqualTo(5); // other realm untouched
    assertThat(claimableRealms(new RealmPurgeStore(ops), Long.MAX_VALUE)).isEmpty();
  }

  @Test
  void drainOnceRejectsNonPositiveBatchSizeInsteadOfClearingMarkers()
      throws IOException, SQLException {
    DatasourceOperations ops = newH2("batch-guard");
    RealmPurgeStore store = new RealmPurgeStore(ops);
    enqueue(ops, "R1", 1000L);

    RealmPurgeService service = newService(ops);
    assertThatThrownBy(() -> service.drainOnce("nodeA", 0, 60_000L, List.of(), line -> {}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("batchSize");

    assertThat(claimableRealms(store, Long.MAX_VALUE)).containsExactly("R1"); // marker survives
  }

  @Test
  void drainOnceIsolatesAFailingRealmAndDrainsTheRest() throws IOException, SQLException {
    DatasourceOperations ops = newH2("drain-isolate");
    RealmPurgeStore store = new RealmPurgeStore(ops);
    enqueue(ops, "BAD", PURGE_REQUESTED_AT);
    enqueue(ops, "GOOD", PURGE_REQUESTED_AT);

    // A purger that fails only for realm "BAD".
    RealmDataPurger flaky =
        new RealmDataPurger() {
          @Override
          public long purgeBatch(String realmId, long cutoffMs, int batchSize) {
            if ("BAD".equals(realmId)) {
              throw new RuntimeException("boom");
            }
            return 0; // "GOOD" is already empty
          }
        };
    RealmPurgeService service = new RealmPurgeService(store, List.of(flaky), fixedClock());

    int drained = service.drainOnce("nodeA", 10, 60_000L, List.of(), line -> {});

    assertThat(drained).isEqualTo(1); // healthy realm drained
    assertThat(claimableRealms(store, Long.MAX_VALUE)).containsExactly("BAD"); // retried later
  }

  @Test
  void deleteAllAtomicallyEnqueuesRealmForPurgeAndLeavesTimeSeriesData()
      throws IOException, SQLException {
    DatasourceOperations ops = newH2("deleteall-enqueue");
    for (String realmId : List.of("R1", "R2")) {
      writeEvents(ops, realmId, 3);
      writeMetrics(ops, realmId, 3);
    }

    RealmContext realm = () -> "R1";
    JdbcBasePersistenceImpl impl =
        new JdbcBasePersistenceImpl(new PolarisDefaultDiagServiceImpl(), ops, RANDOM_SECRETS, "R1");
    impl.deleteAll(new PolarisCallContext(realm, impl));

    assertThat(claimableRealms(new RealmPurgeStore(ops), Long.MAX_VALUE))
        .containsExactly("R1"); // only the purged realm is queued

    // Every time-series table of the purged realm survives deleteAll, for the drainer to delete.
    for (String table : TIME_SERIES_TABLES) {
      assertThat(countRows(ops, table, "R1"))
          .as("rows in %s of the purged realm", table)
          .isEqualTo(3);
      assertThat(countRows(ops, table, "R2")).as("rows in %s of another realm", table).isEqualTo(3);
    }
  }

  /**
   * A realm of the same name can be bootstrapped before its purge is drained. The drain must delete
   * only the retired realm's data, which the marker's {@code requested_at} bounds.
   */
  @Test
  void drainOncePreservesTheDataOfARealmBootstrappedAfterThePurge()
      throws IOException, SQLException {
    DatasourceOperations ops = newH2("drain-rebootstrap");
    writeEvents(ops, "R1", 6, PURGE_REQUESTED_AT - 1); // retired realm
    writeMetrics(ops, "R1", 6, PURGE_REQUESTED_AT - 1);

    RealmPurgeService service = newService(ops);
    enqueue(ops, "R1", PURGE_REQUESTED_AT);

    // The realm is bootstrapped again under the same name and starts writing before the drain runs.
    writeEvents(ops, "R1", 4, PURGE_REQUESTED_AT + 1);
    writeMetrics(ops, "R1", 4, PURGE_REQUESTED_AT + 1);

    int drained = service.drainOnce("nodeA", 10, 60_000L, List.of(), line -> {});

    assertThat(drained).isEqualTo(1);
    for (String table : TIME_SERIES_TABLES) {
      assertThat(countRows(ops, table, "R1"))
          .as("rows in %s written after the purge was requested", table)
          .isEqualTo(4);
    }
    assertThat(claimableRealms(new RealmPurgeStore(ops), Long.MAX_VALUE)).isEmpty();
  }

  /** A drain restricted to named realms leaves the others queued, and says so for unknown names. */
  @Test
  void drainOnceDrainsOnlyTheRequestedRealms() throws IOException, SQLException {
    DatasourceOperations ops = newH2("drain-selected");
    for (String realmId : List.of("R1", "R2")) {
      writeEvents(ops, realmId, 3, PURGE_REQUESTED_AT - 1);
      enqueue(ops, realmId, PURGE_REQUESTED_AT);
    }

    List<String> progress = new ArrayList<>();
    assertThat(newService(ops).drainOnce("nodeA", 10, 60_000L, List.of("R1", "R3"), progress::add))
        .isEqualTo(1);

    assertThat(countRows(ops, ModelEvent.TABLE_NAME, "R1")).isZero();
    assertThat(countRows(ops, ModelEvent.TABLE_NAME, "R2")).isEqualTo(3); // not requested
    assertThat(claimableRealms(new RealmPurgeStore(ops), Long.MAX_VALUE)).containsExactly("R2");
    assertThat(progress)
        .anyMatch(line -> line.contains("R3") && line.contains("no queued purge"))
        .anyMatch(line -> line.contains("Drained realm R1") && line.contains("3 row(s)"));
  }

  /** An exhausted store is not queried again, so a long drain costs no scans on the empty ones. */
  @Test
  void drainRealmStopsQueryingAStoreOnceItIsEmpty() throws IOException, SQLException {
    DatasourceOperations ops = newH2("drain-exhausted");
    enqueue(ops, "R1", PURGE_REQUESTED_AT);

    // Two stores: one with a single batch of rows, one that keeps returning a full batch for a
    // while. The first must not be asked again after it reports a partial batch.
    AtomicInteger emptyingCalls = new AtomicInteger();
    RealmDataPurger emptying =
        (realmId, cutoffMs, batchSize) -> {
          emptyingCalls.incrementAndGet();
          return batchSize - 1L; // a partial batch: nothing left behind it
        };
    AtomicInteger slowRounds = new AtomicInteger();
    RealmDataPurger slow =
        (realmId, cutoffMs, batchSize) -> slowRounds.incrementAndGet() < 4 ? batchSize : 0L;

    RealmPurgeService service =
        new RealmPurgeService(new RealmPurgeStore(ops), List.of(emptying, slow), fixedClock());

    assertThat(service.drainOnce("nodeA", 10, 60_000L, List.of(), line -> {})).isEqualTo(1);

    assertThat(emptyingCalls).hasValue(1); // asked once, never again
    assertThat(slowRounds).hasValue(4);
  }

  /** A realm being drained elsewhere reads differently from one with nothing queued. */
  @Test
  void drainOnceTellsALiveClaimApartFromNothingQueued() throws IOException, SQLException {
    DatasourceOperations ops = newH2("drain-requested-states");
    RealmPurgeStore store = new RealmPurgeStore(ops);
    enqueue(ops, "R1", PURGE_REQUESTED_AT);
    assertThat(store.claim("R1", "nodeB", NOW, NOW - 1)).isTrue(); // held by another drainer

    List<String> progress = new ArrayList<>();
    assertThat(newService(ops).drainOnce("nodeA", 10, 60_000L, List.of("R1", "R2"), progress::add))
        .isZero();

    assertThat(progress)
        .anyMatch(line -> line.contains("R1") && line.contains("being drained by nodeB"))
        .anyMatch(line -> line.contains("R2") && line.contains("no queued purge"));
  }

  /** Taking a claim over names the new owner, so an operator can tell what stopped the drain. */
  @Test
  void drainOnceReportsTheNodeThatTookTheClaimOver() throws IOException, SQLException {
    DatasourceOperations ops = newH2("drain-report-owner");
    RealmPurgeStore store = new RealmPurgeStore(ops);
    enqueue(ops, "R1", PURGE_REQUESTED_AT);

    RealmDataPurger stolenMidDrain =
        new RealmDataPurger() {
          private int rounds;

          @Override
          public long purgeBatch(String realmId, long cutoffMs, int batchSize) {
            store.claim(realmId, "nodeB", NOW, NOW + 1);
            return ++rounds < 3 ? batchSize : 0;
          }
        };
    List<String> progress = new ArrayList<>();
    RealmPurgeService service = new RealmPurgeService(store, List.of(stolenMidDrain), fixedClock());

    assertThat(service.drainOnce("nodeA", 10, 60_000L, List.of(), progress::add)).isZero();

    assertThat(progress)
        .anyMatch(line -> line.contains("Stopped draining realm R1") && line.contains("nodeB"));
    assertThat(store.claimedBy("R1")).contains("nodeB");
  }

  /**
   * The same realm name can be purged, bootstrapped, written to and purged again before any drain
   * runs. The second purge must carry the cutoff forward, or the data written in between is never
   * reclaimed once the marker is cleared.
   */
  @Test
  void drainOnceReclaimsDataWrittenBetweenTwoPurgesOfTheSameRealm()
      throws IOException, SQLException {
    DatasourceOperations ops = newH2("drain-repurge");
    long firstPurge = PURGE_REQUESTED_AT;
    long secondPurge = PURGE_REQUESTED_AT + 1_000;

    writeEvents(ops, "R1", 3, firstPurge - 1); // first incarnation
    enqueue(ops, "R1", firstPurge);

    // The realm is bootstrapped again, writes, and is purged a second time before any drain runs.
    writeEvents(ops, "R1", 5, firstPurge + 1);
    writeMetrics(ops, "R1", 5, firstPurge + 1);
    enqueue(ops, "R1", secondPurge);

    assertThat(newService(ops).drainOnce("nodeA", 10, 60_000L, List.of(), line -> {})).isEqualTo(1);

    for (String table : TIME_SERIES_TABLES) {
      assertThat(countRows(ops, table, "R1"))
          .as("rows in %s left behind by the second purge", table)
          .isZero();
    }
    assertThat(claimableRealms(new RealmPurgeStore(ops), Long.MAX_VALUE)).isEmpty();
  }

  /** A re-queue releases the claim, so a drain still running on the stale cutoff stands down. */
  @Test
  void enqueueReleasesAnOutstandingClaimSoTheStaleCutoffIsNotDrained()
      throws IOException, SQLException {
    DatasourceOperations ops = newH2("repurge-claim");
    RealmPurgeStore store = new RealmPurgeStore(ops);
    enqueue(ops, "R1", PURGE_REQUESTED_AT);
    assertThat(store.claim("R1", "nodeA", NOW, NOW - 1)).isTrue();

    enqueue(ops, "R1", PURGE_REQUESTED_AT + 1_000);

    assertThat(store.renew("R1", "nodeA", NOW)).isFalse();
    assertThat(store.complete("R1", "nodeA")).isFalse();
    assertThat(store.listClaimable(Long.MAX_VALUE))
        .containsExactly(new PendingPurge("R1", PURGE_REQUESTED_AT + 1_000));
  }

  /**
   * A drain that outlives its lease must stop rather than race the node that took the claim over,
   * and must not retire a realm it no longer owns.
   */
  @Test
  void drainOnceStopsAndKeepsTheMarkerWhenAnotherNodeTakesOverTheClaim()
      throws IOException, SQLException {
    DatasourceOperations ops = newH2("drain-takeover");
    RealmPurgeStore store = new RealmPurgeStore(ops);
    enqueue(ops, "R1", PURGE_REQUESTED_AT);

    // Simulates nodeA's lease expiring mid-drain: another node claims the realm after the first
    // batch, while nodeA still reports rows left to delete.
    RealmDataPurger stolenMidDrain =
        new RealmDataPurger() {
          private int rounds;

          @Override
          public long purgeBatch(String realmId, long cutoffMs, int batchSize) {
            store.claim(realmId, "nodeB", NOW, NOW + 1);
            // Reports work left so the drainer renews its claim, and eventually reports none so a
            // drainer that never renews still terminates.
            return ++rounds < 3 ? batchSize : 0;
          }
        };
    RealmPurgeService service = new RealmPurgeService(store, List.of(stolenMidDrain), fixedClock());

    assertThat(service.drainOnce("nodeA", 10, 60_000L, List.of(), line -> {})).isZero();

    // The marker survives under its new owner, so the realm is drained by whoever holds the claim.
    assertThat(claimableRealms(store, Long.MAX_VALUE)).containsExactly("R1");
    assertThat(store.complete("R1", "nodeA")).isFalse();
    assertThat(store.complete("R1", "nodeB")).isTrue();
  }

  private static final class CountConverter implements Converter<Long> {
    @Override
    public Long fromResultSet(ResultSet rs) throws SQLException {
      return rs.getLong("c");
    }

    @Override
    public Map<String, Object> toMap(DatabaseType databaseType) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class TestJdbcConfiguration implements RelationalJdbcConfiguration {
    @Override
    public Optional<Integer> maxRetries() {
      return Optional.of(1);
    }

    @Override
    public Optional<Long> maxDurationInMs() {
      return Optional.of(100L);
    }

    @Override
    public Optional<Long> initialDelayInMs() {
      return Optional.of(100L);
    }

    @Override
    public Optional<String> databaseType() {
      return Optional.of("h2");
    }
  }
}
