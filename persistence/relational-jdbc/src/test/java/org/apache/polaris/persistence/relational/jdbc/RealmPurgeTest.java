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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator.PreparedQuery;
import org.apache.polaris.persistence.relational.jdbc.models.Converter;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEvent;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.Test;

class RealmPurgeTest {

  private static final int SCHEMA_VERSION = 5;
  private static final String SCAN_METRICS_TABLE = RealmDataPurgerProducer.SCAN_METRICS_REPORT;
  private static final String COMMIT_METRICS_TABLE = RealmDataPurgerProducer.COMMIT_METRICS_REPORT;

  private DatasourceOperations newH2(String label) throws IOException, SQLException {
    JdbcConnectionPool ds =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:" + label + "_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
            "sa",
            "");
    DatasourceOperations ops = new DatasourceOperations(ds, new TestJdbcConfiguration());
    try (InputStream script = DatabaseType.H2.openInitScriptResource(SCHEMA_VERSION)) {
      ops.executeScript(script);
    }
    return ops;
  }

  private void writeEvents(DatasourceOperations ops, String realmId, int count) {
    JdbcBasePersistenceImpl impl =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(), ops, RANDOM_SECRETS, realmId, SCHEMA_VERSION);
    for (int i = 0; i < count; i++) {
      impl.writeEvents(
          List.of(
              new EventEntity(
                  "catalog-1",
                  UUID.randomUUID().toString(),
                  "req-" + i,
                  "TEST_EVENT",
                  System.currentTimeMillis(),
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
    for (int i = 0; i < count; i++) {
      ops.executeUpdate(
          new PreparedQuery(
              "INSERT INTO "
                  + QueryGenerator.getFullyQualifiedTableName(SCAN_METRICS_TABLE)
                  + " (report_id, realm_id, catalog_id, table_id, timestamp_ms) VALUES (?, ?, ?, ?, ?)",
              List.of(UUID.randomUUID().toString(), realmId, 1L, 2L, 1L)));
      ops.executeUpdate(
          new PreparedQuery(
              "INSERT INTO "
                  + QueryGenerator.getFullyQualifiedTableName(COMMIT_METRICS_TABLE)
                  + " (report_id, realm_id, catalog_id, table_id, timestamp_ms, snapshot_id, operation)"
                  + " VALUES (?, ?, ?, ?, ?, ?, ?)",
              List.of(UUID.randomUUID().toString(), realmId, 1L, 2L, 1L, 3L, "append")));
    }
  }

  private long countRows(DatasourceOperations ops, String table, String realmId)
      throws SQLException {
    String sql =
        "SELECT COUNT(*) AS c FROM "
            + QueryGenerator.getFullyQualifiedTableName(table)
            + " WHERE realm_id = ?";
    return ops.executeSelect(new PreparedQuery(sql, List.of(realmId)), new CountConverter()).get(0);
  }

  // The standard set of purgers, mirroring RealmDataPurgerProducer (which supplies them via CDI in
  // production).
  private RealmPurgeService newService(DatasourceOperations ops) {
    return new RealmPurgeService(
        new RealmPurgeStore(ops),
        List.of(
            new TableRealmDataPurger(ops, ModelEvent.TABLE_NAME, "event_id"),
            new TableRealmDataPurger(ops, SCAN_METRICS_TABLE, "report_id"),
            new TableRealmDataPurger(ops, COMMIT_METRICS_TABLE, "report_id")));
  }

  @Test
  void markerStoreEnqueueClaimComplete() throws IOException, SQLException {
    DatasourceOperations ops = newH2("marker");
    RealmPurgeStore store = new RealmPurgeStore(ops);

    store.enqueue("R1", 1000L);
    store.enqueue("R1", 2000L); // idempotent, still one row
    assertThat(store.listClaimable(Long.MAX_VALUE)).containsExactly("R1");

    // First claim wins; a second claim within the same lease window loses.
    assertThat(store.claim("R1", "nodeA", 5000L, 4000L)).isTrue();
    assertThat(store.claim("R1", "nodeB", 5001L, 4000L)).isFalse();

    // Not claimable while the claim is fresh (claimed_at=5000 is not < 4000)...
    assertThat(store.listClaimable(4000L)).isEmpty();
    // ...but reclaimable once the lease is considered expired (claimed_at=5000 < 6000).
    assertThat(store.listClaimable(6000L)).containsExactly("R1");
    assertThat(store.claim("R1", "nodeB", 7000L, 6000L)).isTrue();

    store.complete("R1");
    assertThat(store.listClaimable(Long.MAX_VALUE)).isEmpty();
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
    while ((deleted = purger.purgeBatch("R1", 10)) > 0) {
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
    service.enqueue("R1", 1000L);

    assertThat(countRows(ops, ModelEvent.TABLE_NAME, "R1")).isEqualTo(23);
    assertThat(countRows(ops, SCAN_METRICS_TABLE, "R1")).isEqualTo(4);
    assertThat(countRows(ops, COMMIT_METRICS_TABLE, "R1")).isEqualTo(4);

    int drained = service.drainOnce("nodeA", 10, 60_000L, 100_000L);

    assertThat(drained).isEqualTo(1);
    assertThat(countRows(ops, ModelEvent.TABLE_NAME, "R1")).isZero();
    assertThat(countRows(ops, SCAN_METRICS_TABLE, "R1")).isZero();
    assertThat(countRows(ops, COMMIT_METRICS_TABLE, "R1")).isZero();
    assertThat(countRows(ops, ModelEvent.TABLE_NAME, "R2")).isEqualTo(5); // other realm untouched
    assertThat(new RealmPurgeStore(ops).listClaimable(Long.MAX_VALUE)).isEmpty(); // marker cleared
  }

  @Test
  void drainOnceRejectsNonPositiveBatchSizeInsteadOfClearingMarkers()
      throws IOException, SQLException {
    DatasourceOperations ops = newH2("batch-guard");
    RealmPurgeStore store = new RealmPurgeStore(ops);
    store.enqueue("R1", 1000L);

    RealmPurgeService service = newService(ops);
    assertThatThrownBy(() -> service.drainOnce("nodeA", 0, 60_000L, 100_000L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("batchSize");

    assertThat(store.listClaimable(Long.MAX_VALUE)).containsExactly("R1"); // marker survives
  }

  @Test
  void drainOnceIsolatesAFailingRealmAndDrainsTheRest() throws IOException, SQLException {
    DatasourceOperations ops = newH2("drain-isolate");
    RealmPurgeStore store = new RealmPurgeStore(ops);
    store.enqueue("BAD", 1000L);
    store.enqueue("GOOD", 1000L);

    // A purger that fails only for realm "BAD".
    RealmDataPurger flaky =
        new RealmDataPurger() {
          @Override
          public String storeName() {
            return "flaky";
          }

          @Override
          public long purgeBatch(String realmId, int batchSize) {
            if ("BAD".equals(realmId)) {
              throw new RuntimeException("boom");
            }
            return 0; // "GOOD" is already empty
          }
        };
    RealmPurgeService service = new RealmPurgeService(store, List.of(flaky));

    int drained = service.drainOnce("nodeA", 10, 60_000L, 100_000L);

    assertThat(drained).isEqualTo(1); // healthy realm drained
    assertThat(store.listClaimable(Long.MAX_VALUE)).containsExactly("BAD"); // failed realm retried
  }

  @Test
  void deleteAllAtomicallyEnqueuesRealmForPurgeAndLeavesTimeSeriesData()
      throws IOException, SQLException {
    DatasourceOperations ops = newH2("deleteall-enqueue");
    writeEvents(ops, "R1", 3);

    RealmContext realm = () -> "R1";
    JdbcBasePersistenceImpl impl =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(), ops, RANDOM_SECRETS, "R1", SCHEMA_VERSION);
    impl.deleteAll(new PolarisCallContext(realm, impl));

    assertThat(new RealmPurgeStore(ops).listClaimable(Long.MAX_VALUE))
        .containsExactly("R1"); // queued
    assertThat(countRows(ops, ModelEvent.TABLE_NAME, "R1")).isEqualTo(3); // data left for drainer
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
