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
package org.apache.polaris.persistence.relational.jdbc;

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.apache.polaris.core.persistence.IdempotencyPersistence;
import org.apache.polaris.core.persistence.IdempotencyPersistence.HeartbeatResult;
import org.apache.polaris.test.commons.PostgresRelationalJdbcLifeCycleManagement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Postgres-backed integration tests for the {@link IdempotencyPersistence} methods on {@link
 * JdbcBasePersistenceImpl}. The same scenarios are covered against the in-memory implementation in
 * {@code IdempotencyPersistenceSpiTest}.
 */
@Testcontainers
public class JdbcBasePersistenceImplIdempotencyPostgresIT {

  @Container
  private static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(
          containerSpecHelper("postgres", PostgresRelationalJdbcLifeCycleManagement.class)
              .dockerImageName(null)
              .asCompatibleSubstituteFor("postgres"));

  private static DataSource dataSource;
  private static IdempotencyPersistence store;

  @BeforeAll
  static void setup() throws Exception {
    POSTGRES.start();
    PGSimpleDataSource ds = new PGSimpleDataSource();
    ds.setURL(POSTGRES.getJdbcUrl());
    ds.setUser(POSTGRES.getUsername());
    ds.setPassword(POSTGRES.getPassword());
    dataSource = ds;

    RelationalJdbcConfiguration cfg =
        new RelationalJdbcConfiguration() {
          @Override
          public Optional<Integer> maxRetries() {
            return Optional.of(3);
          }

          @Override
          public Optional<Long> maxDurationInMs() {
            return Optional.of(5000L);
          }

          @Override
          public Optional<Long> initialDelayInMs() {
            return Optional.of(100L);
          }

          @Override
          public Optional<String> databaseType() {
            return Optional.empty();
          }
        };
    DatasourceOperations ops = new DatasourceOperations(dataSource, cfg);
    try (InputStream is =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("postgres/schema-v5.sql")) {
      if (is == null) {
        throw new IllegalStateException("schema-v5.sql not found on classpath");
      }
      ops.executeScript(is);
    }

    // The idempotency methods on JdbcBasePersistenceImpl only use datasourceOperations and
    // realmId-as-parameter; secrets generator / storage provider / schemaVersion are unused on
    // these paths, so we can safely pass nulls / 0 in this targeted IT.
    store =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(), ops, null, null, "test-realm", 0);
  }

  @AfterAll
  static void teardown() {
    POSTGRES.stop();
  }

  @Test
  void reserveSingleWinnerAndDuplicate() {
    String realm = "test-realm";
    String key = "K1";
    String op = "commit-table";
    String rid = "catalogs/1/tables/ns.tbl";
    Instant now = Instant.now();
    Instant exp = now.plus(Duration.ofMinutes(5));

    String ph = "principal-hash-A";
    IdempotencyPersistence.ReserveResult r1 = store.reserve(realm, key, op, rid, ph, exp, "A", now);
    assertThat(r1.type()).isEqualTo(IdempotencyPersistence.ReserveResultType.OWNED);

    IdempotencyPersistence.ReserveResult r2 = store.reserve(realm, key, op, rid, ph, exp, "B", now);
    assertThat(r2.type()).isEqualTo(IdempotencyPersistence.ReserveResultType.DUPLICATE);
    assertThat(r2.existing()).isPresent();
    IdempotencyRecord rec = r2.existing().get();
    assertThat(rec.realmId()).isEqualTo(realm);
    assertThat(rec.idempotencyKey()).isEqualTo(key);
    assertThat(rec.operationType()).isEqualTo(op);
    assertThat(rec.normalizedResourceId()).isEqualTo(rid);
    assertThat(rec.principalHash()).isEqualTo(ph);
    assertThat(rec.httpStatus()).isNull();
  }

  @Test
  void heartbeatAndFinalize() {
    String realm = "test-realm";
    String key = "K2";
    String op = "create-table";
    String rid = "catalogs/1/tables/ns.tbl2";
    Instant now = Instant.now();
    Instant exp = now.plus(Duration.ofMinutes(5));

    store.reserve(realm, key, op, rid, "principal-hash-A", exp, "A", now);
    HeartbeatResult hb = store.updateHeartbeat(realm, key, "A", now.plusSeconds(1));
    assertThat(hb).isEqualTo(HeartbeatResult.UPDATED);

    boolean fin = store.finalizeRecord(realm, key, "A", 200, null, null, now.plusSeconds(2));
    assertThat(fin).isTrue();

    // finalize again should be a no-op
    boolean fin2 = store.finalizeRecord(realm, key, "A", 200, null, null, now.plusSeconds(3));
    assertThat(fin2).isFalse();

    Optional<IdempotencyRecord> rec = store.loadIdempotencyRecord(realm, key);
    assertThat(rec).isPresent();
    assertThat(rec.get().isFinalized()).isTrue();
    assertThat(rec.get().httpStatus()).isEqualTo(200);
  }

  @Test
  void purgeExpired() {
    String realm = "test-realm";
    String key = "K3";
    String op = "drop-table";
    String rid = "catalogs/1/tables/ns.tbl3";
    Instant now = Instant.now();
    Instant expPast = now.minus(Duration.ofMinutes(1));

    store.reserve(realm, key, op, rid, "principal-hash-A", expPast, "A", now);
    int purged = store.purgeExpired(realm, Instant.now());
    assertThat(purged).isEqualTo(1);
  }

  @Test
  void duplicateReturnsExistingBindingForMismatch() {
    String realm = "test-realm";
    String key = "K4";
    String op1 = "create-table";
    String rid1 = "catalogs/1/tables/ns.tbl4";
    String op2 = "drop-table"; // different binding
    String rid2 = "catalogs/1/tables/ns.tbl4"; // same resource, different op
    Instant now = Instant.now();
    Instant exp = now.plus(Duration.ofMinutes(5));

    IdempotencyPersistence.ReserveResult r1 =
        store.reserve(realm, key, op1, rid1, "principal-hash-A", exp, "A", now);
    assertThat(r1.type()).isEqualTo(IdempotencyPersistence.ReserveResultType.OWNED);

    // Second reserve with different op/resource should *not* overwrite the original binding.
    // The store must return DUPLICATE with the *original* (op1, rid1); the handler will
    // detect the mismatch via principalHash/normalizedResourceId comparison and return 422.
    IdempotencyPersistence.ReserveResult r2 =
        store.reserve(realm, key, op2, rid2, "principal-hash-B", exp, "B", now);
    assertThat(r2.type()).isEqualTo(IdempotencyPersistence.ReserveResultType.DUPLICATE);
    assertThat(r2.existing()).isPresent();
    IdempotencyRecord rec = r2.existing().get();
    assertThat(rec.operationType()).isEqualTo(op1);
    assertThat(rec.normalizedResourceId()).isEqualTo(rid1);
    assertThat(rec.principalHash()).isEqualTo("principal-hash-A");
  }

  @Test
  void duplicateReturnsExistingBindingForCrossPrincipal() {
    // Same (realm, key) + same (operationType, normalizedResourceId), but a different caller.
    // The persistence layer must return DUPLICATE with the *original* principalHash so the
    // handler can reject the cross-principal replay with 422. This is the core security property
    // of the v5 schema change (principal_hash NOT NULL) and is verified at the persistence layer.
    String realm = "test-realm";
    String key = "K4cp";
    String op = "create-table";
    String rid = "catalogs/1/tables/ns.tbl4cp";
    Instant now = Instant.now();
    Instant exp = now.plus(Duration.ofMinutes(5));

    IdempotencyPersistence.ReserveResult r1 =
        store.reserve(realm, key, op, rid, "principal-hash-A", exp, "A", now);
    assertThat(r1.type()).isEqualTo(IdempotencyPersistence.ReserveResultType.OWNED);

    IdempotencyPersistence.ReserveResult r2 =
        store.reserve(realm, key, op, rid, "principal-hash-B", exp, "B", now);
    assertThat(r2.type()).isEqualTo(IdempotencyPersistence.ReserveResultType.DUPLICATE);
    assertThat(r2.existing()).isPresent();
    IdempotencyRecord rec = r2.existing().get();
    assertThat(rec.operationType()).isEqualTo(op);
    assertThat(rec.normalizedResourceId()).isEqualTo(rid);
    assertThat(rec.principalHash()).isEqualTo("principal-hash-A");
  }

  @Test
  void cancelInProgressReservation() {
    String realm = "test-realm";
    String key = "K5";
    String op = "create-table";
    String rid = "catalogs/1/tables/ns.tbl5";
    Instant now = Instant.now();
    Instant exp = now.plus(Duration.ofMinutes(5));

    store.reserve(realm, key, op, rid, "principal-hash-A", exp, "A", now);
    boolean cancelled = store.cancelInProgressReservation(realm, key, "A");
    assertThat(cancelled).isTrue();
    assertThat(store.loadIdempotencyRecord(realm, key)).isEmpty();

    // After cancel a different caller may acquire the same key.
    IdempotencyPersistence.ReserveResult r3 =
        store.reserve(realm, key, op, rid, "principal-hash-B", exp, "B", now);
    assertThat(r3.type()).isEqualTo(IdempotencyPersistence.ReserveResultType.OWNED);
  }

  @Test
  void cancelDoesNotRemoveFinalizedRecord() {
    String realm = "test-realm";
    String key = "K6";
    String op = "create-table";
    String rid = "catalogs/1/tables/ns.tbl6";
    Instant now = Instant.now();
    Instant exp = now.plus(Duration.ofMinutes(5));

    store.reserve(realm, key, op, rid, "principal-hash-A", exp, "A", now);
    store.finalizeRecord(realm, key, "A", 200, null, null, now.plusSeconds(1));

    boolean cancelled = store.cancelInProgressReservation(realm, key, "A");
    assertThat(cancelled).isFalse();
    assertThat(store.loadIdempotencyRecord(realm, key)).isPresent();
  }
}
