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

import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;

import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.core.persistence.IdempotencyStore.HeartbeatResult;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.apache.polaris.test.commons.PostgresRelationalJdbcLifeCycleManagement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class RelationalJdbcIdempotencyStorePostgresIT {

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>(
              containerSpecHelper("postgres", PostgresRelationalJdbcLifeCycleManagement.class)
                  .dockerImageName(null)
                  .asCompatibleSubstituteFor("postgres"))
          .withDatabaseName("polaris_db")
          .withUsername("polaris")
          .withPassword("polaris");

  private static DataSource dataSource;
  private static RelationalJdbcIdempotencyStore store;

  @BeforeAll
  static void setup() throws Exception {
    POSTGRES.start();
    PGSimpleDataSource ds = new PGSimpleDataSource();
    ds.setURL(POSTGRES.getJdbcUrl());
    ds.setUser(POSTGRES.getUsername());
    ds.setPassword(POSTGRES.getPassword());
    dataSource = ds;

    // Apply schema
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
        };
    DatasourceOperations ops = new DatasourceOperations(dataSource, cfg);
    try (InputStream is =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("postgres/schema-v4.sql")) {
      if (is == null) {
        throw new IllegalStateException("schema-v4.sql not found on classpath");
      }
      ops.executeScript(is);
    }

    store = new RelationalJdbcIdempotencyStore(dataSource, cfg);
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
    String rid = "tables/ns.tbl";
    Instant now = Instant.now();
    Instant exp = now.plus(Duration.ofMinutes(5));

    IdempotencyStore.ReserveResult r1 = store.reserve(realm, key, op, rid, exp, "A", now);
    assertThat(r1.getType()).isEqualTo(IdempotencyStore.ReserveResultType.OWNED);

    IdempotencyStore.ReserveResult r2 = store.reserve(realm, key, op, rid, exp, "B", now);
    assertThat(r2.getType()).isEqualTo(IdempotencyStore.ReserveResultType.DUPLICATE);
    assertThat(r2.getExisting()).isPresent();
    IdempotencyRecord rec = r2.getExisting().get();
    assertThat(rec.getRealmId()).isEqualTo(realm);
    assertThat(rec.getIdempotencyKey()).isEqualTo(key);
    assertThat(rec.getOperationType()).isEqualTo(op);
    assertThat(rec.getNormalizedResourceId()).isEqualTo(rid);
    assertThat(rec.getHttpStatus()).isNull();
  }

  @Test
  void heartbeatAndFinalize() {
    String realm = "test-realm";
    String key = "K2";
    String op = "commit-table";
    String rid = "tables/ns.tbl2";
    Instant now = Instant.now();
    Instant exp = now.plus(Duration.ofMinutes(5));

    store.reserve(realm, key, op, rid, exp, "A", now);
    HeartbeatResult hb = store.updateHeartbeat(realm, key, "A", now.plusSeconds(1));
    assertThat(hb).isEqualTo(HeartbeatResult.UPDATED);

    boolean fin =
        store.finalizeRecord(
            realm,
            key,
            201,
            null,
            "{\"ok\":true}",
            "{\"Content-Type\":\"application/json\"}",
            now.plusSeconds(2));
    assertThat(fin).isTrue();

    // finalize again should be a no-op
    boolean fin2 =
        store.finalizeRecord(
            realm,
            key,
            201,
            null,
            "{\"ok\":true}",
            "{\"Content-Type\":\"application/json\"}",
            now.plusSeconds(3));
    assertThat(fin2).isFalse();

    Optional<IdempotencyRecord> rec = store.load(realm, key);
    assertThat(rec).isPresent();
    assertThat(rec.get().isFinalized()).isTrue();
    assertThat(rec.get().getHttpStatus()).isEqualTo(201);
  }

  @Test
  void purgeExpired() {
    String realm = "test-realm";
    String key = "K3";
    String op = "drop-table";
    String rid = "tables/ns.tbl3";
    Instant now = Instant.now();
    Instant expPast = now.minus(Duration.ofMinutes(1));

    store.reserve(realm, key, op, rid, expPast, "A", now);
    int purged = store.purgeExpired(realm, Instant.now());
    assertThat(purged).isEqualTo(1);
  }

  @Test
  void duplicateReturnsExistingBindingForMismatch() {
    String realm = "test-realm";
    String key = "K4";
    String op1 = "commit-table";
    String rid1 = "tables/ns.tbl4";
    String op2 = "drop-table"; // different binding
    String rid2 = "tables/ns.tbl4"; // same resource, different op
    Instant now = Instant.now();
    Instant exp = now.plus(Duration.ofMinutes(5));

    IdempotencyStore.ReserveResult r1 = store.reserve(realm, key, op1, rid1, exp, "A", now);
    assertThat(r1.getType()).isEqualTo(IdempotencyStore.ReserveResultType.OWNED);

    // Second reserve with different op/resource should *not* overwrite the original binding.
    // The store must return DUPLICATE with the *original* (op1, rid1); the HTTP layer
    // (IdempotencyFilter)
    // will detect the mismatch and return 422.
    IdempotencyStore.ReserveResult r2 = store.reserve(realm, key, op2, rid2, exp, "B", now);
    assertThat(r2.getType()).isEqualTo(IdempotencyStore.ReserveResultType.DUPLICATE);
    assertThat(r2.getExisting()).isPresent();
    IdempotencyRecord rec = r2.getExisting().get();
    assertThat(rec.getOperationType()).isEqualTo(op1);
    assertThat(rec.getNormalizedResourceId()).isEqualTo(rid1);
  }
}
