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

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.apache.polaris.test.commons.PostgresRelationalJdbcLifeCycleManagement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
public class RelationalJdbcIdempotencyStorePostgresIT {

  @Container
  private static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(
          containerSpecHelper("postgres", PostgresRelationalJdbcLifeCycleManagement.class)
              .dockerImageName(null)
              .asCompatibleSubstituteFor("postgres"));

  private static final String REALM = "test-realm";

  private static RelationalJdbcIdempotencyStore store;

  @BeforeAll
  static void setup() throws Exception {
    POSTGRES.start();
    PGSimpleDataSource ds = new PGSimpleDataSource();
    ds.setURL(POSTGRES.getJdbcUrl());
    ds.setUser(POSTGRES.getUsername());
    ds.setPassword(POSTGRES.getPassword());
    DataSource dataSource = ds;

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

    store = new RelationalJdbcIdempotencyStore(ops, REALM);
  }

  @AfterAll
  static void teardown() {
    POSTGRES.stop();
  }

  @Test
  void recordFirstWinnerAndDuplicate() {
    String key = "K1";
    String op = "create-table";
    String rid = "catalogs/1/tables/ns.tbl";
    String principalHash = "principal-hash-A";
    Instant now = Instant.now();
    Instant exp = now.plus(Duration.ofMinutes(5));

    IdempotencyStore.RecordResult r1 =
        store.recordIfAbsent(key, op, rid, principalHash, 200, null, now, exp);
    assertThat(r1.type()).isEqualTo(IdempotencyStore.RecordResultType.OWNED);
    assertThat(r1.existing()).isEmpty();

    IdempotencyStore.RecordResult r2 =
        store.recordIfAbsent(key, op, rid, "principal-hash-B", 200, null, now, exp);
    assertThat(r2.type()).isEqualTo(IdempotencyStore.RecordResultType.DUPLICATE);
    assertThat(r2.existing()).isPresent();
    IdempotencyRecord rec = r2.existing().get();
    assertThat(rec.realmId()).isEqualTo(REALM);
    assertThat(rec.idempotencyKey()).isEqualTo(key);
    assertThat(rec.operationType()).isEqualTo(op);
    assertThat(rec.resourceHash()).isEqualTo(rid);
    assertThat(rec.principalHash()).isEqualTo(principalHash);
    assertThat(rec.httpStatus()).isEqualTo(200);
    assertThat(rec.metadataLocation()).isNull();
  }

  @Test
  void loadReturnsRecordedEntry() {
    String key = "K2";
    String op = "create-table";
    String rid = "catalogs/1/tables/ns.tbl2";
    Instant now = Instant.now();
    Instant exp = now.plus(Duration.ofMinutes(5));

    store.recordIfAbsent(key, op, rid, "ph", 200, null, now, exp);

    Optional<IdempotencyRecord> rec = store.load(key);
    assertThat(rec).isPresent();
    assertThat(rec.get().operationType()).isEqualTo(op);
    assertThat(rec.get().resourceHash()).isEqualTo(rid);
    assertThat(rec.get().principalHash()).isEqualTo("ph");
    assertThat(rec.get().httpStatus()).isEqualTo(200);
  }

  @Test
  void purgeExpired() {
    String key = "K3";
    String op = "drop-table";
    String rid = "catalogs/1/tables/ns.tbl3";
    Instant now = Instant.now();
    Instant expPast = now.minus(Duration.ofMinutes(1));

    store.recordIfAbsent(key, op, rid, "ph", 204, null, now, expPast);
    int purged = store.purgeExpired(Instant.now());
    assertThat(purged).isEqualTo(1);
  }

  @Test
  void duplicateAcrossDifferentPrincipalsReturnsOriginal() {
    String key = "K4";
    Instant now = Instant.now();
    Instant exp = now.plus(Duration.ofMinutes(5));

    IdempotencyStore.RecordResult r1 =
        store.recordIfAbsent(
            key, "create-table", "catalogs/1/tables/ns.tbl4", "principal-A", 200, null, now, exp);
    assertThat(r1.type()).isEqualTo(IdempotencyStore.RecordResultType.OWNED);

    // Cross-principal reuse of the same key on a different operation: the second call must NOT
    // overwrite the original binding. The store returns DUPLICATE with the original (op, resource,
    // principalHash); the handler layer will detect the mismatch and surface 422.
    IdempotencyStore.RecordResult r2 =
        store.recordIfAbsent(
            key, "drop-table", "catalogs/1/tables/ns.tbl5", "principal-B", 204, null, now, exp);
    assertThat(r2.type()).isEqualTo(IdempotencyStore.RecordResultType.DUPLICATE);
    assertThat(r2.existing()).isPresent();
    IdempotencyRecord rec = r2.existing().get();
    assertThat(rec.operationType()).isEqualTo("create-table");
    assertThat(rec.resourceHash()).isEqualTo("catalogs/1/tables/ns.tbl4");
    assertThat(rec.principalHash()).isEqualTo("principal-A");
  }
}
