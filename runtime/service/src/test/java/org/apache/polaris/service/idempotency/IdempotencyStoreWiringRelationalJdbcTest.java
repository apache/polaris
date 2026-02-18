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
package org.apache.polaris.service.idempotency;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.core.persistence.IdempotencyStore.ReserveResultType;
import org.apache.polaris.persistence.relational.jdbc.idempotency.RelationalJdbcIdempotencyStore;
import org.apache.polaris.service.catalog.Profiles;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(IdempotencyStoreWiringRelationalJdbcTest.RelationalJdbcProfile.class)
class IdempotencyStoreWiringRelationalJdbcTest {

  public static class RelationalJdbcProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> cfg = new HashMap<>(new Profiles.DefaultProfile().getConfigOverrides());
      cfg.put("polaris.realm-context.realms", "test");
      cfg.put("polaris.persistence.type", "relational-jdbc");
      cfg.put("polaris.persistence.auto-bootstrap-types", "relational-jdbc");
      cfg.put("quarkus.datasource.db-kind", "h2");
      cfg.put(
          "quarkus.datasource.jdbc.url",
          "jdbc:h2:mem:idempotency;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE");
      // Prevent unrelated readiness checks from failing the boot in this focused wiring test.
      cfg.put("polaris.readiness.ignore-severe-issues", "true");
      return cfg;
    }
  }

  @Inject IdempotencyStore store;
  @Inject Provider<DataSource> dataSource;

  @Test
  void injectsRelationalJdbcIdempotencyStore() {
    Object delegate = unwrapArcProxy(store);
    assertThat(delegate).isInstanceOf(RelationalJdbcIdempotencyStore.class);
  }

  @Test
  void reserveSmokeTest() {
    ensureIdempotencyTableExists(dataSource.get());

    Instant now = Instant.now();
    Instant exp = now.plus(Duration.ofMinutes(5));

    var r = store.reserve("test", "K1", "op", "catalogs/1/tables/ns.tbl", exp, "A", now);
    assertThat(r.getType()).isEqualTo(ReserveResultType.OWNED);
  }

  private static void ensureIdempotencyTableExists(DataSource ds) {
    try (Connection c = ds.getConnection();
        Statement s = c.createStatement()) {
      s.execute("CREATE SCHEMA IF NOT EXISTS POLARIS_SCHEMA");
      s.execute(
          """
          CREATE TABLE IF NOT EXISTS POLARIS_SCHEMA.idempotency_records (
              realm_id TEXT NOT NULL,
              idempotency_key TEXT NOT NULL,
              operation_type TEXT NOT NULL,
              resource_id TEXT NOT NULL,
              http_status INTEGER,
              error_subtype TEXT,
              response_summary TEXT,
              response_headers TEXT,
              finalized_at TIMESTAMP,
              created_at TIMESTAMP NOT NULL,
              updated_at TIMESTAMP NOT NULL,
              heartbeat_at TIMESTAMP,
              executor_id TEXT,
              expires_at TIMESTAMP,
              PRIMARY KEY (realm_id, idempotency_key)
          )
          """);
    } catch (Exception e) {
      throw new RuntimeException("Failed to ensure idempotency_records table exists", e);
    }
  }

  private static Object unwrapArcProxy(Object bean) {
    try {
      // Arc client proxies implement this method to expose the contextual instance.
      return bean.getClass().getMethod("arc_contextualInstance").invoke(bean);
    } catch (ReflectiveOperationException ignored) {
      return bean;
    }
  }
}
