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

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.service.catalog.Profiles;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(IdempotencyFilterTest.IdempotencyEnabledProfile.class)
@TestHTTPEndpoint(IdempotencyTestResource.class)
class IdempotencyFilterTest {

  private static String uuidV7() {
    // Pseudo UUIDv7 generator for tests (sets version=7, variant=IETF).
    long msb = ThreadLocalRandom.current().nextLong();
    long lsb = ThreadLocalRandom.current().nextLong();
    msb = (msb & 0xffffffffffff0ffFL) | 0x0000000000007000L; // version 7
    lsb = (lsb & 0x3fffffffffffffffL) | 0x8000000000000000L; // IETF variant
    return new UUID(msb, lsb).toString();
  }

  public static class IdempotencyEnabledProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> cfg = new HashMap<>(new Profiles.DefaultProfile().getConfigOverrides());
      cfg.put("polaris.realm-context.realms", "test");
      cfg.put("polaris.persistence.type", "in-memory");
      cfg.put("polaris.idempotency.enabled", "true");
      cfg.put("polaris.idempotency.scopes[0].method", "POST");
      cfg.put("polaris.idempotency.scopes[0].path-prefix", "test/idempotency");
      cfg.put("polaris.idempotency.scopes[0].operation-type", "test-write");
      cfg.put("polaris.idempotency.response-header-allowlist", "Content-Type,X-Test");
      cfg.put("polaris.idempotency.in-progress-wait-seconds", "PT5S");
      cfg.put("polaris.idempotency.heartbeat-enabled", "true");
      cfg.put("polaris.idempotency.heartbeat-interval-seconds", "PT1S");
      cfg.put("polaris.idempotency.purge-enabled", "true");
      cfg.put("polaris.idempotency.purge-interval-seconds", "PT1S");
      cfg.put("polaris.readiness.ignore-severe-issues", "true");
      return cfg;
    }
  }

  @Inject IdempotencyStore store;

  @BeforeEach
  void resetCounter() {
    IdempotencyTestResource.reset();
    IdempotencyOutOfScopeTestResource.reset();
  }

  @Test
  void replayReturnsSameBodyAndDoesNotReexecute() {
    String key1 = uuidV7();
    String key2 = uuidV7();

    var resp1 =
        given()
            .contentType(ContentType.TEXT)
            .header("Idempotency-Key", key1)
            .body("hello")
            .when()
            .post()
            .then()
            .statusCode(200)
            .extract()
            .response();
    assertThat(resp1.getHeader("X-Idempotency-Replayed")).isNull();
    assertThat(resp1.getHeader("X-Test")).isEqualTo("v1");
    String r1 = resp1.asString();
    assertThat(IdempotencyTestResource.count()).isEqualTo(1);

    var resp2 =
        given()
            .contentType(ContentType.TEXT)
            .header("Idempotency-Key", key1)
            .body("hello")
            .when()
            .post()
            .then()
            .statusCode(200)
            .extract()
            .response();
    assertThat(resp2.getHeader("X-Idempotency-Replayed")).isEqualTo("true");
    assertThat(resp2.getHeader("X-Test")).isEqualTo("v1");
    String r2 = resp2.asString();
    assertThat(r2).isEqualTo(r1);
    assertThat(IdempotencyTestResource.count()).isEqualTo(1);

    String r3 =
        given()
            .contentType(ContentType.TEXT)
            .header("Idempotency-Key", key2)
            .body("hello")
            .when()
            .post()
            .then()
            .statusCode(200)
            .extract()
            .asString();
    assertThat(r3).isNotEqualTo(r1);
    assertThat(IdempotencyTestResource.count()).isEqualTo(2);
  }

  @Test
  void sameKeyDifferentPathReturns422() {
    String key = uuidV7();

    given()
        .contentType(ContentType.TEXT)
        .header("Idempotency-Key", key)
        .body("hello")
        .when()
        .post()
        .then()
        .statusCode(200);
    assertThat(IdempotencyTestResource.count()).isEqualTo(1);

    given()
        .contentType(ContentType.TEXT)
        .header("Idempotency-Key", key)
        .body("hello")
        .when()
        .post("/alt")
        .then()
        .statusCode(422);
    assertThat(IdempotencyTestResource.count()).isEqualTo(1);
  }

  @Test
  void heartbeatAndPurgeMaintenance() throws Exception {
    String key = uuidV7();

    // Slow request should allow at least one heartbeat tick.
    given()
        .contentType(ContentType.TEXT)
        .header("Idempotency-Key", key)
        .body("hello")
        .when()
        .post("/slow")
        .then()
        .statusCode(200);

    var rec = store.load("test", key).orElseThrow();
    assertThat(rec.heartbeatAt()).isNotNull();

    // Create an already-expired record and wait for the purge timer to remove it.
    String expiredKey = uuidV7();
    store.reserve(
        "test",
        expiredKey,
        "post",
        "test/idempotency",
        Instant.now().minusSeconds(5),
        "http",
        Instant.now());

    long deadline = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < deadline) {
      if (store.load("test", expiredKey).isEmpty()) {
        return;
      }
      Thread.sleep(100);
    }
    assertThat(store.load("test", expiredKey)).isEmpty();
  }

  @Test
  void inProgressDuplicateWaitsAndReplays() throws Exception {
    String key = uuidV7();

    var executor = Executors.newFixedThreadPool(2);
    try {
      var f1 =
          CompletableFuture.supplyAsync(
              () ->
                  given()
                      .contentType(ContentType.TEXT)
                      .header("Idempotency-Key", key)
                      .body("hello")
                      .when()
                      .post("/slow")
                      .then()
                      .statusCode(200)
                      .extract()
                      .response(),
              executor);

      // Wait until the first request reserved the key (i.e., the record exists and is in progress).
      long deadline = System.currentTimeMillis() + 2000;
      while (System.currentTimeMillis() < deadline) {
        var rec = store.load("test", key);
        if (rec.isPresent() && !rec.get().isFinalized()) {
          break;
        }
        Thread.sleep(10);
      }

      var resp2 =
          given()
              .contentType(ContentType.TEXT)
              .header("Idempotency-Key", key)
              .body("hello")
              .when()
              .post("/slow")
              .then()
              .statusCode(200)
              .extract()
              .response();
      assertThat(resp2.getHeader("X-Idempotency-Replayed")).isEqualTo("true");
      assertThat(resp2.getHeader("X-Test")).isEqualTo("slow");

      f1.get(10, TimeUnit.SECONDS);

      // Only one handler execution should have happened.
      assertThat(IdempotencyTestResource.count()).isEqualTo(1);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void outOfScopeEndpointDoesNotReplayEvenWithSameKey() {
    String key = uuidV7();

    var resp1 =
        given()
            // Override @TestHTTPEndpoint base path so we can call a different resource.
            .basePath("")
            .contentType(ContentType.TEXT)
            .header("Idempotency-Key", key)
            .body("hello")
            .when()
            .post("/test/no-idempotency")
            .then()
            .statusCode(200)
            .extract()
            .response();
    assertThat(resp1.getHeader("X-Idempotency-Replayed")).isNull();
    assertThat(IdempotencyOutOfScopeTestResource.count()).isEqualTo(1);

    var resp2 =
        given()
            .basePath("")
            .contentType(ContentType.TEXT)
            .header("Idempotency-Key", key)
            .body("hello")
            .when()
            .post("/test/no-idempotency")
            .then()
            .statusCode(200)
            .extract()
            .response();
    assertThat(resp2.getHeader("X-Idempotency-Replayed")).isNull();
    assertThat(IdempotencyOutOfScopeTestResource.count()).isEqualTo(2);
  }

  @Test
  void invalidKeyReturns400AndDoesNotExecute() {
    given()
        .contentType(ContentType.TEXT)
        .header("Idempotency-Key", "not-a-uuid")
        .body("hello")
        .when()
        .post()
        .then()
        .statusCode(400);
    assertThat(IdempotencyTestResource.count()).isEqualTo(0);
  }

  @Test
  void uuidV4KeyReturns400() {
    given()
        .contentType(ContentType.TEXT)
        .header("Idempotency-Key", UUID.randomUUID().toString())
        .body("hello")
        .when()
        .post()
        .then()
        .statusCode(400);
    assertThat(IdempotencyTestResource.count()).isEqualTo(0);
  }
}
