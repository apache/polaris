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
package org.apache.polaris.core.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.junit.jupiter.api.Test;

/**
 * SPI conformance tests for {@link IdempotencyStore} implementations against the in-memory
 * implementation. The same scenarios are covered against the JDBC implementation in {@code
 * RelationalJdbcIdempotencyStorePostgresIT}.
 */
class IdempotencyStoreSpiTest {

  private static final String REALM = "realm-1";
  private static final String OP = "create-table";
  private static final String RESOURCE = "ns:tbl";
  private static final String PRINCIPAL_HASH = "principal-hash-A";
  private static final String OTHER_PRINCIPAL_HASH = "principal-hash-B";

  private final InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();

  @Test
  void firstReserveOwnsAndDuplicateReturnsRecord() {
    Instant now = Instant.parse("2026-04-01T00:00:00Z");
    Instant expires = now.plus(5, ChronoUnit.MINUTES);

    IdempotencyStore.ReserveResult first =
        store.reserve(REALM, "k1", OP, RESOURCE, PRINCIPAL_HASH, expires, "exec-1", now);
    assertThat(first.type()).isEqualTo(IdempotencyStore.ReserveResultType.OWNED);
    assertThat(first.existing()).isEmpty();

    IdempotencyStore.ReserveResult second =
        store.reserve(REALM, "k1", OP, RESOURCE, PRINCIPAL_HASH, expires, "exec-2", now);
    assertThat(second.type()).isEqualTo(IdempotencyStore.ReserveResultType.DUPLICATE);
    assertThat(second.existing()).isPresent();
    IdempotencyRecord existing = second.existing().get();
    assertThat(existing.executorId()).isEqualTo("exec-1");
    assertThat(existing.principalHash()).isEqualTo(PRINCIPAL_HASH);
    assertThat(existing.isFinalized()).isFalse();
  }

  @Test
  void duplicatePreservesOriginalPrincipalHashEvenForDifferentCaller() {
    Instant now = Instant.parse("2026-04-01T00:00:00Z");
    Instant expires = now.plus(5, ChronoUnit.MINUTES);

    store.reserve(REALM, "k2", OP, RESOURCE, PRINCIPAL_HASH, expires, "exec-1", now);

    IdempotencyStore.ReserveResult second =
        store.reserve(REALM, "k2", OP, RESOURCE, OTHER_PRINCIPAL_HASH, expires, "exec-2", now);
    assertThat(second.type()).isEqualTo(IdempotencyStore.ReserveResultType.DUPLICATE);
    assertThat(second.existing()).isPresent();
    // The store does not enforce identity itself; it persists the original principal hash so the
    // handler can compare and reject. This guarantees no cross-principal cache hits.
    assertThat(second.existing().get().principalHash()).isEqualTo(PRINCIPAL_HASH);
  }

  @Test
  void cancelInProgressByOwnerReleasesKey() {
    Instant now = Instant.parse("2026-04-01T00:00:00Z");
    Instant expires = now.plus(5, ChronoUnit.MINUTES);

    store.reserve(REALM, "k3", OP, RESOURCE, PRINCIPAL_HASH, expires, "exec-1", now);
    boolean cancelled = store.cancelInProgressReservation(REALM, "k3", "exec-1");
    assertThat(cancelled).isTrue();

    // After cancel, key is free again.
    IdempotencyStore.ReserveResult retry =
        store.reserve(REALM, "k3", OP, RESOURCE, PRINCIPAL_HASH, expires, "exec-1", now);
    assertThat(retry.type()).isEqualTo(IdempotencyStore.ReserveResultType.OWNED);
  }

  @Test
  void cancelByNonOwnerIsRejected() {
    Instant now = Instant.parse("2026-04-01T00:00:00Z");
    Instant expires = now.plus(5, ChronoUnit.MINUTES);

    store.reserve(REALM, "k4", OP, RESOURCE, PRINCIPAL_HASH, expires, "exec-1", now);
    assertThat(store.cancelInProgressReservation(REALM, "k4", "exec-other")).isFalse();
    // Reservation still owned by exec-1.
    Optional<IdempotencyRecord> rec = store.load(REALM, "k4");
    assertThat(rec).isPresent();
    assertThat(rec.get().executorId()).isEqualTo("exec-1");
  }

  @Test
  void cancelAfterFinalizeIsRejected() {
    Instant now = Instant.parse("2026-04-01T00:00:00Z");
    Instant expires = now.plus(5, ChronoUnit.MINUTES);

    store.reserve(REALM, "k5", OP, RESOURCE, PRINCIPAL_HASH, expires, "exec-1", now);
    boolean finalized =
        store.finalizeRecord(REALM, "k5", "exec-1", 200, null, null, now.plusSeconds(1));
    assertThat(finalized).isTrue();

    assertThat(store.cancelInProgressReservation(REALM, "k5", "exec-1")).isFalse();
  }

  @Test
  void finalizeRecordPersistsHttpStatusAndIsIdempotent() {
    Instant now = Instant.parse("2026-04-01T00:00:00Z");
    Instant expires = now.plus(5, ChronoUnit.MINUTES);

    store.reserve(REALM, "k6", OP, RESOURCE, PRINCIPAL_HASH, expires, "exec-1", now);
    assertThat(store.finalizeRecord(REALM, "k6", "exec-1", 200, null, null, now.plusSeconds(1)))
        .isTrue();
    // Re-finalize is rejected.
    assertThat(store.finalizeRecord(REALM, "k6", "exec-1", 200, null, null, now.plusSeconds(2)))
        .isFalse();

    Optional<IdempotencyRecord> loaded = store.load(REALM, "k6");
    assertThat(loaded).isPresent();
    assertThat(loaded.get().httpStatus()).isEqualTo(200);
    assertThat(loaded.get().isFinalized()).isTrue();
    // Credential-bearing mutations never store a response summary.
    assertThat(loaded.get().responseSummary()).isNull();
  }

  @Test
  void heartbeatTransitionsAndOwnership() {
    Instant now = Instant.parse("2026-04-01T00:00:00Z");
    Instant expires = now.plus(5, ChronoUnit.MINUTES);

    assertThat(store.updateHeartbeat(REALM, "missing", "exec-1", now))
        .isEqualTo(IdempotencyStore.HeartbeatResult.NOT_FOUND);

    store.reserve(REALM, "k7", OP, RESOURCE, PRINCIPAL_HASH, expires, "exec-1", now);
    assertThat(store.updateHeartbeat(REALM, "k7", "exec-1", now.plusSeconds(1)))
        .isEqualTo(IdempotencyStore.HeartbeatResult.UPDATED);
    assertThat(store.updateHeartbeat(REALM, "k7", "exec-other", now.plusSeconds(2)))
        .isEqualTo(IdempotencyStore.HeartbeatResult.LOST_OWNERSHIP);

    store.finalizeRecord(REALM, "k7", "exec-1", 200, null, null, now.plusSeconds(3));
    assertThat(store.updateHeartbeat(REALM, "k7", "exec-1", now.plusSeconds(4)))
        .isEqualTo(IdempotencyStore.HeartbeatResult.FINALIZED);
  }

  @Test
  void purgeExpiredRemovesRecordsBeforeCutoff() {
    Instant t0 = Instant.parse("2026-04-01T00:00:00Z");

    store.reserve(REALM, "expired", OP, RESOURCE, PRINCIPAL_HASH, t0.plusSeconds(10), "exec", t0);
    store.reserve(REALM, "alive", OP, RESOURCE, PRINCIPAL_HASH, t0.plusSeconds(120), "exec", t0);

    int purged = store.purgeExpired(REALM, t0.plusSeconds(60));
    assertThat(purged).isEqualTo(1);
    assertThat(store.load(REALM, "expired")).isEmpty();
    assertThat(store.load(REALM, "alive")).isPresent();
  }
}
