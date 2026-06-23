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
package org.apache.polaris.core.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryIdempotencyStoreTest {

  private static final String REALM = "realm-A";
  private static final String OP = "create-table";
  private static final String BH = "binding-A";
  private static final UUID K1 = UUID.randomUUID();
  private static final UUID K2 = UUID.randomUUID();
  private static final UUID K3 = UUID.randomUUID();
  private static final UUID EXPIRED = UUID.randomUUID();
  private static final UUID LIVE = UUID.randomUUID();
  private static final UUID OTHER_REALM_KEY = UUID.randomUUID();

  private InMemoryIdempotencyStore store;

  @BeforeEach
  void setUp() {
    store = new InMemoryIdempotencyStore(REALM);
  }

  @Test
  void recordIfAbsent_firstCallReturnsOwned_secondCallReturnsDuplicate() {
    Instant now = Instant.parse("2025-01-01T00:00:00Z");
    Instant exp = now.plus(Duration.ofMinutes(5));

    IdempotencyStore.RecordResult first = store.recordIfAbsent(K1, OP, BH, 200, null, now, exp);
    assertThat(first.type()).isEqualTo(IdempotencyStore.RecordResultType.OWNED);
    assertThat(first.existing()).isEmpty();

    IdempotencyStore.RecordResult second =
        store.recordIfAbsent(K1, OP, "binding-other", 200, null, now, exp);
    assertThat(second.type()).isEqualTo(IdempotencyStore.RecordResultType.DUPLICATE);
    assertThat(second.existing()).isPresent();
    // First-writer-wins: the stored record reflects the original caller's binding.
    assertThat(second.existing().get().bindingHash()).isEqualTo(BH);
  }

  @Test
  void load_returnsRecordAfterRecord() {
    Instant now = Instant.parse("2025-01-01T00:00:00Z");
    Instant exp = now.plus(Duration.ofMinutes(5));
    store.recordIfAbsent(K2, OP, BH, 201, null, now, exp);

    Optional<IdempotencyRecord> loaded = store.load(K2);
    assertThat(loaded).isPresent();
    IdempotencyRecord r = loaded.get();
    assertThat(r.realmId()).isEqualTo(REALM);
    assertThat(r.idempotencyKey()).isEqualTo(K2);
    assertThat(r.operationType()).isEqualTo(OP);
    assertThat(r.bindingHash()).isEqualTo(BH);
    assertThat(r.httpStatus()).isEqualTo(201);
    assertThat(r.metadataLocation()).isNull();
    assertThat(r.createdAt()).isEqualTo(now);
    assertThat(r.expiresAt()).isEqualTo(exp);
  }

  @Test
  void load_isScopedByRealmInstance() {
    Instant now = Instant.parse("2025-01-01T00:00:00Z");
    Instant exp = now.plus(Duration.ofMinutes(5));
    store.recordIfAbsent(K3, OP, BH, 200, null, now, exp);

    InMemoryIdempotencyStore otherRealm = new InMemoryIdempotencyStore("realm-B");
    assertThat(otherRealm.load(K3)).isEmpty();
    assertThat(store.load(K3)).isPresent();
  }

  @Test
  void purgeExpired_onlyRemovesExpiredRowsInThisRealm() {
    Instant now = Instant.parse("2025-01-01T00:00:00Z");
    Instant past = now.minus(Duration.ofMinutes(1));
    Instant future = now.plus(Duration.ofMinutes(5));

    store.recordIfAbsent(EXPIRED, OP, BH, 200, null, now, past);
    store.recordIfAbsent(LIVE, OP, BH, 200, null, now, future);
    InMemoryIdempotencyStore otherRealm = new InMemoryIdempotencyStore("realm-B");
    otherRealm.recordIfAbsent(OTHER_REALM_KEY, OP, BH, 200, null, now, past);

    int purged = store.purgeExpired(now);
    assertThat(purged).isEqualTo(1);
    assertThat(store.load(EXPIRED)).isEmpty();
    assertThat(store.load(LIVE)).isPresent();
    // Another realm's store is a separate instance and is untouched by this purge.
    assertThat(otherRealm.load(OTHER_REALM_KEY)).isPresent();
  }
}
