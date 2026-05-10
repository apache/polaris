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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.apache.polaris.core.persistence.IdempotencyPersistence;
import org.apache.polaris.core.persistence.InMemoryIdempotencyPersistence;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link IdempotencyHandlerSupport}. These tests exercise the orchestration logic
 * the catalog handlers depend on (key validation, principal/resource binding, OWNED vs DUPLICATE
 * dispatch, in-progress wait, owner-cancel) without spinning up CDI.
 */
class IdempotencyHandlerSupportTest {

  private static final String REALM = "realm-1";
  private static final String OPERATION = "create-table";
  private static final String RESOURCE = "ns:tbl:none";
  private static final PolarisPrincipal PRINCIPAL_A =
      PolarisPrincipal.of("principal-A", Map.of(), Set.of());
  private static final PolarisPrincipal PRINCIPAL_B =
      PolarisPrincipal.of("principal-B", Map.of(), Set.of());

  private InMemoryIdempotencyPersistence persistence;
  private AtomicReference<Instant> nowRef;
  private IdempotencyHandlerSupport support;
  private Clock testClock;
  private Function<String, IdempotencyPersistence> persistenceLookup;

  @BeforeEach
  void setUp() {
    persistence = new InMemoryIdempotencyPersistence();
    nowRef = new AtomicReference<>(Instant.parse("2026-04-01T00:00:00Z"));

    testClock =
        new Clock() {
          @Override
          public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
          }

          @Override
          public Clock withZone(java.time.ZoneId zone) {
            return this;
          }

          @Override
          public Instant instant() {
            return nowRef.get();
          }
        };

    persistenceLookup = realmId -> persistence;

    support = IdempotencyHandlerSupport.forTesting(testConfig(true), persistenceLookup, testClock);
  }

  @Test
  void validatedKeyAcceptsUuidV7AndRejectsOthers() {
    String v7 = uuidV7();
    assertThat(support.validatedKey(v7)).contains(v7.toLowerCase(Locale.ROOT));

    // Empty / null / blank => empty Optional, never an exception.
    assertThat(support.validatedKey((String) null)).isEmpty();
    assertThat(support.validatedKey("   ")).isEmpty();

    // Non-UUID and non-v7 UUIDs are rejected.
    assertThatThrownBy(() -> support.validatedKey("not-a-uuid"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> support.validatedKey(UUID.randomUUID().toString()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void validatedKeyReturnsEmptyWhenIdempotencyDisabled() {
    support = IdempotencyHandlerSupport.forTesting(testConfig(false), persistenceLookup, testClock);
    assertThat(support.validatedKey(uuidV7())).isEmpty();
  }

  @Test
  void principalHashAndResourceHashAreDeterministic() {
    String h1 = support.principalHash(PRINCIPAL_A, REALM);
    String h2 = support.principalHash(PRINCIPAL_A, REALM);
    String hOther = support.principalHash(PRINCIPAL_B, REALM);
    assertThat(h1).isEqualTo(h2).isNotEqualTo(hOther);

    assertThat(support.resourceHash("a")).isEqualTo(support.resourceHash("a"));
    assertThat(support.resourceHash("a")).isNotEqualTo(support.resourceHash("b"));
  }

  @Test
  void principalHashIncludesRolesAndIgnoresPropertiesAndIsOrderIndependent() {
    PolarisPrincipal aliceReader = PolarisPrincipal.of("alice", Map.of(), Set.of("reader"));
    PolarisPrincipal aliceAdmin = PolarisPrincipal.of("alice", Map.of(), Set.of("admin"));
    assertThat(support.principalHash(aliceReader, REALM))
        .isNotEqualTo(support.principalHash(aliceAdmin, REALM));

    // Properties are admin-mutable and not part of authentication context, so they must NOT
    // affect the hash; otherwise unrelated property edits during the idempotency TTL would
    // invalidate retries from the same client.
    PolarisPrincipal aliceWithProps =
        PolarisPrincipal.of("alice", Map.of("dept", "eng"), Set.of("reader"));
    assertThat(support.principalHash(aliceReader, REALM))
        .isEqualTo(support.principalHash(aliceWithProps, REALM));

    PolarisPrincipal rolesAB = PolarisPrincipal.of("alice", Map.of(), Set.of("a", "b"));
    PolarisPrincipal rolesBA = PolarisPrincipal.of("alice", Map.of(), Set.of("b", "a"));
    assertThat(support.principalHash(rolesAB, REALM))
        .isEqualTo(support.principalHash(rolesBA, REALM));
  }

  @Test
  void firstCallIsOwnedAndFinalizeRecordsHttpStatus() {
    String key = uuidV7();
    String pHash = support.principalHash(PRINCIPAL_A, REALM);

    IdempotencyHandlerSupport.Outcome outcome =
        support.reserveOrWait(REALM, key, OPERATION, RESOURCE, pHash);
    assertThat(outcome).isInstanceOf(IdempotencyHandlerSupport.Outcome.Owned.class);
    String executorId = ((IdempotencyHandlerSupport.Outcome.Owned) outcome).executorId();

    support.finalizeOwned(REALM, key, executorId, 200, null, null);

    IdempotencyRecord stored = persistence.loadIdempotencyRecord(REALM, key).orElseThrow();
    assertThat(stored.httpStatus()).isEqualTo(200);
    assertThat(stored.principalHash()).isEqualTo(pHash);
    assertThat(stored.responseSummary()).isNull();
  }

  @Test
  void duplicateForSameCallerAndBindingReturnsDuplicate() {
    String key = uuidV7();
    String pHash = support.principalHash(PRINCIPAL_A, REALM);

    var first = support.reserveOrWait(REALM, key, OPERATION, RESOURCE, pHash);
    String executorId = ((IdempotencyHandlerSupport.Outcome.Owned) first).executorId();
    support.finalizeOwned(REALM, key, executorId, 200, null, null);

    nowRef.set(nowRef.get().plusSeconds(1));
    var second = support.reserveOrWait(REALM, key, OPERATION, RESOURCE, pHash);
    assertThat(second).isInstanceOf(IdempotencyHandlerSupport.Outcome.Duplicate.class);
  }

  @Test
  void crossPrincipalReuseRejectedAsConflict() {
    String key = uuidV7();
    String pHashA = support.principalHash(PRINCIPAL_A, REALM);
    String pHashB = support.principalHash(PRINCIPAL_B, REALM);

    var first = support.reserveOrWait(REALM, key, OPERATION, RESOURCE, pHashA);
    String executorId = ((IdempotencyHandlerSupport.Outcome.Owned) first).executorId();
    support.finalizeOwned(REALM, key, executorId, 200, null, null);

    assertThatThrownBy(() -> support.reserveOrWait(REALM, key, OPERATION, RESOURCE, pHashB))
        .isInstanceOf(IdempotencyHandlerSupport.ConflictException.class)
        .hasMessageContaining("different caller");
  }

  @Test
  void crossResourceReuseRejectedAsConflict() {
    String key = uuidV7();
    String pHash = support.principalHash(PRINCIPAL_A, REALM);

    var first = support.reserveOrWait(REALM, key, OPERATION, RESOURCE, pHash);
    String executorId = ((IdempotencyHandlerSupport.Outcome.Owned) first).executorId();
    support.finalizeOwned(REALM, key, executorId, 200, null, null);

    assertThatThrownBy(
            () -> support.reserveOrWait(REALM, key, OPERATION, "ns:other-table:none", pHash))
        .isInstanceOf(IdempotencyHandlerSupport.ConflictException.class)
        .hasMessageContaining("different operation/resource");
  }

  @Test
  void cancelOwnedReleasesKeyForRetry() {
    String key = uuidV7();
    String pHash = support.principalHash(PRINCIPAL_A, REALM);

    var first = support.reserveOrWait(REALM, key, OPERATION, RESOURCE, pHash);
    String executorId = ((IdempotencyHandlerSupport.Outcome.Owned) first).executorId();
    support.cancelOwned(REALM, key, executorId);

    var retry = support.reserveOrWait(REALM, key, OPERATION, RESOURCE, pHash);
    assertThat(retry).isInstanceOf(IdempotencyHandlerSupport.Outcome.Owned.class);
  }

  @Test
  void inProgressWaitTimesOutWhenLeaseStillValid() {
    // The polling loop measures elapsed time via Clock#instant(), so this scenario must use a
    // real clock; with a frozen test clock the deadline check never trips and the loop never
    // exits. Configure a tiny wait so the test stays fast, but a long lease so we hit the wait
    // timeout (not the stale-lease branch).
    Clock realClock = Clock.systemUTC();
    support =
        IdempotencyHandlerSupport.forTesting(
            testConfig(true, Duration.ofMillis(50), Duration.ofMillis(10), Duration.ofMinutes(5)),
            persistenceLookup,
            realClock);

    String key = uuidV7();
    String pHash = support.principalHash(PRINCIPAL_A, REALM);

    Instant now = realClock.instant();
    persistence.reserve(
        REALM, key, OPERATION, RESOURCE, pHash, now.plusSeconds(300), "other-executor", now);

    assertThatThrownBy(() -> support.reserveOrWait(REALM, key, OPERATION, RESOURCE, pHash))
        .isInstanceOf(IdempotencyHandlerSupport.InProgressTimeoutException.class);
  }

  private IdempotencyConfiguration testConfig(boolean enabled) {
    return testConfig(
        enabled, Duration.ofSeconds(2), Duration.ofMillis(50), Duration.ofSeconds(25));
  }

  private IdempotencyConfiguration testConfig(
      boolean enabled, Duration inProgressWait, Duration pollInterval, Duration leaseTtl) {
    return new IdempotencyConfiguration() {
      @Override
      public boolean enabled() {
        return enabled;
      }

      @Override
      public String keyHeader() {
        return "Idempotency-Key";
      }

      @Override
      public Optional<String> executorId() {
        return Optional.of("test-executor");
      }

      @Override
      public Optional<String> purgeExecutorId() {
        return Optional.empty();
      }

      @Override
      public Duration ttl() {
        return Duration.ofMinutes(5);
      }

      @Override
      public Duration ttlGrace() {
        return Duration.ZERO;
      }

      @Override
      public Duration inProgressWait() {
        return inProgressWait;
      }

      @Override
      public Duration leaseTtl() {
        return leaseTtl;
      }

      @Override
      public Duration inProgressPollInterval() {
        return pollInterval;
      }

      @Override
      public boolean purgeEnabled() {
        return false;
      }

      @Override
      public Duration purgeInterval() {
        return Duration.ofMinutes(1);
      }

      @Override
      public Duration purgeGrace() {
        return Duration.ZERO;
      }
    };
  }

  /** Returns a UUID v7 with version nibble 7 and variant {@code 10}. */
  private static String uuidV7() {
    UUID base = UUID.randomUUID();
    long msb = base.getMostSignificantBits();
    msb &= ~0x000000000000F000L;
    msb |= 0x0000000000007000L;
    long lsb = base.getLeastSignificantBits();
    lsb &= ~(0xC000000000000000L);
    lsb |= 0x8000000000000000L;
    return new UUID(msb, lsb).toString();
  }
}
