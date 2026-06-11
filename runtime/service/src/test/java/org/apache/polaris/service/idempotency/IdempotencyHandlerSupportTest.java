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
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.InMemoryIdempotencyStoreFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IdempotencyHandlerSupportTest {

  private static final String REALM = "realm-A";
  private static final IdempotentOperation OP = IdempotentOperation.CREATE_TABLE;
  private static final String RID = "catalogs/c1/tables/ns.t1";
  private static final String UUID_V7 = "0190f7f4-21d9-7e8b-9c8a-3c4f0a3e8b21";

  private IdempotencyHandlerSupport support;
  private RealmContext realmContext;

  @BeforeEach
  void setUp() {
    IdempotencyConfiguration config =
        new IdempotencyConfiguration() {
          @Override
          public boolean enabled() {
            return true;
          }

          @Override
          public String type() {
            return "in-memory";
          }

          @Override
          public String keyHeader() {
            return "Idempotency-Key";
          }

          @Override
          public Duration ttl() {
            return Duration.ofMinutes(5);
          }

          @Override
          public boolean purgeEnabled() {
            return false;
          }

          @Override
          public Duration purgeInterval() {
            return Duration.ofDays(1);
          }
        };
    realmContext = () -> REALM;
    support = new IdempotencyHandlerSupport();
    support.configuration = config;
    support.storeFactory = new InMemoryIdempotencyStoreFactory();
    support.realmContext = realmContext;
    support.clock = Clock.fixed(Instant.parse("2025-01-01T00:00:00Z"), ZoneOffset.UTC);
  }

  @Test
  void validatedKey_acceptsUuidV7AndLowercasesIt() {
    Optional<String> v = support.validatedKey(UUID_V7.toUpperCase(Locale.ROOT));
    assertThat(v).contains(UUID_V7);
  }

  @Test
  void validatedKey_rejectsNonUuidV7() {
    // UUIDv4 has version nibble 4, not 7.
    String uuidV4 = "00000000-0000-4000-8000-000000000000";
    assertThatThrownBy(() -> support.validatedKey(uuidV4))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("UUIDv7");
  }

  @Test
  void validatedKey_emptyOrNullReturnsEmpty() {
    assertThat(support.validatedKey((String) null)).isEmpty();
    assertThat(support.validatedKey("   ")).isEmpty();
  }

  @Test
  void validatedKey_returnsEmptyWhenDisabled() {
    IdempotencyHandlerSupport disabled = IdempotencyHandlerSupport.disabled();
    assertThat(disabled.isEnabled()).isFalse();
    assertThat(disabled.validatedKey(UUID_V7)).isEmpty();
  }

  @Test
  void principalHash_isStableAndRoleOrderIndependent() {
    PolarisPrincipal p1 =
        PolarisPrincipal.of("alice", Map.of(), new java.util.LinkedHashSet<>(Set.of("r1", "r2")));
    PolarisPrincipal p2 =
        PolarisPrincipal.of("alice", Map.of(), new java.util.LinkedHashSet<>(Set.of("r2", "r1")));
    String h1 = support.principalHash(p1);
    String h2 = support.principalHash(p2);
    assertThat(h1).isEqualTo(h2);
  }

  @Test
  void principalHash_differsAcrossPrincipalsAndRealms() {
    PolarisPrincipal alice = PolarisPrincipal.of("alice", Map.of(), Set.of("r1"));
    PolarisPrincipal bob = PolarisPrincipal.of("bob", Map.of(), Set.of("r1"));
    String hAlice = support.principalHash(alice);
    String hBob = support.principalHash(bob);
    assertThat(hAlice).isNotEqualTo(hBob);

    // The realm is part of the binding: the same principal under a different realm hashes
    // differently. principalHash only depends on the injected RealmContext, so a second instance
    // bound to another realm is sufficient.
    IdempotencyHandlerSupport supportRealmB = new IdempotencyHandlerSupport();
    supportRealmB.realmContext = () -> "realm-B";
    assertThat(supportRealmB.principalHash(alice)).isNotEqualTo(hAlice);
  }

  @Test
  void preflight_emptyStoreReturnsOwned() {
    IdempotencyHandlerSupport.Outcome o = support.preflight(UUID_V7, OP, RID, "ph-A");
    assertThat(o).isInstanceOf(IdempotencyHandlerSupport.Outcome.Owned.class);
  }

  @Test
  void preflight_existingMatchingRecordReturnsDuplicate() {
    support.recordOutcome(UUID_V7, OP, RID, "ph-A", 200, null);
    IdempotencyHandlerSupport.Outcome o = support.preflight(UUID_V7, OP, RID, "ph-A");
    assertThat(o).isInstanceOf(IdempotencyHandlerSupport.Outcome.Duplicate.class);
  }

  @Test
  void preflight_differentPrincipalRaisesConflict() {
    support.recordOutcome(UUID_V7, OP, RID, "ph-A", 200, null);
    assertThatThrownBy(() -> support.preflight(UUID_V7, OP, RID, "ph-OTHER"))
        .isInstanceOf(IdempotencyConflictException.class);
  }

  @Test
  void preflight_differentResourceRaisesConflict() {
    support.recordOutcome(UUID_V7, OP, RID, "ph-A", 200, null);
    assertThatThrownBy(() -> support.preflight(UUID_V7, OP, "catalogs/c1/tables/ns.other", "ph-A"))
        .isInstanceOf(IdempotencyConflictException.class);
  }

  @Test
  void recordOutcome_secondCallWithSameBindingReturnsDuplicate() {
    IdempotencyHandlerSupport.Outcome first =
        support.recordOutcome(UUID_V7, OP, RID, "ph-A", 200, null);
    assertThat(first).isInstanceOf(IdempotencyHandlerSupport.Outcome.Owned.class);

    IdempotencyHandlerSupport.Outcome second =
        support.recordOutcome(UUID_V7, OP, RID, "ph-A", 200, null);
    assertThat(second).isInstanceOf(IdempotencyHandlerSupport.Outcome.Duplicate.class);
  }

  @Test
  void recordOutcome_raceWithDifferentPrincipalRaisesConflict() {
    support.recordOutcome(UUID_V7, OP, RID, "ph-A", 200, null);
    assertThatThrownBy(() -> support.recordOutcome(UUID_V7, OP, RID, "ph-OTHER", 200, null))
        .isInstanceOf(IdempotencyConflictException.class);
  }
}
