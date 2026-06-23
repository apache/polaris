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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.InMemoryIdempotencyStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IdempotencyHandlerSupportTest {

  private static final String REALM = "realm-A";
  private static final IdempotentOperation OP = IdempotentOperation.CREATE_TABLE;
  private static final String RID = "catalogs/c1/tables/ns.t1";
  private static final String UUID_V7 = "0190f7f4-21d9-7e8b-9c8a-3c4f0a3e8b21";
  private static final UUID KEY = UUID.fromString(UUID_V7);
  private static final PolarisPrincipal PRINCIPAL =
      PolarisPrincipal.of("alice", Map.of(), Set.of("r1"));
  private static final PolarisPrincipal OTHER_PRINCIPAL =
      PolarisPrincipal.of("bob", Map.of(), Set.of("r1"));

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
          public Duration ttl() {
            return Duration.ofMinutes(5);
          }

          @Override
          public int concurrentReplayMaxAttempts() {
            return 5;
          }

          @Override
          public Duration concurrentReplayInitialBackoff() {
            return Duration.ofMillis(5);
          }
        };
    realmContext = () -> REALM;
    support = new IdempotencyHandlerSupport();
    support.configuration = config;
    support.store = new InMemoryIdempotencyStore(REALM);
    support.realmContext = realmContext;
    support.clock = Clock.fixed(Instant.parse("2025-01-01T00:00:00Z"), ZoneOffset.UTC);
  }

  @Test
  void validatedKey_acceptsUuidV7() {
    Optional<UUID> v = support.validatedKey(KEY);
    assertThat(v).contains(KEY);
  }

  @Test
  void validatedKey_rejectsNonUuidV7() {
    // UUIDv4 has version nibble 4, not 7.
    UUID uuidV4 = UUID.fromString("00000000-0000-4000-8000-000000000000");
    assertThatThrownBy(() -> support.validatedKey(uuidV4))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("UUIDv7");
  }

  @Test
  void validatedKey_nullReturnsEmpty() {
    assertThat(support.validatedKey((UUID) null)).isEmpty();
  }

  @Test
  void validatedKey_returnsEmptyWhenDisabled() {
    IdempotencyHandlerSupport disabled = IdempotencyHandlerSupport.disabled();
    assertThat(disabled.isEnabled()).isFalse();
    assertThat(disabled.validatedKey(KEY)).isEmpty();
  }

  @Test
  void bindingHash_isStableAndRoleOrderIndependent() {
    PolarisPrincipal p1 =
        PolarisPrincipal.of("alice", Map.of(), new java.util.LinkedHashSet<>(Set.of("r1", "r2")));
    PolarisPrincipal p2 =
        PolarisPrincipal.of("alice", Map.of(), new java.util.LinkedHashSet<>(Set.of("r2", "r1")));
    assertThat(support.bindingHash(OP, p1, RID)).isEqualTo(support.bindingHash(OP, p2, RID));
  }

  @Test
  void bindingHash_differsAcrossPrincipalsRealmsAndResources() {
    PolarisPrincipal alice = PolarisPrincipal.of("alice", Map.of(), Set.of("r1"));
    PolarisPrincipal bob = PolarisPrincipal.of("bob", Map.of(), Set.of("r1"));
    String hAlice = support.bindingHash(OP, alice, RID);
    assertThat(hAlice).isNotEqualTo(support.bindingHash(OP, bob, RID));

    // A different resource for the same caller/operation hashes differently.
    assertThat(hAlice).isNotEqualTo(support.bindingHash(OP, alice, "catalogs/c1/tables/ns.other"));

    // The realm is part of the binding: the same caller/operation/resource under a different realm
    // hashes differently. bindingHash only depends on the injected RealmContext, so a second
    // instance bound to another realm is sufficient.
    IdempotencyHandlerSupport supportRealmB = new IdempotencyHandlerSupport();
    supportRealmB.realmContext = () -> "realm-B";
    assertThat(supportRealmB.bindingHash(OP, alice, RID)).isNotEqualTo(hAlice);
  }

  @Test
  void preflight_disabledWhenKeyAbsent() {
    assertThat(support.preflight(Optional.empty(), PRINCIPAL, OP, RID))
        .isInstanceOf(IdempotencyOutcome.Disabled.class);
  }

  @Test
  void preflight_disabledWhenFeatureDisabled() {
    IdempotencyHandlerSupport disabled = IdempotencyHandlerSupport.disabled();
    assertThat(disabled.preflight(Optional.of(KEY), PRINCIPAL, OP, RID))
        .isInstanceOf(IdempotencyOutcome.Disabled.class);
  }

  @Test
  void preflight_emptyStoreReturnsNew() {
    IdempotencyOutcome o = support.preflight(Optional.of(KEY), PRINCIPAL, OP, RID);
    assertThat(o).isInstanceOf(IdempotencyOutcome.New.class);
  }

  @Test
  void preflight_existingMatchingRecordReturnsDuplicate() {
    IdempotencyOutcome first = support.preflight(Optional.of(KEY), PRINCIPAL, OP, RID);
    support.recordOutcome(first, 200, null);
    IdempotencyOutcome o = support.preflight(Optional.of(KEY), PRINCIPAL, OP, RID);
    assertThat(o).isInstanceOf(IdempotencyOutcome.Duplicate.class);
  }

  @Test
  void preflight_differentPrincipalRaisesConflict() {
    IdempotencyOutcome first = support.preflight(Optional.of(KEY), PRINCIPAL, OP, RID);
    support.recordOutcome(first, 200, null);
    assertThatThrownBy(() -> support.preflight(Optional.of(KEY), OTHER_PRINCIPAL, OP, RID))
        .isInstanceOf(IdempotencyConflictException.class);
  }

  @Test
  void preflight_differentResourceRaisesConflict() {
    IdempotencyOutcome first = support.preflight(Optional.of(KEY), PRINCIPAL, OP, RID);
    support.recordOutcome(first, 200, null);
    assertThatThrownBy(
            () -> support.preflight(Optional.of(KEY), PRINCIPAL, OP, "catalogs/c1/tables/ns.other"))
        .isInstanceOf(IdempotencyConflictException.class);
  }

  @Test
  void recordOutcome_disabledOutcomeIsNoOp() {
    assertThat(support.recordOutcome(IdempotencyOutcome.disabled(), 200, null))
        .isInstanceOf(IdempotencyOutcome.Disabled.class);
  }

  @Test
  void recordOutcome_secondCallWithSameBindingReturnsDuplicate() {
    IdempotencyOutcome.New newOutcome = new IdempotencyOutcome.New(KEY, OP, "binding-A");
    IdempotencyOutcome first = support.recordOutcome(newOutcome, 200, null);
    assertThat(first).isInstanceOf(IdempotencyOutcome.New.class);

    IdempotencyOutcome second = support.recordOutcome(newOutcome, 200, null);
    assertThat(second).isInstanceOf(IdempotencyOutcome.Duplicate.class);
  }

  @Test
  void recordOutcome_raceWithDifferentBindingRaisesConflict() {
    support.recordOutcome(new IdempotencyOutcome.New(KEY, OP, "binding-A"), 200, null);
    assertThatThrownBy(
            () ->
                support.recordOutcome(
                    new IdempotencyOutcome.New(KEY, OP, "binding-OTHER"), 200, null))
        .isInstanceOf(IdempotencyConflictException.class);
  }
}
