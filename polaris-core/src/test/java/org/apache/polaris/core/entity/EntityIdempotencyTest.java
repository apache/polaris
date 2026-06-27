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
package org.apache.polaris.core.entity;

import static org.apache.polaris.core.entity.EntityIdempotency.IDEMPOTENCY_KEYS_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class EntityIdempotencyTest {

  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
  private static final Instant LATER = NOW.plusSeconds(300);

  @Test
  public void recordKeyStampsKeyWithExpiry() {
    UUID key = UUID.randomUUID();
    Map<String, String> internal = EntityIdempotency.recordKey(Map.of(), key, LATER, NOW);

    assertThat(internal).containsKey(IDEMPOTENCY_KEYS_PROPERTY);
    assertThat(EntityIdempotency.hasLiveKey(internal, key, NOW)).isTrue();
  }

  @Test
  public void recordKeyPreservesOtherInternalProperties() {
    UUID key = UUID.randomUUID();
    Map<String, String> internal =
        EntityIdempotency.recordKey(
            Map.of("metadata-location", "s3://bucket/m.json"), key, LATER, NOW);

    assertThat(internal).containsEntry("metadata-location", "s3://bucket/m.json");
    assertThat(EntityIdempotency.hasLiveKey(internal, key, NOW)).isTrue();
  }

  @Test
  public void hasLiveKeyReturnsFalseForUnknownKey() {
    Map<String, String> internal =
        EntityIdempotency.recordKey(Map.of(), UUID.randomUUID(), LATER, NOW);
    assertThat(EntityIdempotency.hasLiveKey(internal, UUID.randomUUID(), NOW)).isFalse();
  }

  @Test
  public void hasLiveKeyReturnsFalseForEmptyProperties() {
    assertThat(EntityIdempotency.hasLiveKey(Map.of(), UUID.randomUUID(), NOW)).isFalse();
  }

  @Test
  public void expiredKeyIsTreatedAsAbsentAtReadTime() {
    UUID key = UUID.randomUUID();
    Map<String, String> internal = EntityIdempotency.recordKey(Map.of(), key, LATER, NOW);

    // At an instant after the recorded expiry the key is no longer live.
    assertThat(EntityIdempotency.hasLiveKey(internal, key, LATER.plusSeconds(1))).isFalse();
    // The exact expiry instant is exclusive.
    assertThat(EntityIdempotency.hasLiveKey(internal, key, LATER)).isFalse();
  }

  @Test
  public void recordKeyPurgesExpiredKeysInline() {
    UUID stale = UUID.randomUUID();
    Map<String, String> withStale = EntityIdempotency.recordKey(Map.of(), stale, LATER, NOW);

    // A later write, after the stale key has expired, drops it while adding the new one.
    Instant muchLater = LATER.plusSeconds(1);
    UUID fresh = UUID.randomUUID();
    Map<String, String> updated =
        EntityIdempotency.recordKey(withStale, fresh, muchLater.plusSeconds(300), muchLater);

    assertThat(EntityIdempotency.hasLiveKey(updated, fresh, muchLater)).isTrue();
    assertThat(EntityIdempotency.hasLiveKey(updated, stale, muchLater)).isFalse();
    // Confirm the stale entry was physically removed, not just expired.
    assertThat(updated.get(IDEMPOTENCY_KEYS_PROPERTY)).doesNotContain(stale.toString());
  }

  @Test
  public void recordKeyKeepsMultipleLiveKeys() {
    UUID first = UUID.randomUUID();
    UUID second = UUID.randomUUID();
    Map<String, String> internal = EntityIdempotency.recordKey(Map.of(), first, LATER, NOW);
    internal = EntityIdempotency.recordKey(internal, second, LATER, NOW);

    assertThat(EntityIdempotency.hasLiveKey(internal, first, NOW)).isTrue();
    assertThat(EntityIdempotency.hasLiveKey(internal, second, NOW)).isTrue();
  }
}
