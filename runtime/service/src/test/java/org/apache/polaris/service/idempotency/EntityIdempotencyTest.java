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

import static org.apache.polaris.service.idempotency.EntityIdempotency.IDEMPOTENCY_KEYS_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class EntityIdempotencyTest {

  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
  private static final Instant LATER = NOW.plusSeconds(300);

  @Test
  public void encodedWindowUsesSmileFormat() {
    UUID key = UUID.randomUUID();
    Map<String, String> internal = EntityIdempotency.recordKey(Map.of(), key, LATER, NOW);

    String windowJson = internal.get(IDEMPOTENCY_KEYS_PROPERTY);
    assertThat(windowJson).startsWith("IS1");
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

  @Test
  public void recordKeyEvictsEarliestWhenFull() {
    // The cap scales with the key TTL. At the 5-minute baseline the window holds 64 keys.
    // Expiries are spaced by milliseconds so they stay distinct but share one TTL bucket,
    // giving a well-defined earliest-expiring eviction target.
    Instant base = NOW.plus(Duration.ofMinutes(5));
    int baselineCap = 64;

    UUID earliest = UUID.randomUUID();
    Map<String, String> internal =
        EntityIdempotency.recordKey(Map.of(), earliest, base.plusMillis(1), NOW);
    UUID secondEarliest = UUID.randomUUID();
    internal = EntityIdempotency.recordKey(internal, secondEarliest, base.plusMillis(2), NOW);
    for (int i = 3; i <= baselineCap; i++) {
      internal = EntityIdempotency.recordKey(internal, UUID.randomUUID(), base.plusMillis(i), NOW);
    }

    // One more write past capacity must evict only the earliest-expiring key while retaining the
    // key just recorded (trim-before-insert: the new key is never the one dropped).
    UUID newest = UUID.randomUUID();
    internal =
        EntityIdempotency.recordKey(internal, newest, base.plusMillis(baselineCap + 1L), NOW);

    // Read at NOW, when every recorded key would still be live if present, so absence == eviction.
    // Only the single earliest key is dropped: the new key and the next-earliest survive.
    assertThat(EntityIdempotency.hasLiveKey(internal, newest, NOW)).isTrue();
    assertThat(EntityIdempotency.hasLiveKey(internal, secondEarliest, NOW)).isTrue();
    assertThat(EntityIdempotency.hasLiveKey(internal, earliest, NOW)).isFalse();
  }

  @Test
  public void windowCapScalesWithTtl() {
    // A 10-minute TTL doubles the baseline cap to 128, so 65 keys (which would evict the earliest
    // at
    // the 5-minute baseline) all fit and none is evicted.
    Instant base = NOW.plus(Duration.ofMinutes(10));
    UUID earliest = UUID.randomUUID();
    Map<String, String> internal =
        EntityIdempotency.recordKey(Map.of(), earliest, base.plusMillis(1), NOW);
    for (int i = 2; i <= 65; i++) {
      internal = EntityIdempotency.recordKey(internal, UUID.randomUUID(), base.plusMillis(i), NOW);
    }

    assertThat(EntityIdempotency.hasLiveKey(internal, earliest, NOW)).isTrue();
  }

  @Test
  public void malformedWindowIsTreatedAsNoLiveKeys() {
    // A corrupt/unrecognized window must degrade to "no live keys" rather than throwing: an unknown
    // format prefix, and a valid prefix followed by non-base64 bytes.
    Map<String, String> unknownPrefix = Map.of(IDEMPOTENCY_KEYS_PROPERTY, "XYZgarbage");
    Map<String, String> badBase64 = Map.of(IDEMPOTENCY_KEYS_PROPERTY, "IS1@@@not-base64@@@");

    assertThat(EntityIdempotency.hasLiveKey(unknownPrefix, UUID.randomUUID(), NOW)).isFalse();
    assertThat(EntityIdempotency.hasLiveKey(badBase64, UUID.randomUUID(), NOW)).isFalse();
  }

  @Test
  public void recordKeyOverwritesMalformedWindow() {
    UUID key = UUID.randomUUID();
    Map<String, String> corrupt = Map.of(IDEMPOTENCY_KEYS_PROPERTY, "IS1@@@not-base64@@@");

    Map<String, String> updated = EntityIdempotency.recordKey(corrupt, key, LATER, NOW);

    assertThat(updated.get(IDEMPOTENCY_KEYS_PROPERTY)).startsWith("IS1");
    assertThat(EntityIdempotency.hasLiveKey(updated, key, NOW)).isTrue();
  }

  /**
   * {@code createTable} always stamps exactly one key, but the retry check ({@link
   * EntityIdempotency#hasLiveKey}) must still find the right key when the entity carries a larger
   * window — the shape a future {@code updateTable} path would produce.
   */
  @ParameterizedTest
  @ValueSource(ints = {1, 8, 64, 300})
  public void hasLiveKeyFindsTargetAmongManyKeys(int numberOfKeys) {
    Map<String, String> internal = Map.of("metadata-location", "s3://bucket/ns/tbl/metadata.json");
    UUID targetKey = UUID.randomUUID();

    for (int i = 0; i < numberOfKeys - 1; i++) {
      internal = EntityIdempotency.recordKey(internal, UUID.randomUUID(), LATER, NOW);
    }
    internal = EntityIdempotency.recordKey(internal, targetKey, LATER, NOW);

    assertThat(EntityIdempotency.hasLiveKey(internal, targetKey, NOW)).isTrue();
    assertThat(EntityIdempotency.hasLiveKey(internal, UUID.randomUUID(), NOW)).isFalse();
  }
}
