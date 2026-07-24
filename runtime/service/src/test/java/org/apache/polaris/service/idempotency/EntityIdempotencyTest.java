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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
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
  public void recordKeyRetainsAllLiveKeysWithoutCountCap() {
    // The window is bounded only by expiry, never by a fixed count: recording many still-live keys
    // must retain every one. Evicting a live key would silently disable idempotency for it and
    // could reintroduce the corruption failure mode idempotency exists to prevent.
    int numberOfKeys = 500;
    UUID[] keys = new UUID[numberOfKeys];
    Map<String, String> internal = Map.of();
    for (int i = 0; i < numberOfKeys; i++) {
      keys[i] = UUID.randomUUID();
      internal = EntityIdempotency.recordKey(internal, keys[i], LATER, NOW);
    }

    for (UUID key : keys) {
      assertThat(EntityIdempotency.hasLiveKey(internal, key, NOW)).isTrue();
    }
  }

  @Test
  public void recordKeyFailsRatherThanEvictWhenSafetyCeilingReached() {
    // At the safety ceiling the write fails instead of evicting a live key. Fill the window to the
    // (small, test-only) ceiling, then a further live key must throw rather than drop an existing
    // one. Expired entries are still purged, so a full window of live keys is required to trip it.
    int ceiling = 3;
    Map<String, String> internal = Map.of();
    for (int i = 0; i < ceiling; i++) {
      internal = EntityIdempotency.recordKey(internal, UUID.randomUUID(), LATER, NOW, ceiling);
    }

    Map<String, String> full = internal;
    assertThatThrownBy(
            () -> EntityIdempotency.recordKey(full, UUID.randomUUID(), LATER, NOW, ceiling))
        .isInstanceOf(ServiceUnavailableException.class)
        .hasMessageContaining("full");
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
