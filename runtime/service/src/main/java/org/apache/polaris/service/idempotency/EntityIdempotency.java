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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.polaris.core.exceptions.PolarisServiceUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper for the entity-property idempotency model (single-transaction / "embedded key" approach).
 *
 * <p>Instead of persisting idempotency records in a dedicated store, this model stamps the
 * idempotency key directly onto the {@code internalProperties} of the entity produced by the
 * originating operation. Because the key is part of the same {@code writeEntity} call that creates
 * (or updates) the entity, the key and the operation commit atomically in a single transaction —
 * there is no "record-after-success" gap where the entity is committed but a separate idempotency
 * write is lost.
 *
 * <p>The keys are stored under a single reserved internal-properties key ({@link
 * #IDEMPOTENCY_KEYS_PROPERTY}), encoded as the {@code IS1} version marker followed by a base64url
 * SMILE array of {@code (key, expiryMillis)} entries. Entries use single-character property names
 * ({@code k}, {@code e}) to keep the serialized window small. The version prefix leaves room to
 * evolve the encoding later without ambiguity.
 *
 * <p>For {@code createTable} this window holds a single entry, but the shape generalizes to a
 * per-entity window for repeated mutations (e.g. {@code updateTable}). The window is bounded only
 * by expiry (TTL): expired keys are dropped inline whenever the entity is rewritten ({@link
 * #recordKey}), and expiry is also honored at read time ({@link #hasLiveKey}) so a
 * stale-but-not-yet-purged key is treated as absent. Live keys are never evicted by count; a large
 * safety ceiling ({@link #MAX_LIVE_KEYS}) bounds the window by failing the write rather than
 * dropping a still-live key.
 */
public final class EntityIdempotency {

  private static final Logger LOGGER = LoggerFactory.getLogger(EntityIdempotency.class);

  /** Internal-properties key under which the per-entity idempotency key window is stored. */
  public static final String IDEMPOTENCY_KEYS_PROPERTY = "polaris-idempotency-keys";

  /** Magic prefix for the version 1 SMILE window ({@code IS1} + base64url bytes). */
  private static final String FORMAT_SMILE_V1 = "IS1";

  /**
   * Safety ceiling on the number of live keys retained on a single entity. Unlike a count-based
   * eviction cap, this never drops a live key: when the window is full the write fails instead, so
   * a key that could still be retried is never silently discarded. The limit is deliberately high —
   * reaching it requires this many successful commits to one table within the key TTL — so
   * legitimate workloads never hit it, while still bounding stored idempotency data against
   * unbounded growth (a DoS guardrail).
   */
  private static final int MAX_LIVE_KEYS = 10_000;

  private static final ObjectMapper SMILE_MAPPER = SmileMapper.builder().build();

  private static final Comparator<KeyEntry> BY_EXPIRY =
      Comparator.comparingLong((KeyEntry e) -> e.expiryMillis).thenComparing(e -> e.key);

  private EntityIdempotency() {}

  /**
   * Returns {@code true} if {@code key} is recorded on the given internal properties and has not
   * yet expired as of {@code now}.
   */
  public static boolean hasLiveKey(Map<String, String> internalProperties, UUID key, Instant now) {
    String keyString = key.toString();
    long nowMillis = now.toEpochMilli();
    for (KeyEntry entry : decode(internalProperties.get(IDEMPOTENCY_KEYS_PROPERTY))) {
      if (entry.key.equals(keyString) && nowMillis < entry.expiryMillis) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns a copy of {@code internalProperties} with {@code key} recorded (expiring at {@code
   * expiry}) and any keys already expired as of {@code now} dropped. This is the inline
   * purge-on-write step: it runs within the same transaction that persists the entity, so no
   * separate maintenance pass is required.
   *
   * <p>The window is bounded only by expiry (TTL), never by a count-based eviction: a live key is
   * never evicted to make room. Evicting a still-live key would silently disable idempotency for it
   * and reintroduce the corruption failure mode (a retry would re-run and could 409), so under a
   * high write rate we accept a larger window rather than dropping keys that could still be
   * retried. A large safety ceiling ({@link #MAX_LIVE_KEYS}) still bounds the window: when it is
   * reached the write fails with a retryable {@link PolarisServiceUnavailableException} (HTTP 503
   * with a {@code Retry-After}) instead of dropping a key.
   */
  public static Map<String, String> recordKey(
      Map<String, String> internalProperties, UUID key, Instant expiry, Instant now) {
    return recordKey(internalProperties, key, expiry, now, MAX_LIVE_KEYS);
  }

  /**
   * Overload that takes an explicit ceiling so tests can exercise the guardrail without recording
   * {@link #MAX_LIVE_KEYS} keys. Production always calls the public overload with {@link
   * #MAX_LIVE_KEYS}.
   */
  @VisibleForTesting
  static Map<String, String> recordKey(
      Map<String, String> internalProperties,
      UUID key,
      Instant expiry,
      Instant now,
      int maxLiveKeys) {
    long nowMillis = now.toEpochMilli();
    String keyString = key.toString();

    // The window is stored (and therefore decoded) sorted by expiry, so it stays sorted with a
    // binary-search insertion instead of a re-sort on every write. Filtering out expired entries
    // and any prior copy of this key preserves that order.
    List<KeyEntry> window = new ArrayList<>();
    for (KeyEntry entry : decode(internalProperties.get(IDEMPOTENCY_KEYS_PROPERTY))) {
      if (entry.expiryMillis > nowMillis && !entry.key.equals(keyString)) {
        window.add(entry);
      }
    }

    // Fail rather than evict a live key: dropping one would silently disable idempotency for it.
    // The window is sorted by expiry, so its first entry expires soonest; a slot frees (and the
    // write can succeed) once it does, so advertise that as the Retry-After on the 503.
    if (window.size() >= maxLiveKeys) {
      long retryAfterSeconds = Math.max(1, (window.get(0).expiryMillis - nowMillis + 999) / 1000);
      throw new PolarisServiceUnavailableException(
          (int) retryAfterSeconds,
          "Idempotency key window for this entity is full (%d live keys); refusing to record "
              + "another to bound entity size. This indicates an abnormally high rate of "
              + "idempotent writes to a single table within the key TTL.",
          maxLiveKeys);
    }

    KeyEntry newEntry = new KeyEntry(keyString, expiry.toEpochMilli());
    int insertionPoint = Collections.binarySearch(window, newEntry, BY_EXPIRY);
    if (insertionPoint < 0) {
      insertionPoint = -(insertionPoint + 1);
    }
    window.add(insertionPoint, newEntry);

    Map<String, String> updated = new HashMap<>(internalProperties);
    updated.put(IDEMPOTENCY_KEYS_PROPERTY, encode(window));
    return updated;
  }

  private static List<KeyEntry> decode(String raw) {
    if (raw == null || !raw.startsWith(FORMAT_SMILE_V1)) {
      return List.of();
    }
    // An unreadable window (bad base64 or SMILE decode failure) is treated as no live keys rather
    // than failing the operation: a corrupt property degrades to normal behavior and the next
    // recordKey overwrites it.
    try {
      byte[] smile = Base64.getUrlDecoder().decode(raw.substring(FORMAT_SMILE_V1.length()));
      return SMILE_MAPPER.readValue(smile, new TypeReference<List<KeyEntry>>() {});
    } catch (IllegalArgumentException | IOException e) {
      // Server-written data, so a decode failure means corruption or a tampered property; surface
      // it. Only WARN (not ERROR) because it self-heals: the operation proceeds as if no keys were
      // recorded and the next write overwrites the property with a valid window.
      LOGGER.warn(
          "Ignoring unreadable idempotency key window in property '{}'; treating as no live keys.",
          IDEMPOTENCY_KEYS_PROPERTY,
          e);
      return List.of();
    }
  }

  private static String encode(Collection<KeyEntry> window) {
    try {
      byte[] smile = SMILE_MAPPER.writeValueAsBytes(window);
      return FORMAT_SMILE_V1 + Base64.getUrlEncoder().withoutPadding().encodeToString(smile);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to encode idempotency key window", e);
    }
  }

  private record KeyEntry(@JsonProperty("k") String key, @JsonProperty("e") long expiryMillis) {}
}
