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
 * bounded per-entity window for repeated mutations (e.g. {@code updateTable}). Expired keys are
 * dropped inline whenever the entity is rewritten ({@link #recordKey}); expiry is also honored at
 * read time ({@link #hasLiveKey}) so a stale-but-not-yet-purged key is treated as absent.
 */
public final class EntityIdempotency {

  /** Internal-properties key under which the per-entity idempotency key window is stored. */
  public static final String IDEMPOTENCY_KEYS_PROPERTY = "polaris-idempotency-keys";

  /** Upper bound on live idempotency keys stored on a single entity. */
  public static final int MAX_WINDOW_SIZE = 64;

  /** Magic prefix for the version 1 SMILE window ({@code IS1} + base64url bytes). */
  private static final String WINDOW_FORMAT_SMILE_V1 = "IS1";

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
   * separate maintenance pass is required to bound the window. Capped at {@link #MAX_WINDOW_SIZE}.
   */
  public static Map<String, String> recordKey(
      Map<String, String> internalProperties, UUID key, Instant expiry, Instant now) {
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

    // Bounded window: drop the earliest-expiring entries (front of the sorted list) to make room
    // *before* inserting, so the key just recorded is always retained even when every entry shares
    // the same expiry (e.g. a burst of writes at one instant) and expiry order can't rank recency.
    while (window.size() >= MAX_WINDOW_SIZE) {
      window.remove(0);
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
    if (raw == null || raw.isEmpty()) {
      return List.of();
    }
    if (!raw.startsWith(WINDOW_FORMAT_SMILE_V1)) {
      throw new IllegalArgumentException("Unrecognized idempotency key window format");
    }
    byte[] smile = Base64.getUrlDecoder().decode(raw.substring(WINDOW_FORMAT_SMILE_V1.length()));
    try {
      return SMILE_MAPPER.readValue(smile, new TypeReference<List<KeyEntry>>() {});
    } catch (IOException e) {
      throw new RuntimeException("Failed to decode idempotency key window", e);
    }
  }

  private static String encode(Collection<KeyEntry> window) {
    try {
      byte[] smile = SMILE_MAPPER.writeValueAsBytes(window);
      return WINDOW_FORMAT_SMILE_V1 + Base64.getUrlEncoder().withoutPadding().encodeToString(smile);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to encode idempotency key window", e);
    }
  }

  private record KeyEntry(@JsonProperty("k") String key, @JsonProperty("e") long expiryMillis) {}
}
