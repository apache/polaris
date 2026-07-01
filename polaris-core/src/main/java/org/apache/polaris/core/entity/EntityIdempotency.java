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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.polaris.core.persistence.PolarisObjectMapperUtil;

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
 * #IDEMPOTENCY_KEYS_PROPERTY}) as a compact JSON object mapping the key (a UUID string) to its
 * expiry timestamp (epoch millis):
 *
 * <p>Wire formats (prefix + payload):
 *
 * <ul>
 *   <li>{@code IS1} + base64url(SMILE map) — current write format
 *   <li>{@code ID1} + JSON map — prior format, still accepted on read
 * </ul>
 *
 * <p>For {@code createTable} this map holds a single entry, but the shape generalizes to a bounded
 * per-entity window for repeated mutations (e.g. {@code updateTable}). Expired keys are dropped
 * inline whenever the entity is rewritten ({@link #recordKey}); expiry is also honored at read time
 * ({@link #hasLiveKey}) so a stale-but-not-yet-purged key is treated as absent.
 */
public final class EntityIdempotency {

  /** Internal-properties key under which the per-entity idempotency key window is stored. */
  public static final String IDEMPOTENCY_KEYS_PROPERTY = "polaris-idempotency-keys";

  /** Upper bound on live idempotency keys stored on a single entity. */
  public static final int MAX_WINDOW_SIZE = 64;

  /** Magic prefix for version 1 JSON window ({@code ID1} + JSON object). Accepted on read only. */
  private static final String WINDOW_FORMAT_JSON_V1 = "ID1";

  /** Magic prefix for version 1 SMILE window ({@code IS1} + base64url bytes). Used for writes. */
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
    List<KeyEntry> window =
        new ArrayList<>(decode(internalProperties.get(IDEMPOTENCY_KEYS_PROPERTY)));
    long nowMillis = now.toEpochMilli();
    window.removeIf(entry -> entry.expiryMillis <= nowMillis);

    String keyString = key.toString();
    window.removeIf(entry -> entry.key.equals(keyString));
    while (window.size() >= MAX_WINDOW_SIZE) {
      window.sort(BY_EXPIRY);
      window.remove(0);
    }
    window.add(new KeyEntry(keyString, expiry.toEpochMilli()));

    Map<String, String> updated = new HashMap<>(internalProperties);
    updated.put(IDEMPOTENCY_KEYS_PROPERTY, encode(window));
    return updated;
  }

  @SuppressWarnings("unchecked")
  private static List<KeyEntry> decode(String raw) {
    if (raw == null || raw.isEmpty()) {
      return List.of();
    }
    Map<String, ?> parsed;
    if (raw.startsWith(WINDOW_FORMAT_SMILE_V1)) {
      byte[] smile = Base64.getUrlDecoder().decode(raw.substring(WINDOW_FORMAT_SMILE_V1.length()));
      try {
        parsed = SMILE_MAPPER.readValue(smile, Map.class);
      } catch (IOException e) {
        throw new RuntimeException("Failed to decode idempotency key window", e);
      }
    } else {
      String json =
          raw.startsWith(WINDOW_FORMAT_JSON_V1)
              ? raw.substring(WINDOW_FORMAT_JSON_V1.length())
              : raw;
      parsed = PolarisObjectMapperUtil.deserialize(json, Map.class);
    }
    List<KeyEntry> window = new ArrayList<>(parsed.size());
    parsed.forEach(
        (key, value) -> {
          long expiryMillis =
              value instanceof Number number
                  ? number.longValue()
                  : Long.parseLong(value.toString());
          window.add(new KeyEntry(key, expiryMillis));
        });
    return window;
  }

  private static String encode(List<KeyEntry> window) {
    Map<String, Long> asMap = new HashMap<>(window.size());
    for (KeyEntry entry : window) {
      asMap.put(entry.key, entry.expiryMillis);
    }
    try {
      byte[] smile = SMILE_MAPPER.writeValueAsBytes(asMap);
      return WINDOW_FORMAT_SMILE_V1 + Base64.getUrlEncoder().withoutPadding().encodeToString(smile);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to encode idempotency key window", e);
    }
  }

  private record KeyEntry(String key, long expiryMillis) {}
}
