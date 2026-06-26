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

import java.time.Instant;
import java.util.HashMap;
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
 * <pre>{@code
 * {"018f...a1": 1716840000000, "018f...b2": 1716840300000}
 * }</pre>
 *
 * <p>For {@code createTable} this map holds a single entry, but the shape generalizes to a bounded
 * per-entity window for repeated mutations (e.g. {@code updateTable}). Expired keys are dropped
 * inline whenever the entity is rewritten ({@link #recordKey}); expiry is also honored at read time
 * ({@link #hasLiveKey}) so a stale-but-not-yet-purged key is treated as absent.
 */
public final class EntityIdempotency {

  /** Internal-properties key under which the per-entity idempotency key window is stored. */
  public static final String IDEMPOTENCY_KEYS_PROPERTY = "polaris-idempotency-keys";

  private EntityIdempotency() {}

  /**
   * Returns {@code true} if {@code key} is recorded on the given internal properties and has not
   * yet expired as of {@code now}.
   */
  public static boolean hasLiveKey(Map<String, String> internalProperties, UUID key, Instant now) {
    Map<String, Long> window = decode(internalProperties.get(IDEMPOTENCY_KEYS_PROPERTY));
    Long expiry = window.get(key.toString());
    return expiry != null && now.toEpochMilli() < expiry;
  }

  /**
   * Returns a copy of {@code internalProperties} with {@code key} recorded (expiring at {@code
   * expiry}) and any keys already expired as of {@code now} dropped. This is the inline
   * purge-on-write step: it runs within the same transaction that persists the entity, so no
   * separate maintenance pass is required to bound the window.
   */
  public static Map<String, String> recordKey(
      Map<String, String> internalProperties, UUID key, Instant expiry, Instant now) {
    Map<String, Long> window =
        new HashMap<>(decode(internalProperties.get(IDEMPOTENCY_KEYS_PROPERTY)));
    window.values().removeIf(existingExpiry -> existingExpiry <= now.toEpochMilli());
    window.put(key.toString(), expiry.toEpochMilli());

    Map<String, String> updated = new HashMap<>(internalProperties);
    updated.put(IDEMPOTENCY_KEYS_PROPERTY, encode(window));
    return updated;
  }

  private static Map<String, Long> decode(String raw) {
    if (raw == null || raw.isEmpty()) {
      return Map.of();
    }
    Map<String, String> asStrings = PolarisObjectMapperUtil.deserializeProperties(raw);
    Map<String, Long> window = new HashMap<>(asStrings.size());
    asStrings.forEach((k, v) -> window.put(k, Long.parseLong(v)));
    return window;
  }

  private static String encode(Map<String, Long> window) {
    Map<String, String> asStrings = new HashMap<>(window.size());
    window.forEach((k, v) -> asStrings.put(k, Long.toString(v)));
    return PolarisObjectMapperUtil.serializeProperties(asStrings);
  }
}
