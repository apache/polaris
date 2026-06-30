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
import java.util.concurrent.TimeUnit;
import org.apache.polaris.core.persistence.PolarisObjectMapperUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A/B microbenchmark for the entity-property idempotency model: the same entity-internal-properties
 * work <b>without</b> an idempotency key (the baseline Polaris does today) vs. <b>with</b> one. The
 * delta between each {@code *_noKey} and {@code *_withKey} pair is the extra CPU the model adds per
 * request.
 *
 * <p>Two paths are modeled, mirroring what the persistence layer actually does:
 *
 * <ul>
 *   <li><b>create</b> — serialize the table entity's internal properties for persistence ({@code
 *       createSerialize_*}). The {@code withKey} variant additionally records the key (purge + add)
 *       and serializes the slightly larger map.
 *   <li><b>retry/load</b> — parse the entity's internal properties ({@code load_*}). The baseline
 *       parses the map and reads {@code metadata-location} (which the load path needs anyway); the
 *       {@code withKey} variant also runs the replay check ({@link EntityIdempotency#hasLiveKey}).
 * </ul>
 *
 * <p>{@code windowSize} is how many keys the entity already holds: <b>1 is the realistic value for
 * {@code createTable}</b> (created once → one key). Larger values model a bounded multi-key window
 * (e.g. a future {@code updateTable}) and show how the cost grows with the key count.
 *
 * <p>Run with {@code ./gradlew :polaris-core:jmh}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class EntityIdempotencyBench {

  @Param({"1", "8", "64", "300"})
  public int windowSize;

  private Instant now;
  private Instant expiry;

  // Realistic table entity internal properties WITHOUT any idempotency key: the no-idempotency
  // baseline (a typical createTable produces ~13 such entries).
  private Map<String, String> baseProps;
  private String baseJson;

  // Same internal properties already carrying (windowSize - 1) keys; the create benchmark adds one
  // more on top, so the resulting entity holds exactly windowSize keys.
  private Map<String, String> preWindowProps;

  // Same internal properties carrying windowSize keys, serialized: the input to the retry/load
  // path.
  private String withKeyJson;
  private UUID requestKey;

  @Setup(Level.Trial)
  public void setUp() {
    now = Instant.parse("2026-01-01T00:00:00Z");
    expiry = now.plusSeconds(300);

    baseProps = new HashMap<>();
    baseProps.put(
        "metadata-location", "s3://bucket/ns/tbl/metadata/00000-7f3a1b2c-d4e5.metadata.json");
    baseProps.put("location", "s3://bucket/ns/tbl");
    baseProps.put("table-uuid", UUID.randomUUID().toString());
    baseProps.put("format-version", "2");
    baseProps.put("current-schema-id", "0");
    baseProps.put("current-snapshot-id", "3821550127947089060");
    baseProps.put("last-column-id", "12");
    baseProps.put("next-row-id", "0");
    baseProps.put("last-sequence-number", "1");
    baseProps.put("last-updated-ms", "1767225600000");
    baseProps.put("default-sort-order-id", "0");
    baseProps.put("default-spec-id", "0");
    baseProps.put("last-partition-id", "999");
    baseJson = PolarisObjectMapperUtil.serializeProperties(baseProps);

    // Pre-populate windowSize keys to build the load input and the (windowSize - 1) create input.
    Map<String, String> withWindow = baseProps;
    Map<String, String> minusOne = baseProps;
    for (int i = 0; i < windowSize; i++) {
      UUID key = UUID.randomUUID();
      if (i == 0) {
        requestKey = key;
      }
      if (i == windowSize - 1) {
        minusOne = withWindow;
      }
      withWindow = EntityIdempotency.recordKey(withWindow, key, expiry, now);
    }
    preWindowProps = minusOne;
    withKeyJson = PolarisObjectMapperUtil.serializeProperties(withWindow);
  }

  // ---- create path: serialize internal properties for persistence ----

  /** Baseline: serialize the entity's internal properties with no idempotency key. */
  @Benchmark
  public String createSerialize_noKey() {
    return PolarisObjectMapperUtil.serializeProperties(baseProps);
  }

  /** With idempotency: record the key (purge + add) then serialize the resulting larger map. */
  @Benchmark
  public String createSerialize_withKey() {
    Map<String, String> withKey =
        EntityIdempotency.recordKey(preWindowProps, UUID.randomUUID(), expiry, now);
    return PolarisObjectMapperUtil.serializeProperties(withKey);
  }

  // ---- retry/load path: parse internal properties (+ replay check) ----

  /** Baseline: parse internal properties and read the metadata location (the load path's work). */
  @Benchmark
  public void load_noKey(Blackhole bh) {
    Map<String, String> parsed = PolarisObjectMapperUtil.deserializeProperties(baseJson);
    bh.consume(parsed.get("metadata-location"));
  }

  /** With idempotency: parse internal properties and additionally run the replay check. */
  @Benchmark
  public void load_withKey(Blackhole bh) {
    Map<String, String> parsed = PolarisObjectMapperUtil.deserializeProperties(withKeyJson);
    bh.consume(parsed.get("metadata-location"));
    bh.consume(EntityIdempotency.hasLiveKey(parsed, requestKey, now));
  }
}
