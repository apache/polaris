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
package org.apache.polaris.persistence.nosql.impl.cache;

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.cache.CacheConfig;
import org.apache.polaris.persistence.nosql.api.cache.CacheSizing;
import org.apache.polaris.persistence.nosql.api.obj.SimpleTestObj;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestCacheSerialization {
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 127, 128, 65536})
  void binaryRemainsRawAndRoundTrips(int size) {
    byte[] bytes = new byte[size];
    for (int i = 0; i < size; i++) {
      bytes[i] = (byte) i;
    }
    var encoded = CaffeineCacheBackend.SMILE_MAPPER.writeValueAsBytes(bytes);
    assertThat(encoded.length).isLessThanOrEqualTo(size + 16);
    assertThat(CaffeineCacheBackend.SMILE_MAPPER.readValue(encoded, byte[].class))
        .containsExactly(bytes);

    var backend =
        new CaffeineCacheBackend(
            CacheConfig.BuildableCacheConfig.builder()
                .sizing(CacheSizing.builder().fixedSize(MemorySize.ofMega(8)).build())
                .build(),
            Optional.empty());
    var obj = SimpleTestObj.builder().id(42).binary(bytes).build();
    backend.putLocal("realm", obj);
    assertThat(backend.get("realm", objRef(obj))).isEqualTo(obj);
  }

  @ParameterizedTest
  @MethodSource("instants")
  void instantRetainsPrecisionAndRange(Instant instant) {
    var mapper = CaffeineCacheBackend.SMILE_MAPPER;
    var bytes = mapper.writeValueAsBytes(instant);
    assertThat(mapper.readTree(bytes).isArray()).isTrue();
    assertThat(mapper.readValue(bytes, Instant.class)).isEqualTo(instant);
    var backend =
        new CaffeineCacheBackend(
            CacheConfig.BuildableCacheConfig.builder()
                .sizing(CacheSizing.builder().fixedSize(MemorySize.ofMega(8)).build())
                .build(),
            Optional.empty());
    var obj = SimpleTestObj.builder().id(42).instant(instant).build();
    backend.putLocal("realm", obj);
    assertThat(backend.get("realm", objRef(obj))).isEqualTo(obj);
  }

  static Stream<Instant> instants() {
    return Stream.of(
        Instant.MIN,
        Instant.MAX,
        Instant.EPOCH,
        Instant.ofEpochSecond(-1, 1),
        Instant.ofEpochSecond(-1, 999999999),
        Instant.ofEpochSecond(1700000000L, 123456789));
  }

  @ParameterizedTest
  @MethodSource("durations")
  void durationRetainsPrecisionAndRange(Duration duration) {
    var mapper = CaffeineCacheBackend.SMILE_MAPPER;
    var bytes = mapper.writeValueAsBytes(duration);
    assertThat(mapper.readTree(bytes).isArray()).isTrue();
    assertThat(mapper.readValue(bytes, Duration.class)).isEqualTo(duration);
  }

  static Stream<Duration> durations() {
    return Stream.of(
        Duration.ZERO,
        Duration.ofNanos(1),
        Duration.ofNanos(-1),
        Duration.ofSeconds(-1, 1),
        Duration.ofSeconds(300, 123456789),
        Duration.ofSeconds(Long.MIN_VALUE),
        Duration.ofSeconds(Long.MAX_VALUE, 999999999));
  }

  @Test
  void timePairsComposeWithPropertiesAndOptionalValues() {
    var mapper = CaffeineCacheBackend.SMILE_MAPPER;
    var instant = Instant.ofEpochSecond(-1, 123456789);
    var populated = new TimeProperties(instant, Duration.ofNanos(-1), Optional.of(instant));
    var empty = new TimeProperties(null, null, Optional.empty());
    for (var value : new TimeProperties[] {populated, empty}) {
      var encoded = mapper.writeValueAsBytes(value);
      assertThat(mapper.readValue(encoded, TimeProperties.class)).isEqualTo(value);
    }
  }

  private record TimeProperties(
      @JsonFormat(shape = JsonFormat.Shape.NUMBER) Instant instant,
      Duration duration,
      Optional<Instant> optional) {}
}
