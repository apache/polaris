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

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.polaris.persistence.nosql.api.cache.CacheBackend.NON_EXISTENT_REFERENCE_SENTINEL;
import static org.apache.polaris.persistence.nosql.api.cache.CacheBackend.NOT_FOUND_OBJ_SENTINEL;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.cache.CacheConfig;
import org.apache.polaris.persistence.nosql.api.cache.CacheSizing;
import org.apache.polaris.persistence.nosql.api.obj.VersionedTestObj;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestCacheReplacement {
  private final AtomicLong now = new AtomicLong();
  private CaffeineCacheBackend backend;

  @BeforeEach
  void setUp() {
    backend =
        new CaffeineCacheBackend(
            CacheConfig.BuildableCacheConfig.builder()
                .sizing(CacheSizing.builder().fixedSize(MemorySize.ofMega(8)).build())
                .clockNanos(now::get)
                .referenceTtl(Duration.ofSeconds(10))
                .referenceNegativeTtl(Duration.ofSeconds(2))
                .build(),
            Optional.empty());
  }

  @ParameterizedTest
  @CsvSource({"8192, 1024", "1024, 8192"})
  void replacementOwnsPayloadAndMetadata(int oldSize, int newSize) {
    var original =
        VersionedTestObj.builder()
            .id(42)
            .numParts(2)
            .createdAtMicros(123)
            .versionToken("old")
            .binary(new byte[oldSize])
            .build();
    var replacement =
        VersionedTestObj.builder()
            .id(42)
            .numParts(3)
            .createdAtMicros(456)
            .versionToken("new")
            .binary(new byte[newSize])
            .build();

    backend.putLocal("realm", original);
    backend.cache.cleanUp();
    var originalValue = backend.cache.asMap().values().iterator().next();
    var originalWeight = backend.currentWeightReported();

    backend.putLocal("realm", replacement);
    backend.cache.cleanUp();
    var entry = backend.cache.asMap().entrySet().iterator().next();
    // An equal-key replacement must not retain the old payload via the stored key.
    assertThat(entry.getKey()).isNotSameAs(originalValue);
    assertThat(backend.currentWeightReported() - originalWeight)
        .isEqualTo(entry.getValue().serialized().length - originalValue.serialized().length);
    var cached = backend.get("realm", objRef(original));
    assertThat(cached).isEqualTo(replacement);
    // Creation time is auxiliary and is not covered by object equality.
    assertThat(cached.createdAtMicros()).isEqualTo(replacement.createdAtMicros());
  }

  @Test
  void objectNegativeEntryCanBeReplaced() {
    var obj = ImmutableNegativeCachingObj.builder().id(42).build();
    backend.putLocal("realm", obj);
    backend.putNegative("realm", objRef(obj));
    assertThat(backend.get("realm", objRef(obj))).isSameAs(NOT_FOUND_OBJ_SENTINEL);
    backend.putLocal("realm", obj);
    assertThat(backend.get("realm", objRef(obj))).isEqualTo(obj);
  }

  @Test
  void objectReplacementUsesCurrentExpiry() {
    var original = ImmutableDynamicCachingObj.builder().id(42).thatExpireTimestamp(2).build();
    var replacement = ImmutableDynamicCachingObj.builder().id(42).thatExpireTimestamp(10).build();
    backend.putLocal("realm", original);
    backend.putLocal("realm", replacement);
    now.set(MICROSECONDS.toNanos(3));
    assertThat(backend.get("realm", objRef(original))).isEqualTo(replacement);
    now.set(MICROSECONDS.toNanos(10));
    assertThat(backend.get("realm", objRef(original))).isNull();
  }

  @Test
  void referenceReplacementUsesCurrentValueAndExpiry() {
    var original = Reference.builder().name("ref").previousPointers().createdAtMicros(123).build();
    var replacement =
        Reference.builder().name("ref").previousPointers().createdAtMicros(456).build();
    backend.putReferenceLocal("realm", original);
    now.set(Duration.ofSeconds(1).toNanos());
    backend.putReferenceLocal("realm", replacement);
    assertThat(backend.getReference("realm", "ref")).isEqualTo(replacement);
    backend.putReferenceNegative("realm", "ref");
    assertThat(backend.getReference("realm", "ref")).isSameAs(NON_EXISTENT_REFERENCE_SENTINEL);
    now.set(Duration.ofSeconds(3).toNanos());
    assertThat(backend.getReference("realm", "ref")).isNull();

    backend.putReferenceNegative("realm", "ref");
    backend.putReferenceLocal("realm", replacement);
    now.set(Duration.ofSeconds(6).toNanos());
    assertThat(backend.getReference("realm", "ref")).isEqualTo(replacement);
    now.set(Duration.ofSeconds(13).toNanos());
    assertThat(backend.getReference("realm", "ref")).isNull();
  }
}
