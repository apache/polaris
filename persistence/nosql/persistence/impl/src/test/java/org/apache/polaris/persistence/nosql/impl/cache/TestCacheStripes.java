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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.cache.CacheConfig;
import org.apache.polaris.persistence.nosql.api.cache.CacheSizing;
import org.apache.polaris.persistence.nosql.impl.indexes.ImmutableIndexStripeObj;
import org.apache.polaris.persistence.nosql.impl.indexes.IndexStripeObj;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

public class TestCacheStripes {
  private final CaffeineCacheBackend backend =
      new CaffeineCacheBackend(
          CacheConfig.BuildableCacheConfig.builder()
              .sizing(CacheSizing.builder().fixedSize(MemorySize.ofMega(8)).build())
              .build(),
          Optional.empty());

  @ParameterizedTest
  @MethodSource("buffers")
  void stripeSnapshotIsIndependent(ByteBuffer source, byte[] expected) {
    var position = source.position();
    var limit = source.limit();
    var stripe = IndexStripeObj.indexStripeObj(42, source);
    backend.putLocal("realm", stripe);
    assertThat(source.position()).isEqualTo(position);
    assertThat(source.limit()).isEqualTo(limit);
    if (!source.isReadOnly() && source.hasRemaining()) {
      source.put(position, (byte) 99);
    }

    var first = (IndexStripeObj) backend.get("realm", objRef(stripe));
    assertThat(first.index()).isEqualTo(ByteBuffer.wrap(expected));
    // Preserve the existing writable result behavior, without exposing cached bytes.
    assertThat(first.index().isReadOnly()).isFalse();
    if (first.index().hasRemaining()) {
      first.index().put(0, (byte) 99);
      first.index().position(first.index().limit());
    }
    var second = (IndexStripeObj) backend.get("realm", objRef(stripe));
    assertThat(second.index()).isEqualTo(ByteBuffer.wrap(expected));
    assertThat(second.index()).isNotSameAs(first.index());
  }

  static Stream<Arguments> buffers() {
    return Stream.of(0, 256)
        .flatMap(size -> {
          byte[] expected = new byte[size];
          for (int i = 0; i < size; i++) {
            expected[i] = (byte) i;
          }
          var heap = ByteBuffer.wrap(expected.clone());
          var slice = ByteBuffer.allocate(size + 32).position(5).slice();
          slice.position(7).put(expected).flip().position(7);
          var readOnly = ByteBuffer.allocate(size + 32).position(5).slice();
          readOnly.position(7).put(expected).flip().position(7);
          var direct = ByteBuffer.allocateDirect(size + 32);
          direct.position(7).put(expected).flip().position(7);
          var directReadOnly = ByteBuffer.allocateDirect(size + 32);
          directReadOnly.position(7).put(expected).flip().position(7);
          return Stream.of(
              arguments(heap, expected),
              arguments(slice, expected),
              arguments(readOnly.asReadOnlyBuffer(), expected),
              arguments(direct, expected),
              arguments(directReadOnly.asReadOnlyBuffer(), expected));
        });
  }

  @ParameterizedTest
  @CsvSource({"1024, 8192", "8192, 1024"})
  void stripeReplacementUpdatesMetadataAndWeight(int oldSize, int newSize) {
    var original =
        ImmutableIndexStripeObj.builder()
            .id(42)
            .numParts(2)
            .createdAtMicros(123)
            .versionToken("old")
            .index(ByteBuffer.allocate(oldSize))
            .build();
    var replacement =
        ImmutableIndexStripeObj.builder()
            .id(42)
            .numParts(3)
            .createdAtMicros(456)
            .versionToken("new")
            .index(ByteBuffer.allocate(newSize))
            .build();
    backend.putLocal("realm", original);
    backend.cache.cleanUp();
    var weight = backend.currentWeightReported();
    assertThat(weight).isGreaterThan(oldSize);
    backend.putLocal("realm", replacement);
    backend.cache.cleanUp();
    assertThat(backend.currentWeightReported() - weight).isEqualTo((long) newSize - oldSize);
    var cached = (IndexStripeObj) backend.get("realm", objRef(original));
    assertThat(cached).isEqualTo(replacement);
    assertThat(cached.createdAtMicros()).isEqualTo(456);
    assertThat(cached.numParts()).isEqualTo(3);
    assertThat(cached.versionToken()).isEqualTo("new");
    assertThat(backend.get("other-realm", objRef(original))).isNull();

    // Stripes use the default disabled negative caching: a negative put invalidates.
    backend.putNegative("realm", objRef(original));
    assertThat(backend.get("realm", objRef(original))).isNull();
    backend.putLocal("realm", replacement);
    assertThat(backend.get("realm", objRef(original))).isEqualTo(replacement);
  }
}
