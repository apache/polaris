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
package org.apache.polaris.persistence.nosql.api.index;

import static org.apache.polaris.persistence.nosql.api.index.IndexKey.INDEX_KEY_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class TestIndexKeyBuffers {
  private static final byte[] KEY = {'a', 1, 2, 0, (byte) 0xff};
  private static final byte[] ENCODED = {'a', 2, 3, 2, 4, 0, (byte) 0xff, 1};

  enum BufferType {
    HEAP,
    SLICED,
    DIRECT,
    READ_ONLY,
    READ_ONLY_SLICED;

    ByteBuffer buffer(byte[] bytes) {
      var buffer =
          this == DIRECT
              ? ByteBuffer.allocateDirect(bytes.length + 16)
              : ByteBuffer.allocate(bytes.length + 16);
      if (this == SLICED || this == READ_ONLY_SLICED) {
        buffer = buffer.position(5).slice();
      }
      buffer.position(3).put(bytes).limit(3 + bytes.length).position(3);
      return this == READ_ONLY || this == READ_ONLY_SLICED ? buffer.asReadOnlyBuffer() : buffer;
    }
  }

  @ParameterizedTest
  @EnumSource(BufferType.class)
  void decodeAndSkipConsumeOneKey(BufferType type) {
    var bytes = Arrays.copyOf(ENCODED, ENCODED.length + 1);
    bytes[ENCODED.length] = 99;
    var buffer = type.buffer(bytes);
    var limit = buffer.limit();
    assertThat(IndexKey.deserializeKey(buffer)).isEqualTo(key(ByteBuffer.wrap(KEY)));
    assertThat(buffer.position()).isEqualTo(3 + ENCODED.length);
    assertThat(buffer.limit()).isEqualTo(limit);
    assertThat(buffer.get()).isEqualTo((byte) 99);
    buffer.position(3);
    IndexKey.skip(buffer);
    assertThat(buffer.position()).isEqualTo(3 + ENCODED.length);
    assertThat(buffer.limit()).isEqualTo(limit);
  }

  @ParameterizedTest
  @EnumSource(BufferType.class)
  void unescapedKeysAndNullConsumeOneKey(BufferType type) {
    for (var indexKey : new IndexKey[] {null, key(""), key("a\u0000münchen\u0000table"), key(0L)}) {
      var wire = ByteBuffer.allocate(INDEX_KEY_SERIALIZER.serializedSize(indexKey));
      INDEX_KEY_SERIALIZER.serialize(indexKey, wire);
      var buffer = type.buffer(wire.array());
      assertThat(IndexKey.deserializeKey(buffer)).isEqualTo(indexKey);
      assertThat(buffer.position()).isEqualTo(buffer.limit());
      buffer.position(3);
      IndexKey.skip(buffer);
      assertThat(buffer.position()).isEqualTo(buffer.limit());
    }
  }

  @ParameterizedTest
  @EnumSource(BufferType.class)
  void allByteValuesRoundTrip(BufferType type) {
    var bytes = new byte[256];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) i;
    }
    var indexKey = key(ByteBuffer.wrap(bytes));
    var wire = ByteBuffer.allocate(indexKey.serializedSize());
    indexKey.serialize(wire);
    var buffer = type.buffer(wire.array());
    assertThat(IndexKey.deserializeKey(buffer)).isEqualTo(indexKey);
    assertThat(buffer.remaining()).isZero();
    buffer.position(3);
    IndexKey.skip(buffer);
    assertThat(buffer.remaining()).isZero();
  }

  @ParameterizedTest
  @EnumSource(BufferType.class)
  void decodedLengthBoundary(BufferType type) {
    for (byte value : new byte[] {'a', 1, 2}) {
      var bytes = new byte[IndexKey.MAX_LENGTH];
      Arrays.fill(bytes, value);
      var indexKey = key(ByteBuffer.wrap(bytes));
      var wire = ByteBuffer.allocate(indexKey.serializedSize());
      indexKey.serialize(wire);
      var buffer = type.buffer(wire.array());
      assertThat(IndexKey.deserializeKey(buffer)).isEqualTo(indexKey);
      assertThat(buffer.remaining()).isZero();
      buffer.position(3);
      IndexKey.skip(buffer);
      assertThat(buffer.remaining()).isZero();
    }
  }

  @ParameterizedTest
  @EnumSource(BufferType.class)
  void unescapedPrefixWithFinalEscapeAtLengthBoundary(BufferType type) {
    var bytes = new byte[IndexKey.MAX_LENGTH];
    Arrays.fill(bytes, (byte) 'a');
    bytes[bytes.length - 1] = 1;
    var indexKey = key(ByteBuffer.wrap(bytes));
    var wire = ByteBuffer.allocate(indexKey.serializedSize());
    indexKey.serialize(wire);
    var buffer = type.buffer(wire.array());

    assertThat(IndexKey.deserializeKey(buffer)).isEqualTo(indexKey);
    assertThat(buffer.remaining()).isZero();
    buffer.position(3);
    IndexKey.skip(buffer);
    assertThat(buffer.remaining()).isZero();
  }

  @ParameterizedTest
  @MethodSource("malformedKeys")
  void malformedInputPreservesConsumedPosition(
      BufferType type, byte[] wire, Class<? extends Exception> exception) {
    for (boolean skip : new boolean[] {false, true}) {
      var buffer = type.buffer(wire);
      assertThatThrownBy(
              () -> {
                if (skip) {
                  IndexKey.skip(buffer);
                } else {
                  IndexKey.deserializeKey(buffer);
                }
              })
          .isInstanceOf(exception);
      assertThat(buffer.position()).isEqualTo(buffer.limit());
    }
  }

  static Stream<Arguments> malformedKeys() {
    return Arrays.stream(BufferType.values())
        .flatMap(type ->
            Stream.of(
                arguments(type, new byte[0], BufferUnderflowException.class),
                arguments(type, new byte[] {'a'}, BufferUnderflowException.class),
                arguments(type, new byte[] {2}, BufferUnderflowException.class),
                arguments(type, new byte[] {2, 3}, BufferUnderflowException.class),
                arguments(type, new byte[] {2, 4}, BufferUnderflowException.class),
                arguments(type, new byte[] {'a', 2, 3}, BufferUnderflowException.class),
                arguments(type, new byte[] {2, 0}, IllegalArgumentException.class),
                arguments(type, new byte[] {'a', 2, 5}, IllegalStateException.class),
                arguments(type, tooLong(false), IllegalArgumentException.class),
                arguments(type, tooLong(true), IllegalArgumentException.class)));
  }

  private static byte[] tooLong(boolean escaped) {
    var bytes = new byte[(IndexKey.MAX_LENGTH + 1) * (escaped ? 2 : 1)];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = escaped ? (byte) (i % 2 == 0 ? 2 : 3) : (byte) 'a';
    }
    return bytes;
  }

  @ParameterizedTest
  @EnumSource(
      value = BufferType.class,
      names = {"HEAP", "SLICED", "DIRECT"})
  void serializationHonorsEveryCapacityBoundary(BufferType type) {
    var indexKey = key(ByteBuffer.wrap(KEY));
    for (int capacity = 0; capacity <= ENCODED.length + 1; capacity++) {
      var buffer = type.buffer(new byte[capacity + 1]);
      buffer.limit(3 + capacity);
      var end = buffer.limit();
      indexKey.serializeNoFail(buffer);
      assertPrefix(buffer, capacity);
      assertThat(buffer.duplicate().clear().get(end)).isZero();
      buffer.limit(end).position(3);
      if (capacity < ENCODED.length) {
        assertThatThrownBy(() -> indexKey.serialize(buffer))
            .isInstanceOf(BufferOverflowException.class);
      } else {
        assertThat(indexKey.serialize(buffer)).isSameAs(buffer);
      }
      assertPrefix(buffer, capacity);
      assertThat(buffer.duplicate().clear().get(end)).isZero();
    }
  }

  private static void assertPrefix(ByteBuffer buffer, int capacity) {
    var length = Math.min(capacity, ENCODED.length);
    assertThat(buffer.position()).isEqualTo(3 + length);
    assertThat(buffer.limit()).isEqualTo(3 + capacity);
    var bytes = new byte[length];
    buffer.get(3, bytes);
    assertThat(bytes).containsExactly(Arrays.copyOf(ENCODED, length));
  }

  @ParameterizedTest
  @EnumSource(
      value = BufferType.class,
      names = {"READ_ONLY", "READ_ONLY_SLICED"})
  void serializationRetainsReadOnlyBehavior(BufferType type) {
    var indexKey = key("a");
    var buffer = type.buffer(new byte[3]);
    assertThatThrownBy(() -> indexKey.serialize(buffer))
        .isInstanceOf(ReadOnlyBufferException.class);
    assertThatThrownBy(() -> indexKey.serializeNoFail(buffer))
        .isInstanceOf(ReadOnlyBufferException.class);
    assertThat(buffer.position()).isEqualTo(3);
    buffer.limit(3);
    indexKey.serializeNoFail(buffer);
    assertThatThrownBy(() -> indexKey.serialize(buffer))
        .isInstanceOf(ReadOnlyBufferException.class);
  }
}
