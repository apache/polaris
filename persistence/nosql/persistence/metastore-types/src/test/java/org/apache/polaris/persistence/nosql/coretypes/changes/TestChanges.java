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

package org.apache.polaris.persistence.nosql.coretypes.changes;

import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.coretypes.changes.Change.CHANGE_SERIALIZER;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestChanges {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void changeSerialization(Change mapping) {
    var serSize = CHANGE_SERIALIZER.serializedSize(mapping);
    var buffer = ByteBuffer.allocate(serSize + 10);
    CHANGE_SERIALIZER.serialize(mapping, buffer);
    soft.assertThat(buffer)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(serSize, 10);
    buffer.put(new byte[10]);
    soft.assertThat(buffer)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(serSize + 10, 0);

    buffer.flip();
    var nullProbe = buffer.duplicate();
    soft.assertThat(CHANGE_SERIALIZER.isNullSerialized(nullProbe)).isEqualTo(mapping == null);
    soft.assertThat(nullProbe)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(0, serSize + 10);

    soft.assertThat(buffer)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(0, serSize + 10);

    var skip = buffer.duplicate();
    CHANGE_SERIALIZER.skip(skip);
    soft.assertThat(skip)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(serSize, 10);

    var deser = buffer.duplicate();
    var deserialized = CHANGE_SERIALIZER.deserialize(deser);
    soft.assertThat(deser)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(serSize, 10);
    soft.assertThat(deserialized).isEqualTo(mapping);
  }

  static Stream<Change> changeSerialization() {
    return Stream.of(
        null,
        ChangeAdd.builder().build(),
        ChangeRemove.builder().build(),
        ChangeRename.builder().renameFrom(key("foo")).build(),
        ChangeRename.builder().renameFrom(key(42L)).build(),
        ChangeUpdate.builder().build(),
        ImmutableGenericChange.of(ChangeType.valueOf("future"), Map.of("value", "payload")));
  }

  @ParameterizedTest
  @MethodSource("changeSerialization")
  void encodedBytesMatchMapper(Change change) {
    assertEncodedBytesMatchMapper(change);
  }

  @Test
  void knownTypeOnGenericChangeRetainsAttributes() {
    assertEncodedBytesMatchMapper(
        ImmutableGenericChange.of(ChangeType.ADD, Map.of("extra", "preserve me")));
  }

  private void assertEncodedBytesMatchMapper(Change change) {
    var expected = ChangeSerializer.MAPPER.writeValueAsBytes(change);
    var buffer = ByteBuffer.allocate(CHANGE_SERIALIZER.serializedSize(change));
    CHANGE_SERIALIZER.serialize(change, buffer).flip();
    soft.assertThat(readVarInt(buffer)).isEqualTo(expected.length);
    var actual = new byte[buffer.remaining()];
    buffer.get(actual);
    soft.assertThat(actual).containsExactly(expected);
  }

  @ParameterizedTest
  @ValueSource(strings = {"HEAP", "SLICED", "READ_ONLY", "DIRECT"})
  void nullProbePreservesBufferState(String kind) {
    var buffer = kind.equals("DIRECT") ? ByteBuffer.allocateDirect(128) : ByteBuffer.allocate(128);
    if (kind.equals("SLICED")) {
      buffer = buffer.position(7).slice();
    }
    buffer.position(3);
    CHANGE_SERIALIZER.serialize(null, buffer).put((byte) 42).flip().position(3);
    if (kind.equals("READ_ONLY")) {
      buffer = buffer.asReadOnlyBuffer();
    }
    var position = buffer.position();
    var limit = buffer.limit();
    soft.assertThat(CHANGE_SERIALIZER.isNullSerialized(buffer)).isTrue();
    soft.assertThat(buffer.position()).isEqualTo(position);
    soft.assertThat(buffer.limit()).isEqualTo(limit);
    CHANGE_SERIALIZER.skip(buffer);
    soft.assertThat(buffer.get()).isEqualTo((byte) 42);
  }

  @Test
  void nullProbeAcceptsOverlongLength() {
    var bytes = ChangeSerializer.MAPPER.writeValueAsBytes(null);
    soft.assertThat(bytes.length).isLessThan(128);
    var buffer = ByteBuffer.allocate(128).put((byte) (bytes.length | 0x80)).put((byte) 0);
    buffer.put(bytes).put((byte) 42).flip();
    soft.assertThat(CHANGE_SERIALIZER.isNullSerialized(buffer)).isTrue();
    soft.assertThat(buffer.position()).isZero();
    soft.assertThat(CHANGE_SERIALIZER.deserialize(buffer)).isNull();
    soft.assertThat(buffer.get()).isEqualTo((byte) 42);
  }

  @Test
  void nullProbeComparesEntirePayload() {
    var encoded = ByteBuffer.allocate(128);
    CHANGE_SERIALIZER.serialize(null, encoded).flip();
    var length = readVarInt(encoded);
    var payloadOffset = encoded.position();
    for (int i = 0; i < length; i++) {
      var index = payloadOffset + i;
      var original = encoded.get(index);
      encoded.put(index, (byte) (original ^ 1)).position(0);
      soft.assertThat(CHANGE_SERIALIZER.isNullSerialized(encoded)).isFalse();
      soft.assertThat(encoded.position()).isZero();
      encoded.put(index, original);
    }
  }

  @Test
  void serializeNullWritesToTarget() {
    var target = ByteBuffer.allocate(256);
    var returned = CHANGE_SERIALIZER.serialize(null, target);

    soft.assertThat(returned).isSameAs(target);
    soft.assertThat(target.position()).isGreaterThan(0);

    target.flip();
    var deserialized = CHANGE_SERIALIZER.deserialize(target);
    soft.assertThat(deserialized).isNull();
  }
}
