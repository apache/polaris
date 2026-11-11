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

package org.apache.polaris.persistence.nosql.coretypes.catalog;

import static java.lang.Long.MAX_VALUE;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.EntityIdSet.ENTITY_ID_SET_SERIALIZER;
import static org.apache.polaris.persistence.varint.VarInt.putVarInt;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
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
public class TestEntityIdSet {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void negativeIds() {
    soft.assertThatIllegalStateException().isThrownBy(() -> EntityIdSet.entityIdSet(Set.of(-1L)));
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> EntityIdSet.entityIdSet(Set.of(Long.MIN_VALUE, 0L)));
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> EntityIdSet.entityIdSet(Set.of(0L, 1L, -1L)));
  }

  @ParameterizedTest
  @MethodSource
  public void entityIdSetSerialization(EntityIdSet objIds) {
    var serSize = ENTITY_ID_SET_SERIALIZER.serializedSize(objIds);
    var buffer = ByteBuffer.allocate(serSize + 10);
    ENTITY_ID_SET_SERIALIZER.serialize(objIds, buffer);
    soft.assertThat(buffer)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(serSize, 10);
    buffer.put(new byte[10]);
    soft.assertThat(buffer)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(serSize + 10, 0);

    buffer.flip();
    var nullProbe = buffer.duplicate();
    soft.assertThat(ENTITY_ID_SET_SERIALIZER.isNullSerialized(nullProbe))
        .isEqualTo(objIds == null || objIds.entityIds().isEmpty());
    soft.assertThat(nullProbe)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(0, serSize + 10);

    soft.assertThat(buffer)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(0, serSize + 10);

    var skip = buffer.duplicate();
    ENTITY_ID_SET_SERIALIZER.skip(skip);
    soft.assertThat(skip)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(serSize, 10);

    var deser = buffer.duplicate();
    var deserialized = ENTITY_ID_SET_SERIALIZER.deserialize(deser);
    soft.assertThat(deser)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(serSize, 10);
    soft.assertThat(deserialized)
        .isEqualTo(objIds == null || objIds.entityIds().isEmpty() ? null : objIds);
  }

  static Stream<EntityIdSet> entityIdSetSerialization() {
    return Stream.of(
        null,
        EntityIdSet.entityIdSet(Set.of()),
        EntityIdSet.entityIdSet(Set.of(0L)),
        EntityIdSet.entityIdSet(Set.of(1L)),
        EntityIdSet.entityIdSet(Set.of(1L, 2L, 3L)),
        EntityIdSet.entityIdSet(LongStream.range(0, 10000).boxed().collect(Collectors.toSet())),
        EntityIdSet.entityIdSet(
            LongStream.range(MAX_VALUE - 1000, MAX_VALUE).boxed().collect(Collectors.toSet())));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 16, 1024, 2048})
  void deserializeDeduplicatesIds(int count) {
    var buffer = putVarInt(ByteBuffer.allocate(count * 5 + 16), count);
    for (int i = 0; i < count; i++) {
      putVarInt(buffer, 123456789L);
    }
    buffer.put((byte) 42).flip();
    var value = ENTITY_ID_SET_SERIALIZER.deserialize(buffer);
    soft.assertThat(value).isEqualTo(EntityIdSet.entityIdSet(Set.of(123456789L)));
    soft.assertThat(buffer.get()).isEqualTo((byte) 42);
  }

  @Test
  void truncatedLargeCountDoesNotReserveUnboundedCapacity() {
    var buffer = putVarInt(ByteBuffer.allocate(16), Integer.MAX_VALUE).flip();
    var limit = buffer.limit();
    soft.assertThatThrownBy(() -> ENTITY_ID_SET_SERIALIZER.deserialize(buffer))
        .isInstanceOf(BufferUnderflowException.class);
    soft.assertThat(buffer.position()).isEqualTo(limit);
  }

  /**
   * Verifies that {@code serialize(null, target)} writes the null marker directly into {@code
   * target} and does not return a different buffer.
   */
  @Test
  void serializeNullWritesToTarget() {
    var target = ByteBuffer.allocate(64);
    var returned = ENTITY_ID_SET_SERIALIZER.serialize(null, target);

    // Must return the same target buffer
    soft.assertThat(returned).isSameAs(target);
    // Must have advanced the position
    soft.assertThat(target.position()).isGreaterThan(0);

    // Verify that the bytes written to target actually round-trip via deserialize
    target.flip();
    var deserialized = ENTITY_ID_SET_SERIALIZER.deserialize(target);
    soft.assertThat(deserialized).isNull();
  }
}
