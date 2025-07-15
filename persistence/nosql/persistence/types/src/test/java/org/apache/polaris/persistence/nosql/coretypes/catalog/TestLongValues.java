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
import static java.lang.Long.MIN_VALUE;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.LongValues.LONG_VALUES_SERIALIZER;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestLongValues {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void longValuesSerialization(LongValues longValues) {
    var serSize = LONG_VALUES_SERIALIZER.serializedSize(longValues);
    var buffer = ByteBuffer.allocate(serSize + 10);
    LONG_VALUES_SERIALIZER.serialize(longValues, buffer);
    soft.assertThat(buffer)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(serSize, 10);
    buffer.put(new byte[10]);
    soft.assertThat(buffer)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(serSize + 10, 0);

    buffer.flip();

    soft.assertThat(buffer)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(0, serSize + 10);

    var skip = buffer.duplicate();
    LONG_VALUES_SERIALIZER.skip(skip);
    soft.assertThat(skip)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(serSize, 10);

    var deser = buffer.duplicate();
    var deserialized = LONG_VALUES_SERIALIZER.deserialize(deser);
    soft.assertThat(deser)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(serSize, 10);
    soft.assertThat(deserialized)
        .isEqualTo(longValues == null || longValues.entityIds().isEmpty() ? null : longValues);
  }

  static Stream<LongValues> longValuesSerialization() {
    return Stream.of(
        null,
        LongValues.longValues(Set.of()),
        LongValues.longValues(Set.of(0L)),
        LongValues.longValues(Set.of(1L)),
        LongValues.longValues(Set.of(1L, 2L, 3L)),
        LongValues.longValues(LongStream.range(0, 10000).boxed().collect(Collectors.toSet())),
        LongValues.longValues(
            LongStream.range(MIN_VALUE, MIN_VALUE + 1000).boxed().collect(Collectors.toSet())),
        LongValues.longValues(
            LongStream.range(MAX_VALUE - 1000, MAX_VALUE).boxed().collect(Collectors.toSet())));
  }
}
