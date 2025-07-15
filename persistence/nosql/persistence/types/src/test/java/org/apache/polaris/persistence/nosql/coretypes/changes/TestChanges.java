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

import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
        ChangeUpdate.builder().build());
  }
}
