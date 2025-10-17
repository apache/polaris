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
package org.apache.polaris.persistence.nosql.api.backend;

import static java.lang.String.format;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Stream;
import org.apache.polaris.persistence.varint.VarInt;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestPersistId {
  @InjectSoftAssertions SoftAssertions soft;

  protected ObjectMapper mapper;
  protected ObjectMapper smile;

  @BeforeEach
  protected void setUp() {
    mapper = new ObjectMapper();
    smile = new SmileMapper();
  }

  @Test
  public void invalidRepresentations() {
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> PersistId.fromBytes(new byte[] {(byte) 0x3}))
        .withMessage("Unsupported PersistId type: 3");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> PersistId.fromBytes(new byte[] {(byte) 0x0}))
        .withMessage("Unsupported PersistId type: 0");
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> PersistId.persistId(0L, -1))
        .withMessage("part must not be negative");
  }

  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("ByteBufferBackingArray")
  public void serDe(long id, int part, ByteBuffer expected) throws Exception {
    var persistId = PersistId.persistId(id, part);

    var expectedSerializedSize = 1 + Long.BYTES + (part > 0 ? VarInt.varIntLen(part) : 0);

    // ser/deser using byte[]

    var bytes = persistId.toBytes();
    soft.assertThat(bytes).hasSize(expectedSerializedSize).hasSize(expected.remaining());
    soft.assertThat(bytes).containsExactly(Arrays.copyOf(expected.array(), expected.remaining()));

    var deser = PersistId.fromBytes(bytes);
    soft.assertThat(deser).extracting(PersistId::id, PersistId::part).containsExactly(id, part);

    var reser = deser.toBytes();
    soft.assertThat(reser).containsExactly(bytes);

    // JSON serialization

    var serializedJson = mapper.writerFor(PersistId.class).writeValueAsString(persistId);
    var base64 = Base64.getEncoder().encodeToString(bytes);
    soft.assertThat(serializedJson).isEqualTo(format("\"%s\"", base64));
    soft.assertThat(mapper.readValue(serializedJson, PersistId.class))
        .extracting(PersistId::id, PersistId::part)
        .containsExactly(id, part);

    // Smile serialization

    var serializedSmile = smile.writerFor(PersistId.class).writeValueAsBytes(persistId);
    soft.assertThat(smile.readValue(serializedSmile, PersistId.class))
        .extracting(PersistId::id, PersistId::part)
        .containsExactly(id, part);
  }

  static Stream<Arguments> serDe() {
    return Stream.of(
        arguments(0L, 0, ByteBuffer.allocate(50).put((byte) 1).putLong(0L).flip()),
        arguments(42L, 0, ByteBuffer.allocate(50).put((byte) 1).putLong(42L).flip()),
        arguments(
            Long.MIN_VALUE,
            0,
            ByteBuffer.allocate(50).put((byte) 1).putLong(Long.MIN_VALUE).flip()),
        arguments(
            Long.MAX_VALUE,
            0,
            ByteBuffer.allocate(50).put((byte) 1).putLong(Long.MAX_VALUE).flip()),
        arguments(
            0L, 1, VarInt.putVarInt(ByteBuffer.allocate(50).put((byte) 2).putLong(0L), 1).flip()),
        arguments(
            42L, 1, VarInt.putVarInt(ByteBuffer.allocate(50).put((byte) 2).putLong(42L), 1).flip()),
        arguments(
            Long.MIN_VALUE,
            666,
            VarInt.putVarInt(ByteBuffer.allocate(50).put((byte) 2).putLong(Long.MIN_VALUE), 666)
                .flip()),
        arguments(
            Long.MAX_VALUE,
            42,
            VarInt.putVarInt(ByteBuffer.allocate(50).put((byte) 2).putLong(Long.MAX_VALUE), 42)
                .flip()));
  }
}
