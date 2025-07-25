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
package org.apache.polaris.persistence.varint;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestVarInt {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void negative() {
    var buf = ByteBuffer.allocate(9);
    soft.assertThatIllegalArgumentException().isThrownBy(() -> VarInt.putVarInt(buf, -1L));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> VarInt.putVarInt(buf, Long.MIN_VALUE));
  }

  @ParameterizedTest
  @MethodSource
  public void varInt(long value, byte[] binary) {
    var buf = ByteBuffer.allocate(9);
    VarInt.putVarInt(buf, value);
    soft.assertThat(buf.position()).isEqualTo(binary.length);
    soft.assertThat(Arrays.copyOf(buf.array(), buf.position())).containsExactly(binary);
    soft.assertThat(VarInt.varIntLen(value)).isEqualTo(binary.length);

    var read = buf.duplicate().flip();
    VarInt.skipVarInt(read);
    soft.assertThat(read.position()).isEqualTo(binary.length);

    if (value > Integer.MAX_VALUE) {
      soft.assertThatIllegalArgumentException()
          .isThrownBy(() -> VarInt.readVarInt(buf.duplicate().flip()))
          .withMessageStartingWith("Out of range: ");
      soft.assertThat(VarInt.readVarLong(buf.duplicate().flip())).isEqualTo(value);
    } else {
      soft.assertThat(VarInt.readVarInt(buf.duplicate().flip())).isEqualTo(value);
      soft.assertThat(VarInt.readVarLong(buf.duplicate().flip())).isEqualTo(value);
    }
  }

  @Test
  public void notVarInt() {
    var buf = new byte[] {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> VarInt.readVarInt(ByteBuffer.wrap(buf)));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> VarInt.skipVarInt(ByteBuffer.wrap(buf)));
  }

  static Stream<Arguments> varInt() {
    return Stream.of(
        // one byte
        arguments(0L, new byte[] {0}),
        arguments(1L, new byte[] {1}),
        arguments(42L, new byte[] {42}),
        arguments(127L, new byte[] {127}),
        // 2 bytes
        arguments(128L, new byte[] {(byte) 0x80, 1}),
        // 21 bite -> 3 x 7 bits
        arguments(0x1fffff, new byte[] {-1, -1, 127}),
        // 28 bits -> 4 x 7 bits
        arguments(0xfffffff, new byte[] {-1, -1, -1, 127}),
        // 35 bits -> 5 x 7 bits
        arguments(0x7ffffffffL, new byte[] {-1, -1, -1, -1, 127}),
        arguments(0x321321321L, new byte[] {-95, -90, -56, -119, 50}),
        // 42 bits -> 6 x 7 bits
        arguments(0x3ffffffffffL, new byte[] {-1, -1, -1, -1, -1, 127}),
        // 49 bits -> 7 x 7 bits
        arguments(0x1ffffffffffffL, new byte[] {-1, -1, -1, -1, -1, -1, 127}),
        // 56 bits -> 8 x 7 bits
        arguments(0xffffffffffffffL, new byte[] {-1, -1, -1, -1, -1, -1, -1, 127}),
        arguments(0x32132132132132L, new byte[] {-78, -62, -52, -112, -109, -28, -124, 25}),
        // 63 bits -> 9 x 7 bits
        arguments(Long.MAX_VALUE, new byte[] {-1, -1, -1, -1, -1, -1, -1, -1, 127}),
        arguments(
            Long.MAX_VALUE - 0x1111111111111111L,
            new byte[] {-18, -35, -69, -9, -18, -35, -69, -9, 110}));
  }
}
