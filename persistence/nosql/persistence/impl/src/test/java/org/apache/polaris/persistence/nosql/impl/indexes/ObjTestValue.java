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
package org.apache.polaris.persistence.nosql.impl.indexes;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.varint.VarInt;

final class ObjTestValue {
  private final byte[] bytes;

  public ObjTestValue(String idHex) {
    this.bytes = HexFormat.of().parseHex(idHex);
  }

  static ObjTestValue objTestValueFromString(String idHex) {
    return new ObjTestValue(idHex);
  }

  static ObjTestValue objTestValueOfSize(int size) {
    return new ObjTestValue(
        IntStream.range(0, size).mapToObj(i -> "10").collect(Collectors.joining()));
  }

  @Override
  public String toString() {
    return HexFormat.of().formatHex(bytes);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;

    ObjTestValue objTestValue = (ObjTestValue) o;
    return Arrays.equals(bytes, objTestValue.bytes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  static final IndexValueSerializer<ObjTestValue> OBJ_TEST_SERIALIZER =
      new IndexValueSerializer<>() {
        @Override
        public void skip(@Nonnull ByteBuffer buffer) {
          var len = VarInt.readVarInt(buffer);
          if (len > 0) {
            buffer.position(buffer.position() + len);
          }
        }

        @Override
        @Nullable
        public ObjTestValue deserialize(@Nonnull ByteBuffer buffer) {
          var len = VarInt.readVarInt(buffer);
          if (len == 0) {
            return null;
          }
          var bytes = new byte[len];
          buffer.get(bytes);
          return new ObjTestValue(HexFormat.of().formatHex(bytes));
        }

        @Override
        @Nonnull
        public ByteBuffer serialize(@Nullable ObjTestValue value, @Nonnull ByteBuffer target) {
          if (value == null) {
            return target.put((byte) 0);
          }
          return VarInt.putVarInt(target, value.bytes.length).put(value.bytes);
        }

        @Override
        public int serializedSize(@Nullable ObjTestValue value) {
          if (value == null) {
            return 1;
          }
          return VarInt.varIntLen(value.bytes.length) + value.bytes.length;
        }
      };
}
