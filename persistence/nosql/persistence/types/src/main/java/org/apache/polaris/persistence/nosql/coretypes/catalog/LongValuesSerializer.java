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

import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.readVarLong;
import static org.apache.polaris.persistence.varint.VarInt.skipVarInt;
import static org.apache.polaris.persistence.varint.VarInt.varIntLen;

import com.google.common.primitives.Ints;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;

final class LongValuesSerializer implements IndexValueSerializer<LongValues> {
  private static final byte[] NULL;

  private static final long DIRECT_FLAG = 1L << 31;

  static {
    var buffer = putVarInt(ByteBuffer.allocate(1), 0).flip();
    NULL = new byte[buffer.remaining()];
    buffer.get(NULL);
  }

  @Nonnull
  @Override
  public ByteBuffer serialize(@Nullable LongValues value, @Nonnull ByteBuffer target) {
    if (value == null) {
      return target.put(NULL);
    }
    var longs = value.entityIds();
    var num = longs.size();
    if (!value.notVarInt()) {
      putVarInt(target, num);
      for (var l : longs) {
        putVarInt(target, l);
      }
    } else {
      putVarInt(target, DIRECT_FLAG | ((long) num));
      for (var l : longs) {
        target.putLong(l);
      }
    }
    return target;
  }

  @Override
  public int serializedSize(@Nullable LongValues value) {
    if (value == null || value.entityIds().isEmpty()) {
      return NULL.length;
    }
    var longs = value.entityIds();

    var num = longs.size();

    if (!value.notVarInt()) {
      var size = varIntLen(num);
      for (var l : longs) {
        size += varIntLen(l);
      }
      return size;
    }

    var size = varIntLen(DIRECT_FLAG | ((long) num));
    size += num * Long.BYTES;
    return size;
  }

  @Nullable
  @Override
  public LongValues deserialize(@Nonnull ByteBuffer buffer) {
    var num = readVarLong(buffer);
    if (num == 0) {
      return null;
    }

    var b = ImmutableLongValues.builder();
    if ((num & DIRECT_FLAG) == 0) {
      for (int i = 0; i < num; i++) {
        b.addEntityId(readVarLong(buffer));
      }
    } else {
      var realNum = (int) (num & ~DIRECT_FLAG);
      for (int i = 0; i < realNum; i++) {
        b.addEntityId(buffer.getLong());
      }
    }
    return b.build();
  }

  @Override
  public void skip(@Nonnull ByteBuffer buffer) {
    var num = readVarLong(buffer);
    if ((num & DIRECT_FLAG) == 0) {
      for (int i = 0; i < num; i++) {
        skipVarInt(buffer);
      }
    } else {
      var realNum = (int) (num & ~DIRECT_FLAG);
      buffer.position(buffer.position() + Ints.checkedCast(realNum) * Long.BYTES);
    }
  }
}
