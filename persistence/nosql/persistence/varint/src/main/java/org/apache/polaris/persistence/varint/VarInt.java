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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.primitives.Ints;
import java.nio.ByteBuffer;

/** Utility class to de-serialize <em>positive</em> integer values in a space efficient way. */
public final class VarInt {
  // var-int encoded length of Long.MAX_VALUE
  private static final int MAX_LEN = 9;
  // 7 bits
  private static final int MAX_SHIFT_LEN = 7 * MAX_LEN;

  private VarInt() {}

  public static int varIntLen(long v) {
    checkArgument(v >= 0);
    int l = 0;
    while (true) {
      l++;
      if (v <= 0x7f) {
        return l;
      }
      v >>= 7;
    }
  }

  public static ByteBuffer putVarInt(ByteBuffer b, long v) {
    checkArgument(v >= 0);
    while (true) {
      if (v <= 0x7f) {
        // Current "byte" is <= 0x7f - encode as is. The highest bit (0x80) is not set, meaning that
        // this is the last encoded byte value.
        return b.put((byte) v);
      }

      // Current value is > 0x7f - encode its lower 7 bits and set the "more data follows" flag
      // (0x80).
      b.put((byte) (v | 0x80));

      v >>= 7;
    }
  }

  public static int readVarInt(ByteBuffer b) {
    return Ints.checkedCast(readVarLong(b));
  }

  public static long readVarLong(ByteBuffer b) {
    long r = 0;
    for (int shift = 0; ; shift += 7) {
      checkArgument(shift < MAX_SHIFT_LEN, "Illegal variable length integer representation");
      long v = b.get() & 0xff;
      r |= (v & 0x7f) << shift;
      if ((v & 0x80) == 0) {
        break;
      }
    }
    return r;
  }

  public static void skipVarInt(ByteBuffer b) {
    for (int shift = 0; ; shift += 7) {
      checkArgument(shift < MAX_SHIFT_LEN, "Illegal variable length integer representation");
      int v = b.get() & 0xff;
      if ((v & 0x80) == 0) {
        break;
      }
    }
  }
}
