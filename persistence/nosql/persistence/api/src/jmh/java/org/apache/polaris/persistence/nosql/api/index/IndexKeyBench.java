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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.INDEX_KEY_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.varint.VarInt.putVarInt;

import java.nio.ByteBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(NANOSECONDS)
public class IndexKeyBench {
  private static final int KEY_COUNT = 64;

  @State(Scope.Thread)
  public static class Keys {
    @Param({"NAME", "NAMESPACE", "LONG", "ESCAPED_LONG", "POLICY"})
    public String shape;

    @Param({"HEAP", "SLICED"})
    public String bufferType;

    private final IndexKey[] keys = new IndexKey[KEY_COUNT];
    private final ByteBuffer[] encoded = new ByteBuffer[KEY_COUNT];
    private ByteBuffer target;
    private int index;

    @Setup
    public void setup() {
      for (int i = 0; i < KEY_COUNT; i++) {
        keys[i] =
            switch (shape) {
              case "NAME" -> key("table_" + i);
              case "NAMESPACE" -> key("warehouse\u0000analytics\u0000münchen\u0000events_" + i);
              case "LONG" -> key(0x2040506070809000L + ((long) i << 8));
              case "ESCAPED_LONG" -> key(0x0102040506070000L + i);
              case "POLICY" -> {
                var buffer = ByteBuffer.allocate(48).put((byte) 'E');
                putVarInt(buffer, 0x123456789L + i);
                putVarInt(buffer, 0x123456789abL + i);
                putVarInt(buffer, 1);
                putVarInt(buffer, 0x123456789L);
                putVarInt(buffer, 0x23456789abcL + i);
                yield key(buffer.flip());
              }
              default -> throw new IllegalArgumentException(shape);
            };
        encoded[i] = buffer(keys[i].serializedSize());
        keys[i].serialize(encoded[i]).flip();
      }
      target = buffer(1024);
    }

    private ByteBuffer buffer(int length) {
      if (bufferType.equals("SLICED")) {
        return ByteBuffer.allocate(length + 16).position(7).limit(7 + length).slice();
      }
      return ByteBuffer.allocate(length);
    }

    private int next() {
      return index++ & (KEY_COUNT - 1);
    }
  }

  @State(Scope.Thread)
  public static class NullKey {
    private ByteBuffer encoded;

    @Setup
    public void setup() {
      encoded = ByteBuffer.allocate(2);
      INDEX_KEY_SERIALIZER.serialize(null, encoded).flip();
    }
  }

  @State(Scope.Thread)
  public static class Prefixes {
    @Param({"SHORT", "LONG", "MISMATCH", "LONGER"})
    public String shape;

    private final IndexKey[] keys = new IndexKey[KEY_COUNT];
    private final IndexKey[] prefixes = new IndexKey[KEY_COUNT];
    private int index;

    @Setup
    public void setup() {
      for (int i = 0; i < KEY_COUNT; i++) {
        var prefix =
            (shape.equals("SHORT") ? "db\u0000" : "warehouse\u0000analytics\u0000münchen\u0000")
                + i;
        prefixes[i] = key(prefix);
        keys[i] =
            key(
                switch (shape) {
                  case "LONGER" -> prefix.substring(0, prefix.length() - 1);
                  case "MISMATCH" -> prefix.substring(0, prefix.length() - 1) + "x\u0000table";
                  default -> prefix + "\u0000table";
                });
      }
    }
  }

  @State(Scope.Thread)
  public static class Longs {
    private final long[] values = new long[KEY_COUNT];
    private final IndexKey[] keys = new IndexKey[KEY_COUNT];
    private int index;

    @Setup
    public void setup() {
      for (int i = 0; i < KEY_COUNT; i++) {
        values[i] = Long.rotateLeft(0x9e3779b97f4a7c15L * (i + 1), i);
        keys[i] = key(values[i]);
      }
    }
  }

  @Benchmark
  public IndexKey deserialize(Keys state) {
    return IndexKey.deserializeKey(state.encoded[state.next()].position(0));
  }

  @Benchmark
  public int skip(Keys state) {
    var buffer = state.encoded[state.next()].position(0);
    IndexKey.skip(buffer);
    return buffer.position();
  }

  @Benchmark
  public ByteBuffer serialize(Keys state) {
    return state.keys[state.next()].serialize(state.target.clear());
  }

  @Benchmark
  public ByteBuffer serializePartial(Keys state) {
    var i = state.next();
    var key = state.keys[i];
    var target = state.target.clear().limit(state.encoded[i].limit() / 2);
    key.serializeNoFail(target);
    return target;
  }

  @Benchmark
  public IndexKey deserializeNull(NullKey state) {
    return IndexKey.deserializeKey(state.encoded.position(0));
  }

  @Benchmark
  public boolean startsWith(Prefixes state) {
    var i = state.index++ & (KEY_COUNT - 1);
    return state.keys[i].startsWith(state.prefixes[i]);
  }

  @Benchmark
  public IndexKey fromLong(Longs state) {
    return key(state.values[state.index++ & (KEY_COUNT - 1)]);
  }

  @Benchmark
  public long asLong(Longs state) {
    return state.keys[state.index++ & (KEY_COUNT - 1)].asLong();
  }
}
