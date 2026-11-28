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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.nio.ByteBuffer;
import java.util.Random;
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
public class VarIntBench {
  @State(Scope.Thread)
  public static class Values {
    @Param({"SMALL", "LARGE", "MIXED"})
    public String distribution;

    private final long[] values = new long[256];
    private final ByteBuffer[] encoded = new ByteBuffer[256];
    private final ByteBuffer target = ByteBuffer.allocate(9);
    private int index;

    @Setup
    public void setup() {
      var random = new Random(42);
      for (int i = 0; i < values.length; i++) {
        var bits =
            switch (distribution) {
              case "SMALL" -> 7;
              case "LARGE" -> 63;
              case "MIXED" -> 7 * (1 + random.nextInt(9));
              default -> throw new IllegalArgumentException(distribution);
            };
        values[i] = random.nextLong() & (Long.MAX_VALUE >>> (63 - bits));
        encoded[i] = VarInt.putVarInt(ByteBuffer.allocate(9), values[i]).flip();
      }
    }

    private int next() {
      return index++ & (values.length - 1);
    }
  }

  @Benchmark
  public int length(Values state) {
    return VarInt.varIntLen(state.values[state.next()]);
  }

  @Benchmark
  public ByteBuffer write(Values state) {
    return VarInt.putVarInt(state.target.clear(), state.values[state.next()]);
  }

  @Benchmark
  public long read(Values state) {
    return VarInt.readVarLong(state.encoded[state.next()].position(0));
  }

  @Benchmark
  public int skip(Values state) {
    var buffer = state.encoded[state.next()].position(0);
    VarInt.skipVarInt(buffer);
    return buffer.position();
  }
}
