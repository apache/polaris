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
package org.apache.polaris.persistence.nosql.api.obj;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;

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
public class ObjRefBench {
  @State(Scope.Thread)
  public static class Values {
    @Param({"ASCII", "ASCII32", "UNICODE", "MULTIPART"})
    public String shape;

    @Param({"HEAP", "SLICED", "READ_ONLY", "DIRECT"})
    public String bufferType;

    private final ObjRef[] values = new ObjRef[64];
    private final ByteBuffer[] encoded = new ByteBuffer[64];
    private final ByteBuffer target = ByteBuffer.allocate(128);
    private int index;

    @Setup
    public void setup() {
      for (int i = 0; i < values.length; i++) {
        var type =
            switch (shape) {
              case "ASCII", "MULTIPART" -> "entity";
              case "ASCII32" -> "0123456789abcdef0123456789abcdef";
              case "UNICODE" -> "entité";
              default -> throw new IllegalArgumentException(shape);
            };
        values[i] = ObjRef.objRef(type, 0x123456789abcL + i, shape.equals("MULTIPART") ? 3 : 1);
        var buffer =
            bufferType.equals("DIRECT") ? ByteBuffer.allocateDirect(128) : ByteBuffer.allocate(128);
        if (bufferType.equals("SLICED")) {
          buffer = buffer.position(7).slice();
        }
        buffer.position(3);
        OBJ_REF_SERIALIZER.serialize(values[i], buffer);
        buffer.limit(buffer.position());
        encoded[i] = bufferType.equals("READ_ONLY") ? buffer.asReadOnlyBuffer() : buffer;
      }
    }

    private int next() {
      return index++ & (values.length - 1);
    }
  }

  @Benchmark
  public int size(Values state) {
    return OBJ_REF_SERIALIZER.serializedSize(state.values[state.next()]);
  }

  @Benchmark
  public ByteBuffer serialize(Values state) {
    return OBJ_REF_SERIALIZER.serialize(state.values[state.next()], state.target.clear());
  }

  @Benchmark
  public int sizeAndSerialize(Values state) {
    var value = state.values[state.next()];
    var size = OBJ_REF_SERIALIZER.serializedSize(value);
    return size + OBJ_REF_SERIALIZER.serialize(value, state.target.clear()).position();
  }

  @Benchmark
  public ObjRef deserialize(Values state) {
    return OBJ_REF_SERIALIZER.deserialize(state.encoded[state.next()].position(3));
  }
}
