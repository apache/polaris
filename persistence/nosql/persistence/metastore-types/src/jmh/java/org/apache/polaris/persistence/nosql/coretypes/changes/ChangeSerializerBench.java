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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.polaris.persistence.nosql.coretypes.changes.Change.CHANGE_SERIALIZER;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
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
public class ChangeSerializerBench {
  @State(Scope.Thread)
  public static class Values {
    @Param({"NULL", "ADD", "UPDATE", "REMOVE", "RENAME", "GENERIC"})
    public String shape;

    private final Change[] values = new Change[64];
    private final ByteBuffer[] encoded = new ByteBuffer[64];
    private final ByteBuffer target = ByteBuffer.allocate(4096);
    private int index;

    @Setup
    public void setup() {
      for (int i = 0; i < values.length; i++) {
        values[i] =
            switch (shape) {
              case "NULL" -> null;
              case "ADD" -> ChangeAdd.builder().build();
              case "UPDATE" -> ChangeUpdate.builder().build();
              case "REMOVE" -> ChangeRemove.builder().build();
              case "RENAME" ->
                  ChangeRename.builder().renameFrom(IndexKey.key("namespace.table_" + i)).build();
              case "GENERIC" ->
                  ImmutableGenericChange.of(
                      ChangeType.valueOf("future"), Map.of("entity", "table_" + i));
              default -> throw new IllegalArgumentException(shape);
            };
        encoded[i] = CHANGE_SERIALIZER.serialize(values[i], ByteBuffer.allocate(4096)).flip();
      }
    }

    private int next() {
      return index++ & (values.length - 1);
    }
  }

  @Benchmark
  public int size(Values state) {
    return CHANGE_SERIALIZER.serializedSize(state.values[state.next()]);
  }

  @Benchmark
  public ByteBuffer serialize(Values state) {
    return CHANGE_SERIALIZER.serialize(state.values[state.next()], state.target.clear());
  }

  @Benchmark
  public int sizeAndSerialize(Values state) {
    var value = state.values[state.next()];
    return CHANGE_SERIALIZER.serializedSize(value)
        + CHANGE_SERIALIZER.serialize(value, state.target.clear()).position();
  }

  @Benchmark
  public boolean isNull(Values state) {
    return CHANGE_SERIALIZER.isNullSerialized(state.encoded[state.next()]);
  }
}
