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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.EntityIdSet.ENTITY_ID_SET_SERIALIZER;

import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
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
public class EntityIdSetSerializerBench {
  @State(Scope.Thread)
  public static class Values {
    @Param({"-1", "0", "1", "16", "1024"})
    public int cardinality;

    @Param({"SMALL", "LARGE"})
    public String ids;

    private final EntityIdSet[] values = new EntityIdSet[64];
    private final ByteBuffer[] encoded = new ByteBuffer[64];
    private final ByteBuffer target = ByteBuffer.allocate(16384);
    private int index;

    @Setup
    public void setup() {
      for (int i = 0; i < values.length; i++) {
        if (cardinality >= 0) {
          var entries = new LinkedHashSet<Long>();
          var base = ids.equals("SMALL") ? 0L : 0x123456789abcL + ((long) i << 20);
          for (int j = 0; j < cardinality; j++) {
            entries.add(base + j);
          }
          values[i] = EntityIdSet.entityIdSet(entries);
        }
        encoded[i] =
            ENTITY_ID_SET_SERIALIZER.serialize(values[i], ByteBuffer.allocate(16384)).flip();
      }
    }

    private int next() {
      return index++ & (values.length - 1);
    }
  }

  @Benchmark
  public int size(Values state) {
    return ENTITY_ID_SET_SERIALIZER.serializedSize(state.values[state.next()]);
  }

  @Benchmark
  public int sizeAndSerialize(Values state) {
    var value = state.values[state.next()];
    return ENTITY_ID_SET_SERIALIZER.serializedSize(value)
        + ENTITY_ID_SET_SERIALIZER.serialize(value, state.target.clear()).position();
  }

  @Benchmark
  public EntityIdSet deserialize(Values state) {
    return ENTITY_ID_SET_SERIALIZER.deserialize(state.encoded[state.next()].position(0));
  }

  @Benchmark
  public boolean isNull(Values state) {
    return ENTITY_ID_SET_SERIALIZER.isNullSerialized(state.encoded[state.next()]);
  }
}
