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
package org.apache.polaris.persistence.nosql.impl.cache;

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.cache.CacheConfig;
import org.apache.polaris.persistence.nosql.api.cache.CacheSizing;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.SimpleTestObj;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.apache.polaris.persistence.nosql.impl.indexes.IndexStripeObj;
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
import org.openjdk.jmh.annotations.Warmup;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.dataformat.smile.SmileMapper;
import tools.jackson.dataformat.smile.SmileWriteFeature;

/** Cache hits and equal-key updates; fixture creation is outside the timed operations. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 4, time = 1)
@Fork(2)
public class CaffeineCacheBackendBench {
  private static final String REALM = "benchmark";
  private static final int ENTRIES = 16;

  private static CaffeineCacheBackend backend() {
    return new CaffeineCacheBackend(
        CacheConfig.BuildableCacheConfig.builder()
            .sizing(CacheSizing.builder().fixedSize(MemorySize.ofMega(64)).build())
            .referenceTtl(Duration.ofMinutes(10))
            .clockNanos(() -> 0L)
            .build(),
        Optional.empty());
  }

  @State(Scope.Thread)
  public static class References {
    @Param({"0", "20"})
    public int history;

    CaffeineCacheBackend cache;
    Reference[] values;
    int cursor;

    @Setup
    public void setup() {
      cache = backend();
      values = new Reference[ENTRIES];
      for (int i = 0; i < ENTRIES; i++) {
        long[] previous = new long[history];
        for (int j = 0; j < history; j++) {
          previous[j] = 123456789L + i * 1000L + j;
        }
        values[i] =
            Reference.builder()
                .name("catalog/main-" + i)
                .pointer(objRef("commit", 123456789L + i, 2))
                .createdAtMicros(1700000000123456L + i)
                .previousPointers(previous)
                .build();
        cache.putReferenceLocal(REALM, values[i]);
      }
      cache.cache.cleanUp();
    }

    Reference next() {
      return values[cursor++ & (ENTRIES - 1)];
    }
  }

  @Benchmark
  public Reference referenceHit(References state) {
    return state.cache.getReference(REALM, state.next().name());
  }

  @Benchmark
  public void referenceUpdate(References state) {
    state.cache.putReferenceLocal(REALM, state.next());
  }

  @State(Scope.Thread)
  public static class Stripes {
    @Param({"1024", "65536"})
    public int size;

    @Param({"HEAP", "DIRECT", "READ_ONLY_SLICE"})
    public String buffer;

    CaffeineCacheBackend cache;
    IndexStripeObj[] values;
    ObjRef[] ids;
    int cursor;

    @Setup
    public void setup() {
      cache = backend();
      values = new IndexStripeObj[ENTRIES];
      ids = new ObjRef[ENTRIES];
      Random random = new Random(42);
      for (int i = 0; i < ENTRIES; i++) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        ByteBuffer payload;
        if (buffer.equals("DIRECT")) {
          payload = ByteBuffer.allocateDirect(size + 16);
          payload.position(8).put(bytes).flip().position(8);
        } else if (buffer.equals("READ_ONLY_SLICE")) {
          payload = ByteBuffer.allocate(size + 32).position(8).slice();
          payload.position(8).put(bytes).flip().position(8);
          payload = payload.asReadOnlyBuffer();
        } else {
          payload = ByteBuffer.wrap(bytes);
        }
        values[i] = IndexStripeObj.indexStripeObj(i + 1L, payload);
        ids[i] = objRef(values[i]);
        cache.putLocal(REALM, values[i]);
      }
      cache.cache.cleanUp();
    }

    int next() {
      return cursor++ & (ENTRIES - 1);
    }
  }

  @Benchmark
  public Obj stripeHit(Stripes state) {
    return state.cache.get(REALM, state.ids[state.next()]);
  }

  @Benchmark
  public void stripeUpdate(Stripes state) {
    state.cache.putLocal(REALM, state.values[state.next()]);
  }

  @State(Scope.Thread)
  public static class Binary {
    @Param({"1024", "65536"})
    public int size;

    Obj value;
    CaffeineCacheBackend.CacheValue cached;

    @Setup
    public void setup() {
      byte[] bytes = new byte[size];
      new Random(42).nextBytes(bytes);
      value = SimpleTestObj.builder().id(1).binary(bytes).build();
      CaffeineCacheBackend cache = backend();
      cache.putLocal(REALM, value);
      cached = cache.cache.asMap().values().iterator().next();
    }
  }

  @Benchmark
  public byte[] binaryEncode(Binary state) {
    return CaffeineCacheBackend.serializeObj(state.value);
  }

  @Benchmark
  public Obj binaryDecode(Binary state) {
    return state.cached.getObj();
  }

  @State(Scope.Thread)
  public static class Times {
    @Param({"CACHE", "TEXT", "JACKSON_NUMERIC"})
    public String encoding;

    ObjectMapper mapper;
    Instant[] instants;
    Duration[] durations;
    byte[][] encodedInstants;
    byte[][] encodedDurations;
    int cursor;

    @Setup
    public void setup() {
      var builder =
          SmileMapper.builder()
              .findAndAddModules()
              .disable(SmileWriteFeature.ENCODE_BINARY_AS_7BIT);
      if (encoding.equals("JACKSON_NUMERIC")) {
        builder
            .enable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(DateTimeFeature.WRITE_DURATIONS_AS_TIMESTAMPS);
      }
      mapper = encoding.equals("CACHE") ? CaffeineCacheBackend.SMILE_MAPPER : builder.build();
      instants = new Instant[ENTRIES];
      durations = new Duration[ENTRIES];
      encodedInstants = new byte[ENTRIES][];
      encodedDurations = new byte[ENTRIES][];
      for (int i = 0; i < ENTRIES; i++) {
        instants[i] = Instant.ofEpochSecond(1700000000L + i, 123456789 + i);
        durations[i] = Duration.ofSeconds(300L + i, 123456789 + i);
        encodedInstants[i] = mapper.writeValueAsBytes(instants[i]);
        encodedDurations[i] = mapper.writeValueAsBytes(durations[i]);
      }
    }

    int next() {
      return cursor++ & (ENTRIES - 1);
    }
  }

  @Benchmark
  public byte[] instantEncode(Times state) {
    return state.mapper.writeValueAsBytes(state.instants[state.next()]);
  }

  @Benchmark
  public Instant instantDecode(Times state) {
    return state.mapper.readValue(state.encodedInstants[state.next()], Instant.class);
  }

  @Benchmark
  public byte[] durationEncode(Times state) {
    return state.mapper.writeValueAsBytes(state.durations[state.next()]);
  }

  @Benchmark
  public Duration durationDecode(Times state) {
    return state.mapper.readValue(state.encodedDurations[state.next()], Duration.class);
  }
}
