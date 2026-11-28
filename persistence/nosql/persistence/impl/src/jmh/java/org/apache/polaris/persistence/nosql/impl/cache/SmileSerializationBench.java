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
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.cache.CacheConfig;
import org.apache.polaris.persistence.nosql.api.cache.CacheSizing;
import org.apache.polaris.persistence.nosql.api.index.ImmutableIndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexStripe;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.SimpleTestObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStatus;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogType;
import org.apache.polaris.persistence.nosql.coretypes.catalog.ImmutableCatalogObj;
import org.apache.polaris.persistence.nosql.impl.PersistenceImplementation;
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

/**
 * Ordinary object binding, excluding index element decoding and backend I/O.
 *
 * <p>Build with {@code ./gradlew :polaris-persistence-nosql-impl:jmhJar}. A short fixture smoke
 * check (not a performance measurement):
 *
 * <pre>{@code
 * java -jar <jmh-jar> 'SmileSerializationBench.(cacheHit|persistenceDecode)' \
 *   -p kind=CATALOG_STATE -p size=4096 -p stripes=0 -p versioned=false \
 *   -wi 0 -i 1 -r 100ms -f 1 -foe true
 * }</pre>
 *
 * <p>Select striped and versioned fixtures with {@code -p stripes=4 -p versioned=true}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class SmileSerializationBench {
  @State(Scope.Thread)
  public static class ObjectState {
    @Param({"CATALOG_STATE", "CATALOG", "SIMPLE"})
    public String kind;

    @Param({"0", "4096", "65536"})
    public int size;

    @Param({"0"})
    public int stripes;

    @Param({"false"})
    public boolean versioned;

    private Obj obj;
    private ObjRef ref;
    private byte[] persisted;
    private CaffeineCacheBackend cache;

    @Setup
    public void setup() {
      byte[] bytes = new byte[size];
      new Random(42).nextBytes(bytes);
      var names = ImmutableIndexContainer.<ObjRef>builder().embedded(ByteBuffer.wrap(bytes));
      for (int i = 0; i < stripes; i++) {
        names.addStripe(
            IndexStripe.indexStripe(
                IndexKey.key("a" + i), IndexKey.key("z" + i), objRef("idx-stripe", i + 1L, 1)));
      }
      obj =
          switch (kind) {
            case "CATALOG_STATE" ->
                CatalogStateObj.builder()
                    .id(1234567890123456L)
                    .numParts(versioned ? 2 : 1)
                    .createdAtMicros(1700000000123456L)
                    .versionToken(versioned ? "version-42" : null)
                    .seq(100)
                    .tail(
                        new long[] {
                          1234567890123455L, 1234567890123454L, 1234567890123453L, 1234567890123452L
                        })
                    .nameToObjRef(names.build())
                    .stableIdToName(
                        ImmutableIndexContainer.<IndexKey>builder()
                            .embedded(ByteBuffer.wrap(bytes))
                            .build())
                    .build();
            case "CATALOG" ->
                ImmutableCatalogObj.builder()
                    .id(1234567890123456L)
                    .numParts(versioned ? 2 : 1)
                    .createdAtMicros(1700000000123456L)
                    .versionToken(versioned ? "version-42" : null)
                    .name("example")
                    .stableId(1234567890123450L)
                    .createTimestamp(Instant.ofEpochSecond(1700000000L, 123456789))
                    .updateTimestamp(Instant.ofEpochSecond(1700000001L, 987654321))
                    .catalogType(CatalogType.INTERNAL)
                    .status(CatalogStatus.ACTIVE)
                    .build();
            case "SIMPLE" ->
                SimpleTestObj.builder()
                    .id(1234567890123456L)
                    .text("example")
                    .binary(bytes)
                    .createdAtMicros(1700000000123456L)
                    .numParts(versioned ? 2 : 1)
                    .versionToken(versioned ? "version-42" : null)
                    .build();
            default -> throw new IllegalArgumentException("Unknown fixture kind " + kind);
          };
      ref = objRef(obj);
      persisted = PersistenceImplementation.serializeObj(obj);
      cache =
          new CaffeineCacheBackend(
              CacheConfig.BuildableCacheConfig.builder()
                  .sizing(CacheSizing.builder().fixedSize(MemorySize.ofMega(64)).build())
                  .build(),
              Optional.empty());
      cache.putLocal("benchmark", obj);
      cache.cache.cleanUp();
      var cached = cache.get("benchmark", ref);
      if (cached == null
          || !cached.getClass().equals(obj.getClass())
          || !cached.equals(obj)
          || cached.createdAtMicros() != obj.createdAtMicros()) {
        throw new IllegalStateException(
            "Cache fixture must round-trip as "
                + obj.getClass().getName()
                + ", got "
                + (cached == null ? null : cached.getClass().getName()));
      }
      var restored = readPersistence();
      if (!obj.equals(restored) || restored.createdAtMicros() != obj.createdAtMicros()) {
        throw new IllegalStateException(
            "Persistence fixture must round-trip as " + obj.getClass().getName());
      }
    }

    Obj readPersistence() {
      return PersistenceImplementation.deserializeObj(
          obj.type().id(),
          obj.id(),
          obj.numParts(),
          persisted,
          obj.versionToken(),
          obj.createdAtMicros(),
          obj.type().targetClass());
    }
  }

  @State(Scope.Thread)
  public static class ArrayState {
    @Param({"0", "4096", "65536"})
    public int size;

    private long[] values;
    private byte[] numbers;

    @Setup
    public void setup() {
      values = new long[Math.max(4, size / Long.BYTES)];
      for (int i = 0; i < values.length; i++) {
        values[i] = 1700000000L + i;
      }
      numbers = PersistenceImplementation.serialize(values);
      if (!Arrays.equals(values, PersistenceImplementation.deserialize(numbers, long[].class))
          || !Arrays.equals(
              values,
              PersistenceImplementation.deserialize(ByteBuffer.wrap(numbers), long[].class))) {
        throw new IllegalStateException("Numeric fixture must round-trip through both input forms");
      }
    }
  }

  @Benchmark
  public Obj cacheHit(ObjectState state) {
    return state.cache.get("benchmark", state.ref);
  }

  @Benchmark
  public Obj persistenceDecode(ObjectState state) {
    return state.readPersistence();
  }

  @Benchmark
  public byte[] cacheEncode(ObjectState state) {
    return CaffeineCacheBackend.serializeObj(state.obj);
  }

  @Benchmark
  public byte[] persistenceEncode(ObjectState state) {
    return PersistenceImplementation.serializeObj(state.obj);
  }

  @Benchmark
  public byte[] genericEncode(ArrayState state) {
    return PersistenceImplementation.serialize(state.values);
  }

  @Benchmark
  public long[] genericDecode(ArrayState state) {
    return PersistenceImplementation.deserialize(state.numbers, long[].class);
  }

  @Benchmark
  public long[] bufferDecode(ArrayState state) {
    return PersistenceImplementation.deserialize(ByteBuffer.wrap(state.numbers), long[].class);
  }
}
