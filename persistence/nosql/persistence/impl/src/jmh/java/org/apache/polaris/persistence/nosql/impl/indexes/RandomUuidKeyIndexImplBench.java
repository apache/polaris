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

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;

import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.impl.indexes.KeyIndexTestSet.IndexTestSetGenerator;
import org.apache.polaris.persistence.nosql.impl.indexes.KeyIndexTestSet.RandomUuidKeySet;
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
import org.openjdk.jmh.infra.Blackhole;

/** Benchmark that uses {@link RandomUuidKeySet} to generate keys. */
@Warmup(iterations = 3, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class RandomUuidKeyIndexImplBench {
  @State(Scope.Benchmark)
  public static class BenchmarkParam {

    @Param({"1000", "10000", "100000", "200000"})
    public int keys;

    private KeyIndexTestSet<ObjRef> keyIndexTestSet;

    @Setup
    public void init() {
      IndexTestSetGenerator<ObjRef> builder =
          KeyIndexTestSet.<ObjRef>newGenerator()
              .keySet(ImmutableRandomUuidKeySet.builder().numKeys(keys).build())
              .elementSupplier(key -> indexElement(key, Util.randomObjId()))
              .elementSerializer(OBJ_REF_SERIALIZER)
              .build();

      this.keyIndexTestSet = builder.generateIndexTestSet();

      System.err.printf(
          "%nNumber of tables: %d%nSerialized size: %d%n",
          keyIndexTestSet.keys().size(), keyIndexTestSet.serializedSafe().remaining());
    }
  }

  @Benchmark
  public void serialize(BenchmarkParam param, Blackhole bh) {
    bh.consume(param.keyIndexTestSet.serialize());
  }

  @Benchmark
  public void deserialize(BenchmarkParam param, Blackhole bh) {
    bh.consume(param.keyIndexTestSet.deserialize());
  }

  @Benchmark
  public void randomGetKey(BenchmarkParam param, Blackhole bh) {
    bh.consume(param.keyIndexTestSet.randomGetKey());
  }
}
