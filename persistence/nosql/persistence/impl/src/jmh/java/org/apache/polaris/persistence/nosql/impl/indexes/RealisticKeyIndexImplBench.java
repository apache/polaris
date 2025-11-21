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
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;

import java.util.Iterator;
import java.util.Map;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.impl.indexes.KeyIndexTestSet.RealisticKeySet;
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

/** Benchmark that uses {@link RealisticKeySet} to generate keys. */
@Warmup(iterations = 3, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class RealisticKeyIndexImplBench {
  @State(Scope.Benchmark)
  public static class BenchmarkParam {

    @Param({"1", "3"})
    public int namespaceLevels;

    @Param({"5", "50"})
    public int foldersPerLevel;

    @Param({"25", "50", "100"})
    public int tablesPerNamespace;

    @Param({"true"})
    public boolean deterministic;

    private KeyIndexTestSet<ObjRef> keyIndexTestSet;

    @Setup
    public void init() {
      KeyIndexTestSet.IndexTestSetGenerator<ObjRef> builder =
          KeyIndexTestSet.<ObjRef>newGenerator()
              .keySet(
                  ImmutableRealisticKeySet.builder()
                      .namespaceLevels(namespaceLevels)
                      .foldersPerLevel(foldersPerLevel)
                      .tablesPerNamespace(tablesPerNamespace)
                      .deterministic(deterministic)
                      .build())
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
  public Object serializeUnmodifiedIndex(BenchmarkParam param) {
    return param.keyIndexTestSet.serialize();
  }

  @Benchmark
  public Object serializeModifiedIndex(BenchmarkParam param) {
    IndexSpi<ObjRef> deserialized = param.keyIndexTestSet.deserialize();
    ((IndexImpl<?>) deserialized).setModified();
    return deserialized.serialize();
  }

  @Benchmark
  public Object deserializeAdd(BenchmarkParam param) {
    IndexSpi<ObjRef> deserialized = param.keyIndexTestSet.deserialize();
    for (char c = 'a'; c <= 'z'; c++) {
      deserialized.add(indexElement(key(c + "xkey"), Util.randomObjId()));
    }
    return deserialized;
  }

  @Benchmark
  public Object deserializeAddSerialize(BenchmarkParam param) {
    IndexSpi<ObjRef> deserialized = param.keyIndexTestSet.deserialize();
    for (char c = 'a'; c <= 'z'; c++) {
      deserialized.add(indexElement(key(c + "xkey"), Util.randomObjId()));
    }
    return deserialized.serialize();
  }

  @Benchmark
  public Object deserialize(BenchmarkParam param) {
    return param.keyIndexTestSet.deserialize();
  }

  @Benchmark
  public Object deserializeGetRandomKey(BenchmarkParam param) {
    IndexSpi<ObjRef> deserialized = param.keyIndexTestSet.deserialize();
    return deserialized.getElement(param.keyIndexTestSet.randomKey());
  }

  @Benchmark
  public void deserializeIterate250(BenchmarkParam param, Blackhole bh) {
    Index<ObjRef> deserialized = param.keyIndexTestSet.deserialize();
    Iterator<Map.Entry<IndexKey, ObjRef>> iter = deserialized.iterator();
    for (int i = 0; i < 250 && iter.hasNext(); i++) {
      bh.consume(iter.next());
    }
  }
}
