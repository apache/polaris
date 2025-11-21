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
package org.apache.polaris.persistence.nosql.benchmark;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 4, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class SimpleBenchmark {
  @State(Scope.Benchmark)
  public static class BenchmarkParam extends BaseParam {
    private String payload;

    @Param({"1", "1000", "10000", "100000"})
    public int payloadLength;

    private long[] reusedIds;
    private final AtomicInteger reusedIdIndex = new AtomicInteger();

    @Setup
    public void init() {
      setupPersistence();

      this.payload = "x".repeat(payloadLength);

      // populate the rows for 'reusedIds'
      this.reusedIds = IntStream.range(0, 20).mapToLong(x -> persistence.generateId()).toArray();
      persistence.writeMany(
          ImmutableObj.class,
          Arrays.stream(reusedIds)
              .mapToObj(id -> ImmutableImmutableObj.builder().id(id).build())
              .toArray(ImmutableObj[]::new));
    }

    @TearDown
    public void tearDown() throws Exception {
      shutdownPersistence();
    }
  }

  @Benchmark
  public ImmutableObj singleIdConditionalInsert(BenchmarkParam param) {
    return param.persistence.conditionalInsert(
        ImmutableImmutableObj.builder()
            .id(param.persistence.generateId())
            .value(param.payload)
            .build(),
        ImmutableObj.class);
  }

  @Benchmark
  public ImmutableObj singleIdWriteNoConflict(BenchmarkParam param) {
    return param.persistence.write(
        ImmutableImmutableObj.builder()
            .id(param.persistence.generateId())
            .value(param.payload)
            .build(),
        ImmutableObj.class);
  }

  @Benchmark
  public ImmutableObj singleIdWriteExisting(BenchmarkParam param) {
    var id = param.reusedIds[(param.reusedIdIndex.incrementAndGet() % param.reusedIds.length)];
    return param.persistence.write(
        ImmutableImmutableObj.builder().id(id).value(param.payload).build(), ImmutableObj.class);
  }

  @Benchmark
  public ImmutableObj[] manyIdWriteExisting(BenchmarkParam param) {
    return param.persistence.writeMany(
        ImmutableObj.class,
        Arrays.stream(param.reusedIds)
            .mapToObj(id -> ImmutableImmutableObj.builder().id(id).value(param.payload).build())
            .toArray(ImmutableObj[]::new));
  }

  @Benchmark
  public ImmutableObj[] manyIdWriteNoConflict(BenchmarkParam param) {
    return param.persistence.writeMany(
        ImmutableObj.class,
        IntStream.range(0, 20)
            .mapToObj(
                x ->
                    ImmutableImmutableObj.builder()
                        .id(param.persistence.generateId())
                        .value(param.payload)
                        .build())
            .toArray(ImmutableObj[]::new));
  }
}
