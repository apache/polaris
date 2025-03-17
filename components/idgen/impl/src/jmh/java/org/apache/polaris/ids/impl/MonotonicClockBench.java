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
package org.apache.polaris.ids.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.time.Instant;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 6, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(NANOSECONDS)
public class MonotonicClockBench {
  @State(Scope.Benchmark)
  public static class BenchmarkParam {
    MonotonicClockImpl monotonicClock;
    MonotonicClockImpl monotonicClockIdle;

    @Setup
    public void init() {
      monotonicClock = new MonotonicClockImpl().start();
      monotonicClockIdle = new MonotonicClockImpl();
    }
  }

  @Threads(1)
  @Benchmark
  public void tick(BenchmarkParam param) {
    param.monotonicClockIdle.tick();
  }

  @Benchmark
  public long nanoTime(BenchmarkParam param) {
    return param.monotonicClock.nanoTime();
  }

  @Benchmark
  public long currentTimeMicros(BenchmarkParam param) {
    return param.monotonicClock.currentTimeMicros();
  }

  @Benchmark
  public long currentTimeMillis(BenchmarkParam param) {
    return param.monotonicClock.currentTimeMillis();
  }

  @Benchmark
  public Instant currentInstant(BenchmarkParam param) {
    return param.monotonicClock.currentInstant();
  }

  @Benchmark
  public long systemCurrentTimeMillis(BenchmarkParam param) {
    return param.monotonicClock.systemCurrentTimeMillis();
  }

  @Benchmark
  public long systemNanoTime(BenchmarkParam param) {
    return param.monotonicClock.systemNanoTime();
  }
}
