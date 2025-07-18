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

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.polaris.ids.api.SnowflakeIdGenerator.DEFAULT_NODE_ID_BITS;
import static org.apache.polaris.ids.api.SnowflakeIdGenerator.DEFAULT_SEQUENCE_BITS;
import static org.apache.polaris.ids.api.SnowflakeIdGenerator.DEFAULT_TIMESTAMP_BITS;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.ids.api.SnowflakeIdGenerator;
import org.apache.polaris.ids.spi.IdGeneratorSource;
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

@Warmup(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(NANOSECONDS)
public class SnowflakeIdGeneratorBench {
  @State(Scope.Benchmark)
  public static class BenchmarkParam {
    SnowflakeIdGenerator idGeneratorMonotonicClock;
    SnowflakeIdGenerator idGeneratorMonotonicClockHugeSequence;
    SnowflakeIdGenerator idGeneratorFakeClock;
    MonotonicClock monotonicClock;

    @Setup
    public void init() {
      monotonicClock = MonotonicClockImpl.newDefaultInstance();

      var idGeneratorSource =
          new IdGeneratorSource() {
            @Override
            public int nodeId() {
              return 1;
            }

            @Override
            public long currentTimeMillis() {
              return monotonicClock.currentTimeMillis();
            }
          };
      idGeneratorMonotonicClock =
          new SnowflakeIdGeneratorFactory()
              .buildIdGenerator(
                  DEFAULT_TIMESTAMP_BITS,
                  DEFAULT_SEQUENCE_BITS,
                  DEFAULT_NODE_ID_BITS,
                  monotonicClock.currentTimeMillis(),
                  idGeneratorSource);

      idGeneratorMonotonicClockHugeSequence =
          new SnowflakeIdGeneratorFactory()
              .buildIdGenerator(
                  DEFAULT_TIMESTAMP_BITS - 15,
                  DEFAULT_SEQUENCE_BITS + 23,
                  DEFAULT_NODE_ID_BITS - 8,
                  monotonicClock.currentTimeMillis(),
                  idGeneratorSource);

      var off = System.currentTimeMillis();
      var fakeClock = new AtomicLong(off);
      idGeneratorFakeClock =
          new SnowflakeIdGeneratorImpl(
              DEFAULT_TIMESTAMP_BITS,
              DEFAULT_SEQUENCE_BITS,
              DEFAULT_NODE_ID_BITS,
              off,
              new IdGeneratorSource() {
                @Override
                public int nodeId() {
                  return 1;
                }

                @Override
                public long currentTimeMillis() {
                  return fakeClock.get();
                }
              }) {
            @Override
            void spinWaitSequence() {
              fakeClock.incrementAndGet();
            }
          };
    }
  }

  /**
   * <em>WARNING</em>: This {@code generateIdMonotonicSource} benchmark relies indirectly on a real
   * system clock via {@link MonotonicClockImpl} and is therefore not only slower because of hitting
   * the OS clock but mostly because it spin-waits due to too many IDs are generated per
   * millisecond. In other words: the times yielded by JMH <em>MUST NOT</em> be considered as
   * runtimes in production, because it practically never happens that more than 4096 IDs are
   * generated per millisecond.
   */
  @Benchmark
  public long generateIdMonotonicSourceSpinning(BenchmarkParam param) {
    return param.idGeneratorMonotonicClock.generateId();
  }

  /**
   * Snowflake ID generation against a generator configured with an extremely high number of
   * sequence-bits for the sole purpose of benchmarking ID generation with an extremely low chance
   * of spinning.
   */
  @Benchmark
  public long generateIdMonotonicSourceHugeSequence(BenchmarkParam param) {
    return param.idGeneratorMonotonicClockHugeSequence.generateId();
  }

  /** */
  @Benchmark
  @Measurement(iterations = 50, time = 900, timeUnit = MICROSECONDS)
  @Threads(1)
  public long generateIdMonotonicSourceNotSpinning(BenchmarkParam param) {
    return param.idGeneratorMonotonicClock.generateId();
  }

  /**
   * Artificial benchmark to just measure the ID generator without spinning (waiting for the "next"
   * millisecond).
   */
  @Benchmark
  public long generateIdFakeClockSource(BenchmarkParam param) {
    return param.idGeneratorFakeClock.generateId();
  }
}
