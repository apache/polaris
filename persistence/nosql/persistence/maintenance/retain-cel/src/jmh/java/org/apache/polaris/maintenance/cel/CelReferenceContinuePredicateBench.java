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
package org.apache.polaris.maintenance.cel;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Duration;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
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

@Warmup(iterations = 4, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(NANOSECONDS)
public class CelReferenceContinuePredicateBench {

  @State(Scope.Benchmark)
  public static class BenchmarkParam {
    BaseCommitObj commitObj;

    CelReferenceContinuePredicate<BaseCommitObj> predicateConstantCondition;
    CelReferenceContinuePredicate<BaseCommitObj> predicateOneCondition;
    CelReferenceContinuePredicate<BaseCommitObj> predicateTwoConditions;

    @Setup
    public void init() {
      commitObj =
          new BaseCommitObj() {
            @Override
            public long seq() {
              throw new UnsupportedOperationException();
            }

            @Override
            public long[] tail() {
              throw new UnsupportedOperationException();
            }

            @Override
            public ObjType type() {
              throw new UnsupportedOperationException();
            }

            @Override
            public long id() {
              throw new UnsupportedOperationException();
            }

            @Nullable
            @Override
            public String versionToken() {
              throw new UnsupportedOperationException();
            }

            @Nonnull
            @Override
            public Obj withCreatedAtMicros(long createdAt) {
              throw new UnsupportedOperationException();
            }

            @Nonnull
            @Override
            public Obj withNumParts(int numParts) {
              throw new UnsupportedOperationException();
            }
          };
      predicateConstantCondition =
          new CelReferenceContinuePredicate<>("refName", o -> Duration.ZERO, "true");
      predicateOneCondition =
          new CelReferenceContinuePredicate<>("refName", o -> Duration.ZERO, "ageDays < 100");
      predicateTwoConditions =
          new CelReferenceContinuePredicate<>(
              "refName", o -> Duration.ZERO, "ageDays < 100 || commits < 100");
    }
  }

  @Benchmark
  public boolean constantCondition(BenchmarkParam param) {
    return param.predicateConstantCondition.test(param.commitObj);
  }

  @Benchmark
  public boolean oneCondition(BenchmarkParam param) {
    return param.predicateOneCondition.test(param.commitObj);
  }

  @Benchmark
  public boolean twoConditions(BenchmarkParam param) {
    return param.predicateTwoConditions.test(param.commitObj);
  }
}
