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

import java.util.Optional;
import org.apache.polaris.persistence.nosql.api.commit.RetryConfig;
import org.apache.polaris.persistence.nosql.api.commit.RetryTimeoutException;
import org.apache.polaris.persistence.nosql.impl.commits.CommitterWithStats;
import org.apache.polaris.persistence.nosql.impl.commits.retry.RetryStatsConsumer;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 4, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 10_000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class CommitBenchmark {
  @State(Scope.Benchmark)
  public static class BenchmarkParam extends BaseParam {

    RetryConfig retryConfig;

    String refName;
    CommitterWithStats<SimpleCommitTestObj, String> committer;

    @Setup
    public void init() {
      setupPersistence();

      refName = "commit-bench-" + System.currentTimeMillis();
      persistence.createReference(refName, Optional.empty());

      committer =
          (CommitterWithStats<SimpleCommitTestObj, String>)
              persistence.createCommitter(refName, SimpleCommitTestObj.class, String.class);
    }

    @TearDown
    public void tearDown() throws Exception {
      shutdownPersistence();
    }
  }

  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class ThreadParam implements RetryStatsConsumer {
    public int timeouts;
    public int success;
    public int retries;
    public long retrySleepMillis;

    String refName;
    CommitterWithStats<SimpleCommitTestObj, String> committer;

    @Setup(Level.Iteration)
    public void clean() {
      timeouts = 0;
      success = 0;
    }

    @Setup
    public void createBranch(BenchmarkParam param) {
      refName =
          "commit-bench-thread-"
              + System.currentTimeMillis()
              + "-"
              + Thread.currentThread().threadId();
      param.persistence.createReference(refName, Optional.empty());
      committer =
          (CommitterWithStats<SimpleCommitTestObj, String>)
              param.persistence.createCommitter(refName, SimpleCommitTestObj.class, String.class);
    }

    @Override
    public void retryLoopFinished(
        Result result, int retries, long sleepTimeMillis, long totalDurationNanos) {
      switch (result) {
        case SUCCESS:
          success++;
          break;
        case TIMEOUT:
          timeouts++;
          break;
        case CONFLICT:
        case ERROR:
          break;
      }
      this.retries += retries;
      this.retrySleepMillis += sleepTimeMillis;
    }
  }

  @Benchmark
  public Optional<String> commitSingleRef(BenchmarkParam benchParam, ThreadParam threadParam)
      throws Exception {
    try {
      return benchParam.committer.commit(
          (state, refObjSupplier) -> {
            var refObj = refObjSupplier.get();
            return state.commitResult(
                "fooo", ImmutableSimpleCommitTestObj.builder().payload("some payload"), refObj);
          },
          threadParam);
    } catch (RetryTimeoutException e) {
      return null;
    }
  }

  @Benchmark
  public Optional<String> commitDistinctRefs(ThreadParam threadParam) throws Exception {
    try {
      return threadParam.committer.commit(
          (state, refObjSupplier) -> {
            var refObj = refObjSupplier.get();
            return state.commitResult(
                "foo", ImmutableSimpleCommitTestObj.builder().payload("some payload"), refObj);
          },
          threadParam);
    } catch (RetryTimeoutException e) {
      return Optional.empty();
    }
  }
}
