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
package org.apache.polaris.persistence.nosql.correctness;

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.ids.impl.MonotonicClockImpl;
import org.apache.polaris.ids.impl.SnowflakeIdGeneratorFactory;
import org.apache.polaris.ids.spi.IdGeneratorSource;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.commit.CommitException;
import org.apache.polaris.persistence.nosql.api.commit.RetryTimeoutException;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.impl.commits.CommitterWithStats;
import org.apache.polaris.persistence.nosql.testextension.BackendTestFactory;
import org.apache.polaris.persistence.nosql.testextension.BackendTestFactoryLoader;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.assertj.core.util.Streams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class BaseCorrectness {
  @InjectSoftAssertions protected SoftAssertions soft;

  private static BackendTestFactory backendTestFactory;
  protected Backend backend;
  protected Persistence persistence;
  private MonotonicClock clock;

  @BeforeEach
  protected void init() throws Exception {
    this.backend = setupBackend();

    var info = backend.setupSchema().orElse("");
    System.out.printf("Opened new persistence backend '%s' %s%n", backend.type(), info);

    clock = MonotonicClockImpl.newDefaultInstance();

    var idGenerator =
        new SnowflakeIdGeneratorFactory()
            .buildIdGenerator(
                Map.of(),
                new IdGeneratorSource() {
                  @Override
                  public int nodeId() {
                    return 42;
                  }

                  @Override
                  public long currentTimeMillis() {
                    return clock.currentTimeMillis();
                  }
                });
    this.persistence =
        backend.newPersistence(
            identity(),
            PersistenceParams.BuildablePersistenceParams.builder().build(),
            "42",
            clock,
            idGenerator);
  }

  protected Backend setupBackend() throws Exception {
    var backendName = System.getProperty("polaris.testBackend.name");

    if (backendTestFactory == null) {
      backendTestFactory = BackendTestFactoryLoader.findFactoryByName(backendName);
      backendTestFactory.start();
    }

    return backendTestFactory.createNewBackend();
  }

  @AfterEach
  protected void cleanup() throws Exception {
    backend.close();
    clock.close();
  }

  @AfterAll
  static void tearDownBackendTestFactory() throws Exception {
    if (backendTestFactory != null) {
      backendTestFactory.stop();
    }
  }

  @ParameterizedTest
  @CsvSource(value = {"1,3000", "3,2000"})
  public void correctness(int numThreads, int numCommitsPerThread) {
    verifyCommitsCorrectness(numThreads, numCommitsPerThread);
  }

  protected void verifyCommitsCorrectness(int numThreads, int numCommitsPerThread) {
    var refName = "ref-" + UUID.randomUUID();
    persistence.createReference(refName, Optional.empty());
    var committer =
        (CommitterWithStats<SimpleCommitObj, String>)
            persistence.createCommitter(refName, SimpleCommitObj.class, String.class);

    var totalRetries = new AtomicInteger(0);
    var totalTimeouts = new AtomicInteger(0);
    var totalSleepTimeMillis = new AtomicLong(0);
    var totalDurationNanos = new AtomicLong(0);

    try (var executor = Executors.newFixedThreadPool(numThreads)) {
      var futures = new ArrayList<CompletableFuture<?>>();
      for (int t = 0; t < numThreads; t++) {
        var thread = t;
        futures.add(
            CompletableFuture.runAsync(
                () -> {
                  var retryTimeouts = 0;
                  for (int c = 0; c < numCommitsPerThread; c++) {
                    try {
                      committer.commit(
                          (state, refObjSupplier) -> {
                            var refObj = refObjSupplier.get();
                            var commitSeqPerThread =
                                refObj
                                    .map(SimpleCommitObj::commitSeqPerThread)
                                    .map(ArrayList::new)
                                    .orElseGet(
                                        () -> {
                                          var initial = new ArrayList<Integer>(numThreads);
                                          for (int i = 0; i < numThreads; i++) {
                                            initial.add(-1);
                                          }
                                          return initial;
                                        });

                            commitSeqPerThread.set(thread, commitSeqPerThread.get(thread) + 1);

                            return state.commitResult(
                                "foo",
                                ImmutableSimpleCommitObj.builder()
                                    .commitSeqPerThread(commitSeqPerThread)
                                    .thread(thread),
                                refObj);
                          },
                          (result, retries, sleepTimeMillis, durationNanos) -> {
                            totalRetries.addAndGet(retries);
                            totalSleepTimeMillis.addAndGet(sleepTimeMillis);
                            totalDurationNanos.addAndGet(durationNanos);
                            switch (result) {
                              case SUCCESS, CONFLICT, ERROR -> {}
                              case TIMEOUT -> totalTimeouts.incrementAndGet();
                              default -> fail("Unexpected/invalid result %s", result);
                            }
                          });
                    } catch (RetryTimeoutException e) {
                      if (retryTimeouts++ >= 10) {
                        throw new RuntimeException(e);
                      }
                    } catch (CommitException e) {
                      throw new RuntimeException(e);
                    }
                  }
                },
                executor));
      }

      soft.assertThatCode(
              () ->
                  CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                      .get(5, TimeUnit.MINUTES))
          .doesNotThrowAnyException();
    }

    System.out.printf(
        """
            Threads: %d
            Commits per thread: %d
            Retry timeouts: %d
            Total retries: %d
            Total sleep time: %s
            Total work duration: %s
            """,
        numThreads,
        numCommitsPerThread,
        totalTimeouts.get(),
        totalRetries.get(),
        Duration.ofMillis(totalSleepTimeMillis.get()),
        Duration.ofSeconds(
            TimeUnit.NANOSECONDS.toSeconds(totalDurationNanos.get()),
            totalDurationNanos.get() % TimeUnit.SECONDS.toNanos(1)));

    soft.assertThat(totalTimeouts)
        .describedAs(
            "Retry timeouts: %d, retries total: %d", totalTimeouts.get(), totalRetries.get())
        .hasValue(0);

    var commitThreadSeqDesc =
        IntStream.range(0, numThreads)
            .boxed()
            .collect(Collectors.toMap(t -> t, t -> new AtomicInteger(numCommitsPerThread - 1)));

    Streams.stream(
            persistence.commits().commitLog(refName, OptionalLong.empty(), SimpleCommitObj.class))
        .forEach(
            co -> {
              var expected = commitThreadSeqDesc.get(co.thread());
              var expectedSeq = expected.get();
              var commitSeq = co.commitSeqPerThread().get(co.thread());
              soft.assertThat(commitSeq)
                  .describedAs(
                      "Descending (normal order) - thread %d, commit %d", co.thread(), commitSeq)
                  .isEqualTo(expectedSeq);
              expected.set(commitSeq - 1);
            });

    soft.assertThat(commitThreadSeqDesc.values().stream().map(AtomicInteger::get))
        .describedAs("Descending (normal order)")
        .containsExactlyElementsOf(IntStream.range(0, numThreads).mapToObj(x -> -1).toList());

    var commitThreadSeq =
        IntStream.range(0, numThreads)
            .boxed()
            .collect(Collectors.toMap(t -> t, t -> new AtomicInteger(0)));

    Streams.stream(persistence.commits().commitLogReversed(refName, -1L, SimpleCommitObj.class))
        .forEach(
            co -> {
              var expected = commitThreadSeq.get(co.thread());
              var expectedSeq = expected.get();
              var commitSeq = co.commitSeqPerThread().get(co.thread());
              soft.assertThat(commitSeq)
                  .describedAs(
                      "Ascending (reverse order) - thread %d, commit %d", co.thread(), commitSeq)
                  .isEqualTo(expectedSeq);
              expected.set(commitSeq + 1);
            });

    soft.assertThat(commitThreadSeq.values().stream().map(AtomicInteger::get))
        .describedAs("Ascending (reverse order)")
        .containsExactlyElementsOf(
            IntStream.range(0, numThreads).mapToObj(x -> numCommitsPerThread).toList());
  }

  @ParameterizedTest
  @MethodSource
  public void bigIndex(int numCommits, int additionsPerCommit) throws Exception {
    var refName = "ref-" + UUID.randomUUID();
    persistence.createReference(refName, Optional.empty());
    var committer =
        (CommitterWithStats<SimpleCommitObj, String>)
            persistence.createCommitter(refName, SimpleCommitObj.class, String.class);

    var objIdGen = (IntFunction<ObjRef>) i -> objRef("foo", i, 1);
    var keyGen = (IntFunction<IndexKey>) i -> IndexKey.key("my-table." + i + ".suffix");

    var table = 0;
    for (var i = 0; i < numCommits; i++, table += additionsPerCommit) {

      var off = table;

      committer.commit(
          (state, refObjSupplier) -> {
            var refObj = refObjSupplier.get();

            var index =
                refObj
                    .map(
                        r ->
                            requireNonNull(r.index())
                                .asUpdatableIndex(persistence, OBJ_REF_SERIALIZER))
                    .orElseGet(
                        () -> IndexContainer.newUpdatableIndex(persistence, OBJ_REF_SERIALIZER));

            for (var t = off; t < off + additionsPerCommit; t++) {
              index.put(keyGen.apply(t), objIdGen.apply(t));
            }

            var newRefObj =
                ImmutableSimpleCommitObj.builder()
                    .index(index.toIndexed("idx-", state::writeOrReplace))
                    .thread(0);

            return state.commitResult("result", newRefObj, refObj);
          });

      var head = persistence.fetchReferenceHead(refName, SimpleCommitObj.class);
      assertThat(head).isNotEmpty();
      var idx = requireNonNull(head.get().index()).indexForRead(persistence, OBJ_REF_SERIALIZER);
      soft.assertThat(IntStream.range(0, table + additionsPerCommit))
          .allMatch(t -> objIdGen.apply(t).equals(idx.get(keyGen.apply(t))));
    }
  }

  static Stream<Arguments> bigIndex() {
    return Stream.of(arguments(3, 10), arguments(50, 1000));
  }
}
