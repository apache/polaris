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
package org.apache.polaris.persistence.nosql.cassandra;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(10)
class LimitedConcurrentRequestsTest {
  // Interrupt tests must not target workers that can be reused by unrelated tests.
  private static final Executor TEST_THREADS = task -> Thread.ofVirtual().start(task);

  @Test
  void acquiresPermitBeforeStartingRequest() throws Exception {
    var first = new CompletableFuture<Void>();
    var second = new CompletableFuture<Void>();
    var started = new CompletableFuture<Void>();
    try (var requests = new LimitedConcurrentRequests(1)) {
      requests.submit(() -> first);
      var submission =
          CompletableFuture.runAsync(
              () ->
                  requests.submit(() -> {
                    started.complete(null);
                    return second;
                  }),
              TEST_THREADS);
      try {
        assertThatThrownBy(() -> started.get(100, MILLISECONDS))
            .isInstanceOf(TimeoutException.class);
        first.complete(null);
        started.get(5, SECONDS);
      } finally {
        first.complete(null);
        second.complete(null);
        submission.get(5, SECONDS);
      }
    }
  }

  @Test
  void interruptedAcquisitionDoesNotStartOrCountRequest() throws Exception {
    var first = new CompletableFuture<Void>();
    var submittingThread = new CompletableFuture<Thread>();
    var secondStarted = new AtomicBoolean();
    try (var requests = new LimitedConcurrentRequests(1)) {
      requests.submit(() -> first);
      var submission =
          CompletableFuture.runAsync(
              () -> {
                submittingThread.complete(Thread.currentThread());
                try {
                  assertThatThrownBy(() ->
                          requests.submit(() -> {
                            secondStarted.set(true);
                            return CompletableFuture.completedFuture(null);
                          }))
                      .hasCauseInstanceOf(InterruptedException.class);
                  assertThat(Thread.currentThread().isInterrupted()).isTrue();
                } finally {
                  Thread.interrupted();
                }
              },
              TEST_THREADS);
      try {
        submittingThread.get(5, SECONDS).interrupt();
        submission.get(5, SECONDS);
        assertThat(secondStarted).isFalse();
      } finally {
        first.complete(null);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void closeDrainsRequestsAndRestoresInterrupt(boolean fail) throws Exception {
    var pending = new CompletableFuture<Void>();
    var closingThread = new CompletableFuture<Thread>();
    var requests = new LimitedConcurrentRequests(1);
    var failure = new IllegalStateException("request failed");
    requests.submit(() -> pending);
    var closed =
        CompletableFuture.supplyAsync(
            () -> {
              closingThread.complete(Thread.currentThread());
              try {
                if (fail) {
                  assertThatThrownBy(requests::close)
                      .hasMessage("Cassandra requests failed")
                      .hasSuppressedException(failure);
                } else {
                  requests.close();
                }
                return Thread.currentThread().isInterrupted();
              } finally {
                Thread.interrupted();
              }
            },
            TEST_THREADS);
    try {
      closingThread.get(5, SECONDS).interrupt();
      assertThatThrownBy(() -> closed.get(100, MILLISECONDS)).isInstanceOf(TimeoutException.class);
    } finally {
      if (fail) {
        pending.completeExceptionally(failure);
      } else {
        pending.complete(null);
      }
    }
    assertThat(closed.get(5, SECONDS)).isTrue();
  }

  @Test
  void synchronousSubmissionFailureReleasesPermit() {
    try (var requests = new LimitedConcurrentRequests(1)) {
      var failure = new IllegalStateException("prepare failed");
      assertThatThrownBy(() ->
              requests.submit(() -> {
                throw failure;
              }))
          .isSameAs(failure);
      requests.submit(() -> CompletableFuture.completedFuture(null));
    }
  }

  @Test
  void aggregatesUnwrappedFailuresWithoutSelfSuppression() {
    var first = new IllegalStateException("first");
    var second = new IllegalStateException("second");
    var requests = new LimitedConcurrentRequests(1);
    requests.submit(() -> CompletableFuture.failedFuture(new CompletionException(first)));
    requests.submit(() -> CompletableFuture.failedFuture(new CompletionException(second)));
    requests.submit(() -> CompletableFuture.failedFuture(first));

    var thrown = catchThrowable(requests::close);
    assertThat(thrown).hasMessage("Cassandra requests failed");
    assertThat(thrown.getSuppressed()).containsExactly(first, second, first);
    assertThat(first.getSuppressed()).isEmpty();
    assertThat(second.getSuppressed()).isEmpty();
  }

  @Test
  void runStopsProducerAndDrainsSubmissionAndAsyncFailures() throws Exception {
    var pending = new CompletableFuture<Void>();
    var submitted = new CompletableFuture<Void>();
    var submissionFailure = new IllegalArgumentException("submission failed");
    var asyncFailure = new IllegalStateException("async failed");
    var continued = new AtomicBoolean();
    var finished =
        CompletableFuture.supplyAsync(
            () ->
                catchThrowable(() ->
                    LimitedConcurrentRequests.run(
                        2,
                        requests -> {
                          requests.submit(() -> pending);
                          submitted.complete(null);
                          requests.submit(() -> {
                            throw submissionFailure;
                          });
                          continued.set(true);
                        })),
            TEST_THREADS);
    try {
      submitted.get(5, SECONDS);
      assertThatThrownBy(() -> finished.get(100, MILLISECONDS))
          .isInstanceOf(TimeoutException.class);
    } finally {
      pending.completeExceptionally(asyncFailure);
    }
    var thrown = finished.get(5, SECONDS);
    assertThat(thrown).hasMessage("Cassandra requests failed");
    assertThat(thrown.getSuppressed()).containsExactlyInAnyOrder(submissionFailure, asyncFailure);
    assertThat(continued).isFalse();
    assertThat(submissionFailure.getSuppressed()).isEmpty();
  }

  @Test
  void runDrainsAfterInterruptedAcquisition() throws Exception {
    var pending = new CompletableFuture<Void>();
    var submitting = new CompletableFuture<Thread>();
    var started = new AtomicBoolean();
    var finished =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                var failure =
                    catchThrowable(() ->
                        LimitedConcurrentRequests.run(
                            1,
                            requests -> {
                              requests.submit(() -> pending);
                              submitting.complete(Thread.currentThread());
                              requests.submit(() -> {
                                started.set(true);
                                return CompletableFuture.completedFuture(null);
                              });
                            }));
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
                return failure;
              } finally {
                Thread.interrupted();
              }
            },
            TEST_THREADS);
    try {
      submitting.get(5, SECONDS).interrupt();
      assertThatThrownBy(() -> finished.get(100, MILLISECONDS))
          .isInstanceOf(TimeoutException.class);
    } finally {
      pending.complete(null);
    }
    var failure = finished.get(5, SECONDS);
    assertThat(failure).hasMessage("Cassandra requests failed");
    assertThat(failure.getSuppressed())
        .singleElement()
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InterruptedException.class));
    assertThat(started).isFalse();
  }

  @Test
  void runPreservesFatalSubmissionErrors() {
    var failure = new AssertionError("fatal");
    assertThatThrownBy(() ->
            LimitedConcurrentRequests.run(
                1,
                requests ->
                    requests.submit(() -> {
                      throw failure;
                    })))
        .isSameAs(failure);
  }

  @Test
  void submitAfterCloseDoesNotStartRequest() {
    var requests = new LimitedConcurrentRequests(1);
    requests.close();
    var started = new AtomicBoolean();
    assertThatThrownBy(() ->
            requests.submit(() -> {
              started.set(true);
              return CompletableFuture.completedFuture(null);
            }))
        .isInstanceOf(IllegalStateException.class);
    assertThat(started).isFalse();
  }

  @Test
  void closeRejectsSubmissionsWaitingForPermit() throws Exception {
    var acquiring = new CountDownLatch(3);
    var closeStarted = new CountDownLatch(1);
    var pending = new CompletableFuture<Void>();
    var started = new AtomicBoolean();
    var requests =
        new LimitedConcurrentRequests(1) {
          @Override
          void beforeAcquire() {
            acquiring.countDown();
          }

          @Override
          void onClosed() {
            closeStarted.countDown();
          }
        };
    requests.submit(() -> pending);
    var submissions =
        IntStream.range(0, 2)
            .mapToObj(ignored ->
                CompletableFuture.supplyAsync(
                    () ->
                        catchThrowable(() ->
                            requests.submit(() -> {
                              started.set(true);
                              return CompletableFuture.completedFuture(null);
                            })),
                    TEST_THREADS))
            .toList();
    CompletableFuture<Void> closing = null;
    try {
      // Both submitters passed the first open check, but the occupied permit prevents acceptance.
      assertThat(acquiring.await(5, SECONDS)).isTrue();
      closing = CompletableFuture.runAsync(requests::close, TEST_THREADS);
      assertThat(closeStarted.await(5, SECONDS)).isTrue();
      pending.complete(null);
      for (var submission : submissions) {
        assertThat(submission.get(5, SECONDS)).isInstanceOf(IllegalStateException.class);
      }
      closing.get(5, SECONDS);
      assertThat(started).isFalse();
    } finally {
      pending.complete(null);
      // Both waiters must finish: rejecting the first must return its permit to the second.
      for (var submission : submissions) {
        submission.get(5, SECONDS);
      }
      if (closing != null) {
        closing.get(5, SECONDS);
      }
    }
  }

  @Test
  void closeWaitsForSupplierAndRequestCompletion() throws Exception {
    var supplierEntered = new CountDownLatch(1);
    var closeStarted = new CountDownLatch(1);
    var releaseSupplier = new CompletableFuture<Void>();
    var pending = new CompletableFuture<Void>();
    var requests =
        new LimitedConcurrentRequests(1) {
          @Override
          void onClosed() {
            closeStarted.countDown();
          }
        };
    var submission =
        CompletableFuture.runAsync(
            () ->
                requests.submit(() -> {
                  supplierEntered.countDown();
                  assertThat(releaseSupplier).succeedsWithin(5, SECONDS);
                  return pending;
                }),
            TEST_THREADS);
    CompletableFuture<Void> closing = null;
    try {
      assertThat(supplierEntered.await(5, SECONDS)).isTrue();
      closing = CompletableFuture.runAsync(requests::close, TEST_THREADS);
      assertThat(closeStarted.await(5, SECONDS)).isTrue();
      assertThat(closing).isNotDone();
      releaseSupplier.complete(null);
      submission.get(5, SECONDS);
      assertThat(closing).isNotDone();
      pending.complete(null);
      closing.get(5, SECONDS);
    } finally {
      releaseSupplier.complete(null);
      pending.complete(null);
      submission.get(5, SECONDS);
      if (closing != null) {
        closing.get(5, SECONDS);
      }
    }
  }
}
