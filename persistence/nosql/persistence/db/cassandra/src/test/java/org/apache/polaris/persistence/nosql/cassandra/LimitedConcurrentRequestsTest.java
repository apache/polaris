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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
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
                  assertThatThrownBy(requests::close).isSameAs(failure);
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

    assertThatThrownBy(requests::close).isSameAs(first).hasSuppressedException(second);
  }
}
