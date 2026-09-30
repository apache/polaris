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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Bounds asynchronous requests and drains submitted requests when closed. */
class LimitedConcurrentRequests implements AutoCloseable {
  private final Semaphore permits;

  @GuardedBy("this")
  private RuntimeException failure;

  @GuardedBy("this")
  private boolean closed;

  @GuardedBy("this")
  private int started;

  @GuardedBy("this")
  private int finished;

  LimitedConcurrentRequests(int maxChildQueries) {
    permits = new Semaphore(maxChildQueries);
  }

  static void run(int maxChildQueries, Consumer<LimitedConcurrentRequests> submitRequests) {
    try (var requests = new LimitedConcurrentRequests(maxChildQueries)) {
      try {
        submitRequests.accept(requests);
      } catch (RuntimeException e) {
        // Stop submitting, then drain and report submission and async failures as siblings.
        requests.noteFailure(e);
      }
    }
  }

  @VisibleForTesting
  void beforeAcquire() {}

  /** Invoked with the monitor held; test overrides must not block. */
  @VisibleForTesting
  void onClosed() {}

  void submit(Supplier<? extends CompletionStage<?>> request) {
    checkOpen();
    beforeAcquire();
    try {
      permits.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    synchronized (this) {
      // close() may have started while this submission was waiting for a permit.
      if (closed) {
        permits.release();
        checkOpen();
      }
      started++;
    }

    CompletionStage<?> stage;
    try {
      stage = requireNonNull(request.get());
    } catch (RuntimeException | Error e) {
      // run() records submission failures after stopping the producer. Recording and throwing
      // the same exception here would let try-with-resources attempt self-suppression.
      completed(null);
      throw e;
    }
    stage.whenComplete((result, error) -> completed(error));
  }

  private synchronized void checkOpen() {
    if (closed) {
      throw new IllegalStateException("Cannot submit requests after close");
    }
  }

  private synchronized void noteFailure(Throwable error) {
    while (error instanceof CompletionException && error.getCause() != null) {
      error = error.getCause();
    }
    if (failure == null) {
      failure = new RuntimeException("Cassandra requests failed");
    }
    if (failure != error) {
      // Requests are siblings; do not attach later failures to the first driver exception.
      failure.addSuppressed(error);
    }
  }

  private void completed(Throwable error) {
    synchronized (this) {
      if (error != null) {
        noteFailure(error);
      }
      finished++;
      notifyAll();
    }
    permits.release();
  }

  @Override
  public void close() {
    boolean interrupted = false;
    try {
      synchronized (this) {
        closed = true;
        onClosed();
        // Interrupting the caller does not cancel mutations already sent to Cassandra. Drain them
        // before returning so late writes cannot race the caller's cleanup, then restore the flag.
        while (finished != started) {
          try {
            wait();
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
        if (failure != null) {
          throw failure;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
