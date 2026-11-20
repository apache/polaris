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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

/** Bounds asynchronous requests and drains submitted requests when closed. */
final class LimitedConcurrentRequests implements AutoCloseable {
  private final Semaphore permits;

  @GuardedBy("this")
  private Throwable failure;

  @GuardedBy("this")
  private int started;

  @GuardedBy("this")
  private int finished;

  LimitedConcurrentRequests(int maxChildQueries) {
    permits = new Semaphore(maxChildQueries);
  }

  void submit(Supplier<? extends CompletionStage<?>> request) {
    try {
      permits.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    synchronized (this) {
      started++;
    }

    CompletionStage<?> stage;
    try {
      stage = requireNonNull(request.get());
    } catch (RuntimeException | Error e) {
      completed(null);
      throw e;
    }
    stage.whenComplete((result, error) -> completed(error));
  }

  private void completed(Throwable error) {
    while (error instanceof CompletionException && error.getCause() != null) {
      error = error.getCause();
    }
    synchronized (this) {
      if (error != null) {
        if (failure == null) {
          failure = error;
        } else if (failure != error) {
          failure.addSuppressed(error);
        }
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
        // Drain requests even when interrupted; restore the flag before returning or throwing.
        while (finished != started) {
          try {
            wait();
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
        if (failure instanceof RuntimeException re) {
          throw re;
        }
        if (failure != null) {
          throw new RuntimeException(failure);
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
