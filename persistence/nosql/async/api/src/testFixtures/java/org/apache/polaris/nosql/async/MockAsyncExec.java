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
package org.apache.polaris.nosql.async;

import static com.google.common.base.Preconditions.checkState;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.polaris.ids.api.MonotonicClock;

/**
 * An {@link AsyncExec} implementation used in tests that need to verify the interaction with {@link
 * AsyncExec}. Execution of submitted tasks needs to be manually triggered by the tests that use
 * this class.
 *
 * <p>This implementation is best used in combination with {@code MutableMonotonicClock} to allow
 * assertions on scheduled/execution timestamps.
 */
public class MockAsyncExec implements AsyncExec {

  public MockAsyncExec(MonotonicClock clock) {
    this.clock = clock;
  }

  public record CallResult<T>(boolean called, T result, Throwable failure) {}

  public final class Task<T> {
    private final Callable<T> callable;
    private final Duration initialDelay;
    private final Duration delay;
    public Instant nextExecution;
    public final MockCancelable<T> cancelable;

    Task(Callable<T> callable, Duration initialDelay, Duration delay) {
      this.callable = callable;
      this.initialDelay = initialDelay;
      this.delay = delay;
      this.nextExecution = clock.currentInstant().plus(initialDelay);
      this.cancelable = new MockCancelable<>();
    }

    public Duration initialDelay() {
      return initialDelay;
    }

    public Duration delay() {
      return delay;
    }

    public Instant nextExecution() {
      return nextExecution;
    }

    public boolean ready() {
      return ready(clock.currentInstant());
    }

    public boolean ready(Instant at) {
      return nextExecution.compareTo(at) <= 0;
    }

    public CallResult<T> call() {
      switch (cancelable.future.state()) {
        case CANCELLED -> throw new CancellationException();
        case FAILED ->
            throw new IllegalStateException("Already completed", cancelable.future.exceptionNow());
        case SUCCESS -> throw new IllegalStateException("Already completed");
        case RUNNING -> {}
        default -> throw new IllegalStateException("Unknown state " + cancelable.future.state());
      }
      try {
        var r = callable.call();
        if (delay == null) {
          cancelable.future.complete(r);
          done();
        } else {
          nextExecution = clock.currentInstant().plus(delay);
        }
        return new CallResult<>(true, r, null);
      } catch (Exception e) {
        cancelable.future.completeExceptionally(e);
        return new CallResult<>(true, null, e);
      }
    }

    private void done() {
      checkState(tasks.remove(Task.this));
    }

    public class MockCancelable<R> implements Cancelable<R> {
      private final CompletableFuture<R> future = new CompletableFuture<>();

      @Override
      public CompletionStage<R> completionStage() {
        return future;
      }

      @Override
      public void cancel() {
        future.cancel(true);
        done();
      }
    }
  }

  private final MonotonicClock clock;
  private final List<Task<?>> tasks = new ArrayList<>();

  public List<Task<?>> tasks() {
    return tasks;
  }

  public Optional<Task<?>> nextReady() {
    return nextReadyAt(clock.currentInstant());
  }

  public Optional<Task<?>> nextReadyAt(Instant at) {
    return tasks.stream().filter(t -> t.ready(at)).min(Comparator.comparing(Task::nextExecution));
  }

  public long readyCount() {
    return readyCount(clock.currentInstant());
  }

  public long readyCount(Instant at) {
    return tasks.stream().filter(t -> t.ready(at)).count();
  }

  public List<Task<?>> readyCallables() {
    var ready = new ArrayList<Task<?>>();
    for (var iter = tasks.iterator(); iter.hasNext(); ) {
      var task = iter.next();
      if (task.ready()) {
        ready.add(task);
        iter.remove();
      }
    }
    return ready;
  }

  @Override
  public Cancelable<Void> schedulePeriodic(
      Runnable runnable, Duration initialDelay, Duration delay) {
    var scheduled =
        new Task<Void>(
            () -> {
              runnable.run();
              return null;
            },
            initialDelay,
            delay);
    this.tasks.add(scheduled);
    return scheduled.cancelable;
  }

  @Override
  public <R> Cancelable<R> schedule(Callable<R> callable, Duration delay) {
    var scheduled = new Task<>(callable, delay, null);
    this.tasks.add(scheduled);
    return scheduled.cancelable;
  }
}
