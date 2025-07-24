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
package org.apache.polaris.tasks.store;

import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Predicate;
import org.apache.polaris.tasks.api.TaskId;
import org.apache.polaris.tasks.api.Tasks;

/** Persistence interface for {@link Tasks} implementations. */
public interface TaskStore {
  /**
   * Retrieve tasks with a {@linkplain TaskState#scheduleNotBefore() schedule timestamp} up to
   * (including) {@code now}.
   *
   * <p>While it would be programmatically easier to return a {@link java.util.stream.Stream} from
   * this function, letting it return a {@link List} helps keeping (backend database) resource usage
   * enclosed in the implementation.
   *
   * @param now maximum schedule-not-before timestamp. Entries that have no or a higher
   *     schedule-not-before timestamp are filtered out by the implementation before calling the
   *     {@code filter}, which allows pushing down this value as a predicate to the persistence
   *     layer.
   * @param filter filter to be matched, only entries for which this predicate yields {@code true}
   *     are returned.
   * @param limit maximum number of entries to return, empty means unlimited. Only eligible task
   *     that match the maximum schedule-not-before timestamp and pass the filter test are counted
   *     towards this value.
   * @return matching tasks, the order of elements within the returned list is undefined
   */
  @Nonnull
  List<TaskStoreResult> scheduledTasks(
      @Nonnull Instant now, @Nonnull Predicate<TaskStoreResult> filter, OptionalInt limit);

  /**
   * Retrieve the {@linkplain TaskState task state} and the {@linkplain TaskHandle task handle} to
   * it for the given ID.
   *
   * @throws IllegalArgumentException if the task does not exist
   */
  Optional<TaskStoreResult> fetchTask(@Nonnull TaskId taskId);

  /**
   * Fetch the task state for the given {@linkplain TaskHandle task handle}.
   *
   * @throws IllegalArgumentException if the task does not exist
   */
  Optional<TaskState> fetchTaskState(@Nonnull TaskHandle taskHandle);

  /**
   * Update the task state for the given ID.
   *
   * <p>The {@code updater} function might be called multiple times, for example when commit-retries
   * happen. Therefore {@code updater} function must be free of side effects.
   *
   * @param updater Update function, must be idempotent and expect to be invoked multiple times.
   *     Gets the current state as its input, may throw
   * @throws IllegalArgumentException if the task does not exist
   */
  Optional<TaskStoreResult> updateTask(@Nonnull TaskId taskId, @Nonnull TaskStateUpdater updater);

  @FunctionalInterface
  interface TaskStateUpdater {
    TaskChange update(@Nonnull Optional<TaskStoreResult> currentState);
  }

  interface TaskChange {
    record TaskNoChange() implements TaskChange {}

    record TaskRemove() implements TaskChange {}

    record TaskWrite(@Nonnull TaskState state) implements TaskChange {}

    record TaskUpdateAndRemove(@Nonnull TaskState state) implements TaskChange {}

    /** Performs no change. */
    static TaskChange noChange() {
      return new TaskNoChange();
    }

    /** Removes a task from the set of scheduled tasks. */
    static TaskChange remove() {
      return new TaskRemove();
    }

    /**
     * Updates an existing task to reflect the new state and removes it from the set of scheduled
     * tasks.
     */
    static TaskChange updateAndRemove(@Nonnull TaskState state) {
      return new TaskUpdateAndRemove(state);
    }

    /** Creates or updates a task to the given task state value. */
    static TaskChange write(@Nonnull TaskState state) {
      return new TaskWrite(state);
    }
  }
}
