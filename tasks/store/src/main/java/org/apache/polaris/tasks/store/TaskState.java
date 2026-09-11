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

import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.tasks.api.TaskBehaviorId;
import org.apache.polaris.tasks.api.TaskId;
import org.apache.polaris.tasks.api.TaskParameter;
import org.apache.polaris.tasks.api.TaskResult;
import org.apache.polaris.tasks.spi.TaskBehavior;
import org.apache.polaris.tasks.spi.TaskExecutionError;
import org.immutables.value.Value;

/** Represents the state of a particular task execution. */
@PolarisImmutable
@JsonSerialize(as = ImmutableTaskState.class)
@JsonDeserialize(as = ImmutableTaskState.class)
public interface TaskState {
  TaskId taskId();

  /**
   * ID of the {@linkplain TaskBehavior behavior}.
   *
   * <p>Users of a {@link TaskState} object must be aware that a {@linkplain TaskBehavior behavior}
   * may not be available. Unknown behaviors must not be prematurely deleted, because another
   * instance might be able to handle those behaviors. Rolling upgrades are legit use cases for the
   * case hitting an unknown behavior.
   */
  TaskBehaviorId behaviorId();

  /** The current task status. */
  TaskStatus status();

  /** Represents an error message, intended for humans, not machines. */
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<TaskExecutionError> error();

  /** Represents the earliest timestamp when a task can be run (again). */
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<Instant> scheduleNotBefore();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<Instant> executedAt();

  /**
   * Represents the earliest timestamp when a task service can assume that a {@link
   * TaskStatus#RUNNING RUNNING} task is lost and should be re-started. Only valid for {@link
   * TaskStatus#RUNNING RUNNING}.
   */
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<Instant> lostNotBefore();

  TaskParameter taskParam();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<TaskResult> taskResult();

  static ImmutableTaskState.Builder builder() {
    return ImmutableTaskState.builder();
  }

  /**
   * Checks whether the task is eligible to be scheduled.
   *
   * <p>A task is eligible to be scheduled if:
   *
   * <ul>
   *   <li>its state is {@link TaskStatus#SCHEDULED}, {@link TaskStatus#FAILURE} or {@link
   *       TaskStatus#SUCCESS} and its {@linkplain #scheduleNotBefore()} is not in the future or
   *   <li>its state is {@link TaskStatus#RUNNING} and its {@linkplain #lostNotBefore()} is not in
   *       the future.
   * </ul>
   *
   * <p>The timestamps are compared against the given value for {@code now}.
   */
  @SuppressWarnings("UnnecessaryDefault")
  @JsonIgnore
  default boolean canBeScheduled(Instant now) {
    var testInstant =
        switch (status()) {
          case SCHEDULED, FAILURE, SUCCESS -> scheduleNotBefore();
          case RUNNING -> lostNotBefore();
          default -> throw new IllegalStateException("Unexpected status " + status());
        };
    if (testInstant.isEmpty()) {
      return false;
    }
    var instant = testInstant.get();
    return now.compareTo(instant) >= 0;
  }

  @Value.Check
  default void check() {
    switch (status()) {
      case SCHEDULED -> {
        check(executedAt(), false, "executedAt", status());
        check(scheduleNotBefore(), true, "scheduleNotBefore", status());
        check(lostNotBefore(), false, "lostNotBefore", status());
        check(error(), false, "error", status());
        check(taskResult(), false, "taskResult", status());
      }
      case RUNNING -> {
        check(executedAt(), true, "executedAt", status());
        checkState(
            lostNotBefore().isPresent()
                && executedAt().isPresent()
                && lostNotBefore().get().compareTo(executedAt().get()) > 0,
            "lostNotBefore must be present and greater than executedAt for status RUNNING");
        check(error(), false, "error", status());
        check(taskResult(), false, "taskResult", status());
      }
      case SUCCESS -> {
        check(executedAt(), true, "executedAt", status());
        check(lostNotBefore(), false, "lostNotBefore", status());
        check(error(), false, "error", status());
        check(taskResult(), true, "taskResult", status());
      }
      case FAILURE -> {
        check(executedAt(), true, "executedAt", status());
        check(lostNotBefore(), false, "lostNotBefore", status());
        check(error(), true, "error", status());
        check(taskResult(), false, "taskResult", status());
      }
      default -> throw new IllegalStateException("Unexpected status " + status());
    }
  }

  static void check(Optional<?> optional, boolean present, String name, TaskStatus status) {
    checkState(
        optional.isPresent() == present,
        "%s must be %s for status %s",
        name,
        present ? "present" : "absent",
        status);
  }
}
