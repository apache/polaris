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
package org.apache.polaris.tasks.api;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.polaris.immutables.PolarisImmutable;

/** Advanced configuration options for distributed and converged task handling. */
@ConfigMapping(prefix = "polaris.coordinated-tasks")
@PolarisImmutable
@JsonSerialize(as = ImmutableTasksConfiguration.class)
@JsonDeserialize(as = ImmutableTasksConfiguration.class)
@JsonTypeName(TasksConfiguration.TYPE_ID)
public interface TasksConfiguration {
  String TYPE_ID = "coordinated-tasks";

  int DEFAULT_RETAINED_HISTORY_LIMIT = 5;

  /** Number of history entries for task scheduling/submission updates. */
  @WithDefault("" + DEFAULT_RETAINED_HISTORY_LIMIT)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalInt retainedHistoryLimit();

  String DEFAULT_UPDATE_STATE_INTERVAL_STRING = "PT1S";
  Duration DEFAULT_UPDATE_STATE_INTERVAL = Duration.parse(DEFAULT_UPDATE_STATE_INTERVAL_STRING);

  /**
   * The state of running tasks is regularly updated in the task store. This parameter defines the
   * interval of these updates.
   */
  @WithDefault(DEFAULT_UPDATE_STATE_INTERVAL_STRING)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> updateStateInterval();

  String DEFAULT_LOST_NOT_BEFORE_STRING = "PT5M";
  Duration DEFAULT_LOST_NOT_BEFORE = Duration.parse(DEFAULT_LOST_NOT_BEFORE_STRING);

  /**
   * Tasks in "running" status are considered "lost" when the last state-update was before the
   * duration specified by this parameter.
   *
   * <p>Running tasks regularly update the task store with their current state (see {@code
   * updateStateInterval}). Tasks states with a {@code lostNotBefore} in the past are considered as
   * "dead", which means that the task's execution can be picked up by another node.
   *
   * <p>This value should be large enough and quite a multiple of {@code updateStateInterval}, in
   * the range of 1 or more minutes.
   */
  @WithDefault(DEFAULT_LOST_NOT_BEFORE_STRING)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> lostNotBeforeDuration();

  String DEFAULT_RUNNABLE_TASKS_POLL_INTERVAL_STRING = "PT5S";
  Duration DEFAULT_RUNNABLE_TASKS_POLL_INTERVAL =
      Duration.parse(DEFAULT_RUNNABLE_TASKS_POLL_INTERVAL_STRING);

  /** Interval at which nodes check for scheduled tasks that eligible to run. */
  @WithDefault(DEFAULT_RUNNABLE_TASKS_POLL_INTERVAL_STRING)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> runnableTasksPollInterval();

  String DEFAULT_REMOTE_TASK_POLL_INTERVAL_STRING = "PT0.5S";
  Duration DEFAULT_REMOTE_TASK_POLL_INTERVAL =
      Duration.parse(DEFAULT_REMOTE_TASK_POLL_INTERVAL_STRING);

  /**
   * The interval at which the state of a task running on another node is being polled.
   *
   * <p>Call sites that subscribe to the result of a task running on a different node use this value
   * as the polling interval.
   *
   * <p>This parameter should be lower than the duration defined by the parameter {@code
   * updateStateInterval}.
   */
  @WithDefault(DEFAULT_REMOTE_TASK_POLL_INTERVAL_STRING)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> remoteTaskPollInterval();

  String DEFAULT_LOCAL_SCHEDULING_INTERVAL_STRING = "PT2S";
  Duration DEFAULT_LOCAL_SCHEDULING_INTERVAL =
      Duration.parse(DEFAULT_LOCAL_SCHEDULING_INTERVAL_STRING);

  /**
   * Implementations may consider tasks that are eligible to be executed within this interval for
   * immediate local execution.
   *
   * <p>This is rather a performance optimization for implementations to avoid the additional work
   * for task acquisitions. This value implies no guarantee that this optimization will be applied.
   *
   * @hidden advanced usage, not in public docs
   */
  @WithDefault(DEFAULT_LOCAL_SCHEDULING_INTERVAL_STRING)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> localSchedulingInterval();

  static ImmutableTasksConfiguration.Builder builder() {
    return ImmutableTasksConfiguration.builder();
  }
}
