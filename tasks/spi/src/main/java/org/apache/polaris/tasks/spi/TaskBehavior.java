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
package org.apache.polaris.tasks.spi;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.Optional;
import org.apache.polaris.tasks.api.TaskBehaviorId;
import org.apache.polaris.tasks.api.TaskId;
import org.apache.polaris.tasks.api.TaskParameter;
import org.apache.polaris.tasks.api.TaskResult;
import org.apache.polaris.tasks.api.Tasks;

/**
 * Task behaviors provide/define how tasks behave and how tasks are handled.
 *
 * <p>Implementations are provided as {@link ApplicationScoped @ApplicationScoped} beans.
 *
 * <p>Each behavior must define its {@linkplain TaskParameter input parameter} and {@linkplain
 * TaskResult result} types.
 */
public interface TaskBehavior<PARAM extends TaskParameter, RESULT extends TaskResult> {
  /** Human-readable name. */
  String name();

  /** Globally unique ID of the task behavior. */
  TaskBehaviorId id();

  Class<PARAM> paramType();

  Class<RESULT> resultType();

  /**
   * Provide a task-runnable that can perform the task behavior's operation.
   *
   * <p>No guarantees are made about whether CDI is available and which scope is active.
   *
   * <p>Implementations must <em>never</em> assume that any values or context information from a
   * "scheduling" context (think: CDI request context, even propagated) is available. This is
   * neither supported by the CDI specification nor practically doable, especially considering that
   * task functions are executed "far" in the future and/or on a different node.
   */
  TaskFunction<PARAM, RESULT> function();

  /**
   * Generate a task ID.
   *
   * <p>Task behavior implementations produce either new, unique task IDs per submission or a
   * deterministic task IDs based on the task parameters.
   *
   * @param taskIdGenerator task ID generator to use to generate task IDs
   * @param param task parameter
   */
  TaskId generateTaskId(TaskIdGenerator taskIdGenerator, PARAM param);

  /**
   * Provides the instant at which a newly submitted task shall be executed, default to immediate
   * execution.
   *
   * @param param task parameter
   * @param now instant to assume for "now"
   */
  default Instant initialDelay(PARAM param, Instant now) {
    return now;
  }

  /**
   * Provides the instant at which a successfully completed task shall be executed again, default is
   * no rescheduling.
   *
   * @param param task parameter
   * @param result result of the task execution
   * @param now instant to assume for "now"
   */
  default Optional<Instant> rescheduleAfterSuccessAt(PARAM param, RESULT result, Instant now) {
    return Optional.empty();
  }

  /**
   * Provides the instant at after which a failed task execution shall be attempted again, default
   * is no rescheduling.
   *
   * @param param task parameter
   * @param error value describing the error that happened.
   * @param now instant to assume for "now"
   */
  default Optional<Instant> rescheduleAfterFailureAt(
      PARAM param, TaskExecutionError error, Instant now) {
    return Optional.empty();
  }

  /**
   * Called to combine task parameters from the persisted state and the requested state.
   *
   * <p>When a task is re-submitting from a {@linkplain Tasks#submit(TaskBehaviorId, TaskParameter,
   * Class) task submission}. Default behavior is to use the <em>persisted</em> task parameter.
   *
   * @param previousParam the task parameter of the <em>previous</em> execution
   * @param submitParam the task parameter passed to {@link Tasks#submit(TaskBehaviorId,
   *     TaskParameter, Class)}
   */
  default PARAM combinePersistedAndRequestedParam(PARAM previousParam, PARAM submitParam) {
    return previousParam;
  }
}
