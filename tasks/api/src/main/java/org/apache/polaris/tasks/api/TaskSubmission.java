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

import java.util.concurrent.CompletionStage;

/**
 * Represents the result of a {@linkplain Tasks#submit(TaskBehaviorId, String, TaskParameter, Class)
 * task submission}.
 *
 * @param <RESULT> task execution result type.
 */
public interface TaskSubmission<RESULT> extends AutoCloseable {
  TaskId taskId();

  /**
   * Returns a completion stage to subscribe to the result of a task execution.
   *
   * <p>This should only be used for tasks that will execute soon. Do not use this for tasks
   * scheduled far in the future or for recurring tasks.
   *
   * <p>It only makes sense to subscribe to the execution of a non-repeating task. The behavior of
   * the returned completion stage is undefined for rescheduled tasks. Implementations may return
   * the result of the current or next task run or may not yield a result as long as the task gets
   * rescheduled.
   */
  CompletionStage<RESULT> completionStage();

  @Override
  void close();
}
