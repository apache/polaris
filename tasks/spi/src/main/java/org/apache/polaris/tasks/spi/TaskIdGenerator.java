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

import com.google.common.hash.PrimitiveSink;
import java.util.function.Consumer;
import org.apache.polaris.tasks.api.TaskId;
import org.apache.polaris.tasks.api.TaskParameter;
import org.apache.polaris.tasks.api.TaskResult;
import org.apache.polaris.tasks.api.Tasks;

public interface TaskIdGenerator {

  /** Used to generate a non-deterministic and unique task ID. */
  TaskId generateNonDeterministicUniqueId(String realmId);

  /**
   * Used to generate a deterministic task ID.
   *
   * <p>Call sites usually include relevant parameters of the respective attributes of a {@linkplain
   * TaskParameter task parameter} used to generate deterministic task IDs.
   *
   * <p>Deterministic task IDs are useful to converge requests for the same task to a single task
   * execution. {@link Tasks} implementations ensure that task submissions against the same task ID
   * are executed only once and all submissions share the same {@linkplain TaskResult task result}.
   *
   * <p>Implementations must include the given {@code realmId} in the task ID generation
   * respectively the calculation.
   */
  // TODO this makes Guava a part of this API - acceptable in this case?
  @SuppressWarnings("UnstableApiUsage")
  TaskId generateDeterministicId(String realmId, Consumer<PrimitiveSink> funnel);
}
