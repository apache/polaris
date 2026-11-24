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

import java.util.Optional;
import java.util.Set;
import org.apache.polaris.tasks.api.TaskBehaviorId;
import org.apache.polaris.tasks.api.TaskParameter;
import org.apache.polaris.tasks.api.TaskResult;
import org.apache.polaris.tasks.api.TaskSerializationResolver;

/**
 * An implementation of this interface is provided by the runtime environment to resolve available
 * task behaviors.
 *
 * <p>Runtime environments are usually but not exclusively CDI runtimes.
 */
public interface TaskBehaviorResolver extends TaskSerializationResolver {
  <PARAM extends TaskParameter, RESULT extends TaskResult>
      Optional<TaskBehavior<PARAM, RESULT>> resolveBehavior(TaskBehaviorId behaviorId);

  Set<TaskBehaviorId> knownTaskBehaviorIds();
}
