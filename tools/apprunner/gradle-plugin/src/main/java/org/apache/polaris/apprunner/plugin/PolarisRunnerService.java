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
package org.apache.polaris.apprunner.plugin;

import java.util.IdentityHashMap;
import java.util.Map;
import org.gradle.api.Task;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PolarisRunnerService
    implements BuildService<BuildServiceParameters.None>, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisRunnerService.class);

  private final Map<Task, ProcessState> processes = new IdentityHashMap<>();

  @Override
  public void close() {
    synchronized (processes) {
      if (!processes.isEmpty()) {
        LOGGER.warn("Cleaning up {} Polaris Quarkus servers", processes.size());
      }
      for (var state : processes.values()) {
        state.quarkusStop(LOGGER);
      }
    }
  }

  public void register(ProcessState processState, Task task) {
    synchronized (processes) {
      processes.put(task, processState);
    }
  }

  public void finished(Task task) {
    ProcessState state;
    synchronized (processes) {
      state = processes.remove(task);
    }
    if (state != null) {
      state.quarkusStop(task.getLogger());
    }
  }
}
