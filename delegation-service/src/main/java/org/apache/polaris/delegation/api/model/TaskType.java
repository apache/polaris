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
package org.apache.polaris.delegation.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Types of tasks that can be delegated to the Delegation Service.
 *
 * <p>This enum defines the various long-running, resource-intensive operations that can be
 * offloaded from the main Polaris catalog service.
 */
public enum TaskType {

  /**
   * Data file deletion task for DROP TABLE WITH PURGE operations. This is the initial task type
   * supported by the delegation service.
   */
  PURGE_TABLE("PURGE_TABLE");

  private final String value;

  TaskType(String value) {
    this.value = value;
  }

  @JsonValue
  public String getValue() {
    return value;
  }

  @JsonCreator
  public static TaskType fromValue(String value) {
    for (TaskType taskType : TaskType.values()) {
      if (taskType.value.equals(value)) {
        return taskType;
      }
    }
    throw new IllegalArgumentException("Unknown TaskType: " + value);
  }

  @Override
  public String toString() {
    return value;
  }
}
