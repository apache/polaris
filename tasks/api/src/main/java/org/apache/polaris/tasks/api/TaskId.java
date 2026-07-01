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

import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * Represents the ID of a task, provided by task behavior implementations.
 *
 * <p>Task behavior implementations are responsible for generating globally unique task IDs.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableTaskId.class)
@JsonDeserialize(as = ImmutableTaskId.class)
public interface TaskId {
  @Value.Parameter(order = 1)
  String realm();

  @Value.Parameter(order = 2)
  String id();

  static TaskId taskId(String realm, String id) {
    return ImmutableTaskId.of(realm, id);
  }

  @Value.Check
  default void check() {
    var idl = id().length();
    checkState(idl >= 3 && idl <= 128, "TaskId id must have a length of 3..128 characters");
  }
}
