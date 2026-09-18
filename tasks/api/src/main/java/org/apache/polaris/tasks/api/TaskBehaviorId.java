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

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/** Task behavior ID, as a type safe holder. */
@PolarisImmutable
@JsonSerialize(as = ImmutableTaskBehaviorId.class)
@JsonDeserialize(as = ImmutableTaskBehaviorId.class)
public interface TaskBehaviorId {
  @JsonValue
  @Value.Parameter
  String id();

  static TaskBehaviorId taskBehaviorId(String id) {
    return ImmutableTaskBehaviorId.of(id);
  }

  @Value.Check
  default void check() {
    var idl = id().length();
    checkState(idl >= 5 && idl <= 48, "TaskBehaviorId id must have a length of 5..48 characters");
  }
}
