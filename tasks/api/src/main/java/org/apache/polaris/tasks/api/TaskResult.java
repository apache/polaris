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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Base interface for task result objects.
 *
 * <p>Each task behavior defines exactly one task result type.
 *
 * <p>This is the necessary base type for Jackson type polymorphism.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
public interface TaskResult {
  /** Convenience "void" task result. */
  @JsonTypeName("void")
  @TaskResultTypeId("void")
  record EmptyTaskResult() implements TaskResult {}
}
