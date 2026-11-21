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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

@PolarisImmutable
@JsonSerialize(as = ImmutableTaskExecutionError.class)
@JsonDeserialize(as = ImmutableTaskExecutionError.class)
public interface TaskExecutionError {
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  @Value.Default
  default boolean retryable() {
    return false;
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> technicalErrorCode();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> technicalErrorMessage();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> properties();

  static ImmutableTaskExecutionError.Builder builder() {
    return ImmutableTaskExecutionError.builder();
  }
}
