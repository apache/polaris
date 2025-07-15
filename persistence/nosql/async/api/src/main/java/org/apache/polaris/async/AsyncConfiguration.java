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
package org.apache.polaris.async;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.polaris.immutables.PolarisImmutable;

/** Advanced configuration options to tune async activities. */
@PolarisImmutable
@ConfigMapping(prefix = "polaris.async")
@JsonSerialize(as = ImmutableAsyncConfiguration.class)
@JsonDeserialize(as = ImmutableAsyncConfiguration.class)
public interface AsyncConfiguration {

  String DEFAULT_THREAD_KEEP_ALIVE_STRING = "PT1S";
  Duration DEFAULT_THREAD_KEEP_ALIVE = Duration.parse(DEFAULT_THREAD_KEEP_ALIVE_STRING);

  String DEFAULT_MAX_THREADS_STRING = "256";
  int DEFAULT_MAX_THREADS = Integer.parseInt(DEFAULT_MAX_THREADS_STRING);

  /** Duration to keep idle threads alive. */
  @WithDefault(DEFAULT_THREAD_KEEP_ALIVE_STRING)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> threadKeepAlive();

  /** Maximum number of threads available for asynchronous execution. Default is 256. */
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalInt maxThreads();

  static ImmutableAsyncConfiguration.Builder builder() {
    return ImmutableAsyncConfiguration.builder();
  }
}
