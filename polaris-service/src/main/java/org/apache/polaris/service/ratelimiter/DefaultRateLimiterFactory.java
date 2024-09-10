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
package org.apache.polaris.service.ratelimiter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.concurrent.CompletableFuture;

/**
 * Simple rate limiter factory that just constructs an OpenTelemetryRateLimiter and is
 * indiscriminate of the key
 */
@JsonTypeName("default")
public class DefaultRateLimiterFactory implements RateLimiterFactory {
  @JsonProperty("requestsPerSecond")
  private double requestsPerSecond;

  @JsonProperty("windowSeconds")
  private double windowSeconds;

  @Override
  public CompletableFuture<RateLimiter> createRateLimiter(String key, Clock clock) {
    return CompletableFuture.supplyAsync(
        () ->
            new OpenTelemetryRateLimiter(
                requestsPerSecond, requestsPerSecond * windowSeconds, clock));
  }
}
