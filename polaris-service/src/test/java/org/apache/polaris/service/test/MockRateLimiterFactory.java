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
package org.apache.polaris.service.test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.polaris.service.ratelimiting.Clock;
import org.apache.polaris.service.ratelimiting.OpenTelemetryRateLimiter;
import org.apache.polaris.service.ratelimiting.RateLimiter;
import org.apache.polaris.service.ratelimiting.RateLimiterFactory;

/**
 * Factory that constructs a standard OpenTelemetryRateLimiter but with a mock Clock and an optional
 * construction delay
 */
@JsonTypeName("mock")
public class MockRateLimiterFactory implements RateLimiterFactory {
  public static MockClock clock = new MockClock();

  @JsonProperty("requestsPerSecond")
  public double requestsPerSecond;

  @JsonProperty("windowSeconds")
  public double windowSeconds;

  @JsonProperty("delaySeconds")
  public long delaySeconds;

  @Override
  public CompletableFuture<RateLimiter> createRateLimiter(String key, Clock clock) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            Thread.sleep(Duration.ofSeconds(delaySeconds));
          } catch (InterruptedException e) {
          }

          return new OpenTelemetryRateLimiter(
              requestsPerSecond, requestsPerSecond * windowSeconds, MockRateLimiterFactory.clock);
        });
  }
}
