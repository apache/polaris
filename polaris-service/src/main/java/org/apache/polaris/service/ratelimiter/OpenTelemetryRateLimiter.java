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

/**
 * Wrapper around the opentelemetry RateLimiter that implements the Polaris RateLimiter interface
 * The opentelemetry limiter uses a credits/balance system. We treat 1 request as 1 credit.
 */
public class OpenTelemetryRateLimiter implements RateLimiter {
  private final io.opentelemetry.sdk.internal.RateLimiter rateLimiter;

  public OpenTelemetryRateLimiter(double creditsPerSecond, double maxBalance, Clock clock) {
    rateLimiter =
        new io.opentelemetry.sdk.internal.RateLimiter(
            creditsPerSecond, maxBalance, new OpenTelemetryClock(clock));
  }

  @Override
  public boolean tryAcquire() {
    return rateLimiter.trySpend(1);
  }
}
