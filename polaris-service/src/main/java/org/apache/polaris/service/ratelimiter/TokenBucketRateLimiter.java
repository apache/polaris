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

/** Token bucket implementation of a Polaris RateLimiter. */
public class TokenBucketRateLimiter implements RateLimiter {
  private final double tokensPerNano;
  private final double maxTokens;
  private final Clock clock;

  private double tokens;
  private long lastAcquireNanos;

  public TokenBucketRateLimiter(double tokensPerSecond, double maxTokens, Clock clock) {
    this.tokensPerNano = tokensPerSecond / 1e9;
    this.maxTokens = maxTokens;
    this.clock = clock;

    tokens = maxTokens;
    lastAcquireNanos = clock.nanoTime();
  }

  @Override
  public synchronized boolean tryAcquire() {
    // Grant tokens for the time that has passed since our last tryAcquire()
    long t = clock.nanoTime();
    long nanosPassed = t - lastAcquireNanos;
    lastAcquireNanos = t;
    tokens = Math.min(maxTokens, tokens + (nanosPassed * tokensPerNano));

    // Take a token if they have one available
    if (tokens >= 1) {
      tokens--;
      return true;
    }
    return false;
  }
}
