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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;

/**
 * Rate limiter that maps the request's realm identifier to its own TokenBucketRateLimiter, with its
 * own capacity.
 */
@JsonTypeName("realm-token-bucket")
public class RealmTokenBucketRateLimiter implements RateLimiter {
  private final long requestsPerSecond;
  private final long windowSeconds;
  private final Map<String, RateLimiter> perRealmLimiters;

  @VisibleForTesting
  @JsonCreator
  public RealmTokenBucketRateLimiter(
      @JsonProperty("requestsPerSecond") final long requestsPerSecond,
      @JsonProperty("windowSeconds") final long windowSeconds) {
    this.requestsPerSecond = requestsPerSecond;
    this.windowSeconds = windowSeconds;
    this.perRealmLimiters = new ConcurrentHashMap<>();
  }

  /**
   * This signifies that a request is being made. That is, the rate limiter should count the request
   * at this point.
   *
   * @return Whether the request is allowed to proceed by the rate limiter
   */
  @Override
  public boolean tryAcquire() {
    String key =
        Optional.ofNullable(CallContext.getCurrentContext())
            .map(CallContext::getRealmContext)
            .map(RealmContext::getRealmIdentifier)
            .orElse("");

    return perRealmLimiters
        .computeIfAbsent(
            key,
            (k) ->
                new TokenBucketRateLimiter(
                    requestsPerSecond,
                    Math.multiplyExact(requestsPerSecond, windowSeconds),
                    getClock()))
        .tryAcquire();
  }

  @VisibleForTesting
  protected Clock getClock() {
    return Clock.systemUTC();
  }
}
