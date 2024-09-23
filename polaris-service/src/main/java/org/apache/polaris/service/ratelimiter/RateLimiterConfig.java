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
import org.apache.hadoop.util.Preconditions;

/** Configuration for the rate limiter */
public class RateLimiterConfig {
  private RateLimiterFactory rateLimiterFactory;

  /**
   * Rate limiters can be constructed asynchronously, so this config determines the construction
   * timeout before we default to a NoOpRateLimiter.
   *
   * <p>The default value is 2000ms.
   */
  private long constructionTimeoutMillis = 2000;

  /**
   * Since rate limiter construction is asynchronous and has a timeout, construction may fail. If
   * this option is enabled, the request will still be allowed when construction fails.
   *
   * <p>The default value is true which allows requests when rate limiter construction times out.
   */
  private boolean allowRequestOnConstructionTimeout = true;

  @JsonProperty("factory")
  public void setRateLimiterFactory(RateLimiterFactory rateLimiterFactory) {
    Preconditions.checkNotNull(
        rateLimiterFactory,
        "rateLimiterFactory must not be null. Set it using rateLimiter.factory.");
    this.rateLimiterFactory = rateLimiterFactory;
  }

  @JsonProperty("factory")
  public RateLimiterFactory getRateLimiterFactory() {
    return rateLimiterFactory;
  }

  @JsonProperty("constructionTimeoutMillis")
  public void setConstructionTimeoutMillis(long constructionTimeoutMillis) {
    Preconditions.checkArgument(
        constructionTimeoutMillis >= 0, "constructionTimeoutMillis must be non-negative");
    this.constructionTimeoutMillis = constructionTimeoutMillis;
  }

  public long getConstructionTimeoutMillis() {
    return constructionTimeoutMillis;
  }

  public boolean getAllowRequestOnConstructionTimeout() {
    return allowRequestOnConstructionTimeout;
  }
}
