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

import jakarta.annotation.Priority;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Request filter that returns a 429 Too Many Requests if the rate limiter says so */
@Priority(Priorities.AUTHORIZATION + 1)
public class RateLimiterFilter implements Filter {
  private static final Logger LOGGER = LoggerFactory.getLogger(RateLimiterFilter.class);
  private static final RateLimiter NO_OP_LIMITER = new NoOpRateLimiter();
  private static final RateLimiter ALWAYS_REJECT_LIMITER =
      new TokenBucketRateLimiter(0, 0, Clock.systemUTC());

  private final RateLimiterConfig config;
  private final Map<RateLimiterKey, Future<RateLimiter>> perRealmLimiters =
      new ConcurrentHashMap<>();

  public RateLimiterFilter(RateLimiterConfig config) {
    this.config = config;
  }

  /** Returns a 429 if the rate limiter says so. Otherwise, forwards the request along. */
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    // Build the rate limiting key
    String realmIdentifier =
        Optional.ofNullable(CallContext.getCurrentContext())
            .map(CallContext::getRealmContext)
            .map(RealmContext::getRealmIdentifier)
            .orElse("");
    RateLimiterKey key = new RateLimiterKey(realmIdentifier);

    // Maybe enforce the rate limit
    RateLimiter rateLimiter = maybeBlockToGetRateLimiter(key);
    if (response instanceof HttpServletResponse servletResponse && !rateLimiter.tryAcquire()) {
      servletResponse.setStatus(Response.Status.TOO_MANY_REQUESTS.getStatusCode());
      return;
    }
    chain.doFilter(request, response);
  }

  private RateLimiter maybeBlockToGetRateLimiter(RateLimiterKey key) {
    try {
      System.out.println("ANDREW keys = " + perRealmLimiters.keySet());
      return perRealmLimiters
          .computeIfAbsent(key, (k) -> config.getRateLimiterFactory().createRateLimiter(k))
          .get(config.getConstructionTimeoutMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      return getDefaultRateLimiterOnConstructionFailed(e);
    }
  }

  private RateLimiter getDefaultRateLimiterOnConstructionFailed(Exception e) {
    if (config.getAllowRequestOnConstructionTimeout()) {
      LOGGER.error("Failed to fetch rate limiter, allowing the request", e);
      return NO_OP_LIMITER;
    } else {
      LOGGER.error("Failed to fetch rate limiter, rejecting the request", e);
      return ALWAYS_REJECT_LIMITER;
    }
  }
}
