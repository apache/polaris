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
package org.apache.polaris.service.ratelimiting;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.polaris.core.context.CallContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Request filter that returns a 429 Too Many Requests if the rate limiter says so */
@Priority(Priorities.AUTHORIZATION + 1)
public class RateLimitingFilter implements Filter {
  private static final Logger LOGGER = LoggerFactory.getLogger(RateLimitingFilter.class);
  private static final RateLimiter NO_OP_LIMITER = new NoOpRateLimiter();
  private static final int GET_RATE_LIMITER_TIMEOUT_SECONDS = 2;

  private final AsyncLoadingCache<String, RateLimiter> perRealmLimiters;

  public RateLimitingFilter(RateLimiterFactory rateLimiterFactory) {
    var clock = new ClockImpl();
    perRealmLimiters =
        Caffeine.newBuilder()
            .buildAsync((key, executor) -> rateLimiterFactory.createRateLimiter(key, clock));
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    String realm = CallContext.getCurrentContext().getRealmContext().getRealmIdentifier();
    RateLimiter rateLimiter = maybeBlockToGetRateLimiter(realm);
    if (!rateLimiter.trySpend(1)) {
      ((HttpServletResponse) response).setStatus(Response.Status.TOO_MANY_REQUESTS.getStatusCode());
      return;
    }
    chain.doFilter(request, response);
  }

  RateLimiter maybeBlockToGetRateLimiter(String realm) {
    try {
      return perRealmLimiters.get(realm).get(GET_RATE_LIMITER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOGGER.error("Failed to fetch rate limiter", e);
      return NO_OP_LIMITER;
    }
  }
}
