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

import com.google.common.util.concurrent.RateLimiter;

/**
 * General-purpose Token bucket implementation around Guava's {@link RateLimiter}. Acquires tokens
 * at a fixed rate and has a maximum amount of tokens. Each successful "tryAcquire" costs 1 token.
 */
@SuppressWarnings("UnstableApiUsage")
public class TokenBucket {
  private final RateLimiter rateLimiter;

  public TokenBucket(double permitsPerSecond) {
    this.rateLimiter = RateLimiter.create(permitsPerSecond);
  }

  /**
   * Tries to acquire and spend 1 token. Doesn't block if a token isn't available.
   *
   * @return whether a token was successfully acquired and spent
   */
  public synchronized boolean tryAcquire() {
    return rateLimiter.tryAcquire();
  }
}
