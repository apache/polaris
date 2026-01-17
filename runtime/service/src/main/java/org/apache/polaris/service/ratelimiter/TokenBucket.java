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

import java.time.InstantSource;

/**
 * General-purpose Token bucket implementation. Acquires tokens at a fixed rate and has a maximum
 * amount of tokens. Each successful "tryAcquire" costs 1 token.
 */
public class TokenBucket {
  /** Conversion factor: 1 token = 1000 milli-tokens. */
  private static final long MILLI_TOKENS_PER_TOKEN = 1000L;

  private static final long MAX_TOKENS_LIMIT = Long.MAX_VALUE / MILLI_TOKENS_PER_TOKEN;

  private final long tokensPerSecond;
  private final long maxMilliTokens;
  private final InstantSource instantSource;

  private long milliTokens;
  private long lastUpdateMillis;

  public TokenBucket(long tokensPerSecond, long maxTokens, InstantSource instantSource) {
    if (maxTokens > MAX_TOKENS_LIMIT) {
      throw new IllegalArgumentException(
          "maxTokens must be <= " + MAX_TOKENS_LIMIT + " to avoid overflow");
    }
    this.tokensPerSecond = tokensPerSecond;
    this.maxMilliTokens = maxTokens * MILLI_TOKENS_PER_TOKEN;
    this.instantSource = instantSource;

    milliTokens = maxMilliTokens;
    lastUpdateMillis = instantSource.millis();
  }

  /**
   * Tries to acquire and spend 1 token. Doesn't block if a token isn't available.
   *
   * @return whether a token was successfully acquired and spent
   */
  public synchronized boolean tryAcquire() {
    long t = instantSource.millis();
    long millisPassed = Math.subtractExact(t, lastUpdateMillis);
    lastUpdateMillis = t;

    long grant = millisPassed * tokensPerSecond;
    if (tokensPerSecond != 0 && grant / tokensPerSecond != millisPassed) {
      // Overflow occurred. If this much time passed, the bucket would be full anyway.
      milliTokens = maxMilliTokens;
    } else {
      // refill tokens
      milliTokens = Math.min(maxMilliTokens, milliTokens + grant);
    }

    // Take a token if available (1 token = 1000 milli-tokens)
    if (milliTokens >= MILLI_TOKENS_PER_TOKEN) {
      milliTokens -= MILLI_TOKENS_PER_TOKEN;
      return true;
    }
    return false;
  }
}
