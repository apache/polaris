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
  private final double tokensPerMilli;
  private final long maxTokens;
  private final InstantSource instantSource;

  private long tokens;
  private long lastTokenGenerationMillis;

  public TokenBucket(long tokensPerSecond, long maxTokens, InstantSource instantSource) {
    this.tokensPerMilli = tokensPerSecond / 1000D;
    this.maxTokens = maxTokens;
    this.instantSource = instantSource;

    tokens = maxTokens;
    lastTokenGenerationMillis = instantSource.millis();
  }

  /**
   * Tries to acquire and spend 1 token. Doesn't block if a token isn't available.
   *
   * @return whether a token was successfully acquired and spent
   */
  public synchronized boolean tryAcquire() {
    // Grant tokens for the time that has passed since our last tryAcquire()
    long t = instantSource.millis();
    long millisPassed = Math.subtractExact(t, lastTokenGenerationMillis);
    lastTokenGenerationMillis = t;
    tokens = Math.min(maxTokens, tokens + ((long) (millisPassed * tokensPerMilli)));

    // Take a token if they have one available
    if (tokens >= 1) {
      tokens--;
      return true;
    }
    return false;
  }
}
