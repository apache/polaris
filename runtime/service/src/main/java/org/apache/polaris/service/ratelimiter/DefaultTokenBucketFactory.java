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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.core.context.RealmContext;

@ApplicationScoped
@Identifier("default")
public class DefaultTokenBucketFactory implements TokenBucketFactory {

  private final long requestsPerSecond;
  private final Duration window;
  private final Clock clock;
  private final Map<String, TokenBucket> perRealmBuckets = new ConcurrentHashMap<>();

  @Inject
  public DefaultTokenBucketFactory(TokenBucketConfiguration configuration, Clock clock) {
    this(configuration.requestsPerSecond(), configuration.window(), clock);
  }

  public DefaultTokenBucketFactory(long requestsPerSecond, Duration window, Clock clock) {
    this.requestsPerSecond = requestsPerSecond;
    this.window = window;
    this.clock = clock;
  }

  @Override
  public TokenBucket getOrCreateTokenBucket(RealmContext realmContext) {
    String realmId = realmContext.getRealmIdentifier();
    return perRealmBuckets.computeIfAbsent(
        realmId,
        k ->
            new TokenBucket(
                requestsPerSecond,
                Math.multiplyExact(requestsPerSecond, window.toSeconds()),
                clock));
  }
}
