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

import io.quarkiverse.bucket4j.runtime.RateLimitException;
import io.quarkiverse.bucket4j.runtime.RateLimited;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Default rate limiter implementation using Bucket4J.
 *
 * <p>This service uses the "polaris" bucket with per-realm segmentation via {@link
 * RealmIdentityResolver}.
 */
@ApplicationScoped
@Identifier("default")
public class RealmTokenBucketRateLimiter implements RateLimiter {

  @Inject
  @Identifier("default")
  RealmTokenBucketRateLimiter self;

  @Override
  public boolean canProceed() {
    try {
      // call the method through the proxy
      self.tryAcquire();
      return true;
    } catch (RateLimitException e) {
      return false;
    }
  }

  @RateLimited(bucket = "polaris", identityResolver = RealmIdentityResolver.class)
  public void tryAcquire() {
    // This method must be public and is intentionally empty.
    // The rate limiting is handled by the Bucket4J interceptor.
    // If the rate limit is exceeded, a RateLimitException is thrown.
  }
}
