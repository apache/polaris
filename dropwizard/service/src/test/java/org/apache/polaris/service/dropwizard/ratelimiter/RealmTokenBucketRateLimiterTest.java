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
package org.apache.polaris.service.dropwizard.ratelimiter;

import static org.apache.polaris.service.dropwizard.ratelimiter.MockTokenBucketFactory.CLOCK;

import java.time.Duration;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.service.ratelimiter.DefaultTokenBucketFactory;
import org.apache.polaris.service.ratelimiter.RealmTokenBucketRateLimiter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Main unit test class for TokenBucketRateLimiter */
public class RealmTokenBucketRateLimiterTest {
  @Test
  void testDifferentBucketsDontTouch() {
    RealmTokenBucketRateLimiter rateLimiter = new RealmTokenBucketRateLimiter();
    rateLimiter.setTokenBucketFactory(new DefaultTokenBucketFactory(10, 10, CLOCK));

    for (int i = 0; i < 202; i++) {
      String realm = (i % 2 == 0) ? "realm1" : "realm2";
      CallContext.setCurrentContext(CallContext.of(() -> realm, null));

      if (i < 200) {
        Assertions.assertTrue(rateLimiter.canProceed());
      } else {
        assertCannotProceed(rateLimiter);
      }
    }

    CLOCK.add(Duration.ofSeconds(1));
    for (int i = 0; i < 22; i++) {
      String realm = (i % 2 == 0) ? "realm1" : "realm2";
      CallContext.setCurrentContext(CallContext.of(() -> realm, null));

      if (i < 20) {
        Assertions.assertTrue(rateLimiter.canProceed());
      } else {
        assertCannotProceed(rateLimiter);
      }
    }
  }

  private void assertCannotProceed(RealmTokenBucketRateLimiter rateLimiter) {
    for (int i = 0; i < 5; i++) {
      Assertions.assertFalse(rateLimiter.canProceed());
    }
  }
}
