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

<<<<<<<< HEAD:dropwizard/service/src/test/java/org/apache/polaris/service/dropwizard/ratelimiter/RateLimitResultAsserter.java
import org.apache.polaris.service.ratelimiter.RateLimiter;
========
import java.util.function.BooleanSupplier;
>>>>>>>> 0727ce8e (Refactor RateLimiter):dropwizard/service/src/test/java/org/apache/polaris/service/dropwizard/ratelimiter/TokenBucketResultAsserter.java
import org.junit.jupiter.api.Assertions;

/** Utility class for testing rate limiters. Lets you easily assert the result of tryAcquire(). */
public class TokenBucketResultAsserter {
  private final BooleanSupplier acquire;

  public TokenBucketResultAsserter(BooleanSupplier acquire) {
    this.acquire = acquire;
  }

  public void canAcquire(int times) {
    for (int i = 0; i < times; i++) {
      Assertions.assertTrue(acquire.getAsBoolean());
    }
  }

  public void cantAcquire() {
    for (int i = 0; i < 5; i++) {
      Assertions.assertFalse(acquire.getAsBoolean());
    }
  }
}
