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
package org.apache.polaris.persistence.nosql.impl.commits.retry;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.ids.impl.MonotonicClockImpl;
import org.apache.polaris.persistence.nosql.api.commit.RetryConfig;
import org.apache.polaris.persistence.nosql.api.commit.RetryTimeoutException;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
@Disabled("Long running test disabled")
public class TestRetryLoopConcurrency {
  @InjectSoftAssertions SoftAssertions soft;

  MonotonicClock clock;
  RetryConfig retryConfig;

  @BeforeEach
  void setUp() {
    clock = MonotonicClockImpl.newDefaultInstance();
    retryConfig = RetryConfig.BuildableRetryConfig.builder().build();
  }

  @AfterEach
  void tearDown() {
    clock.close();
  }

  @Test
  public void retryLoopConcurrencyRetryNoTimeout() throws Exception {
    var value = new AtomicInteger();
    var threads = 8;
    var stop = new AtomicBoolean();
    var timeouts = new AtomicInteger();
    var successes = new AtomicInteger();

    var startLatch = new CountDownLatch(threads);
    var runLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(threads);

    var totalRetries = new AtomicInteger();
    var totalSleepTime = new AtomicLong();

    var futures = new ArrayList<Future<?>>();

    try (var executor = Executors.newFixedThreadPool(threads)) {
      for (int i = 0; i < threads; i++) {
        futures.add(
            executor.submit(
                () -> {
                  try {
                    startLatch.countDown();
                    try {
                      runLatch.await();
                    } catch (InterruptedException e) {
                      throw new RuntimeException(e);
                    }
                    while (!stop.get()) {
                      try {
                        RetryLoop.newRetryLoop(retryConfig, clock)
                            .setRetryStatsConsumer(
                                ((result, retries, sleepTimeMillis, totalDurationNanos) -> {
                                  totalRetries.addAndGet(retries);
                                  totalSleepTime.addAndGet(sleepTimeMillis);
                                }))
                            .retryLoop(
                                (long nanosRemaining) -> {
                                  int v = value.get();
                                  // Let other thread(s) continue to cause CAS failures.
                                  Thread.yield();
                                  return value.compareAndSet(v, v + 1)
                                      ? Optional.of(v)
                                      : Optional.empty();
                                });
                        successes.incrementAndGet();
                      } catch (RetryTimeoutException timeoutException) {
                        timeouts.incrementAndGet();
                      } catch (Exception e) {
                        throw new RuntimeException(e);
                      }
                    }
                  } finally {
                    doneLatch.countDown();
                  }
                }));
      }

      startLatch.await();
      runLatch.countDown();

      Thread.sleep(60_000);
      stop.set(true);

      doneLatch.await();

      System.err.printf(
          """
            Successes: %d
            Timeouts: %d
            Retries: %d
            SleepTime: %d

            """,
          successes.get(), timeouts.get(), totalRetries.get(), totalSleepTime.get());

      soft.assertThat(timeouts).hasValue(0);
      soft.assertThat(successes).hasValueGreaterThan(0);

      for (Future<?> f : futures) {
        soft.assertThatCode(f::get).doesNotThrowAnyException();
      }
    }
  }
}
