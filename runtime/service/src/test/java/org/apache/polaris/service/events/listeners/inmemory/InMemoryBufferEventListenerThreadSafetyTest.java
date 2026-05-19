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

package org.apache.polaris.service.events.listeners.inmemory;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Regression coverage for the concurrent-{@code onNext} race on the per-realm {@link
 * io.smallrye.mutiny.operators.multi.processors.UnicastProcessor} held by {@link
 * InMemoryBufferEventListener#processors}.
 *
 * <p>The Reactive Streams specification (rule 1.3) requires {@code onNext()} to be called
 * sequentially, and Mutiny's {@code UnicastProcessor} relies on that contract; concurrent {@code
 * onNext()} invocations on the same processor can silently drop events. The {@link
 * InMemoryBufferEventListener#processEvent} method serializes those calls via {@code
 * synchronized(processor)}; this test exercises that path under deliberate concurrency and asserts
 * the expected number of events lands in the events table.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestProfile(InMemoryBufferEventListenerThreadSafetyTest.Profile.class)
class InMemoryBufferEventListenerThreadSafetyTest extends InMemoryBufferEventListenerTestBase {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      // Small buffer-size with a long buffer-time so the size-based flush path is exercised
      // repeatedly during the burst, maximizing the concurrent surface area on the processor.
      return ImmutableMap.<String, String>builder()
          .putAll(BASE_CONFIG)
          .put("polaris.event-listener.persistence-in-memory-buffer.buffer-time", "60s")
          .put("polaris.event-listener.persistence-in-memory-buffer.max-buffer-size", "10")
          .build();
    }
  }

  @Test
  void testProcessEventIsThreadSafe() throws InterruptedException {
    int threadCount = 10;
    int eventsPerThread = 100;
    int expected = threadCount * eventsPerThread;
    String realmId = "test1";

    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);

    for (int i = 0; i < threadCount; i++) {
      Thread t =
          new Thread(
              () -> {
                try {
                  // All threads block here so they all release simultaneously and produce
                  // the worst-case concurrent burst into processEvent for the same realm.
                  startLatch.await();
                  for (int j = 0; j < eventsPerThread; j++) {
                    producer.processEvent(realmId, event());
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                } finally {
                  doneLatch.countDown();
                }
              },
              "processEvent-thread-safety-" + i);
      t.start();
    }

    startLatch.countDown();
    assertThat(doneLatch.await(30, TimeUnit.SECONDS))
        .as("all producer threads should complete within 30s")
        .isTrue();

    // Without the synchronized guard on processor.onNext(), some events would be silently
    // dropped and the row count would fall below the expected value. With the guard, all
    // events serialize through onNext() and land in the events table after the size-based
    // flushes complete.
    assertRows(realmId, expected);
  }
}
