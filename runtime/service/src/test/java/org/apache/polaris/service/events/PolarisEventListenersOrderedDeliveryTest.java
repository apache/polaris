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

package org.apache.polaris.service.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableMap;
import io.quarkus.runtime.BlockingOperationControl;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(
    PolarisEventListenersOrderedDeliveryTest.PolarisEventListenersOrderedDeliveryTestProfile.class)
public class PolarisEventListenersOrderedDeliveryTest {

  static final int EVENTS_COUNT = 500;

  public static class PolarisEventListenersOrderedDeliveryTestProfile
      implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.event-listener.executor.queue-size", String.valueOf(EVENTS_COUNT))
          .put(
              "polaris.event-listener.types",
              "ordered-listener1,ordered-listener2,ordered-listener3")
          .put(
              "polaris.event-listener.ordered-listener1.enabled-event-types",
              "AFTER_SEND_NOTIFICATION")
          .put(
              "polaris.event-listener.ordered-listener2.enabled-event-types",
              "AFTER_SEND_NOTIFICATION")
          .put(
              "polaris.event-listener.ordered-listener3.enabled-event-types",
              "AFTER_SEND_NOTIFICATION")
          .build();
    }

    @Singleton
    @Identifier("ordered-listener1")
    OrderedEventListener orderedEventListener1() {
      return new OrderedEventListener();
    }

    @Singleton
    @Identifier("ordered-listener2")
    OrderedEventListener orderedEventListener2() {
      return new OrderedEventListener();
    }

    @Singleton
    @Identifier("ordered-listener3")
    OrderedEventListener orderedEventListener3() {
      return new OrderedEventListener();
    }
  }

  static class OrderedEventListener implements PolarisEventListener {

    static final AttributeKey<Integer> SEQ = new AttributeKey<>("seq", Integer.class);

    final List<Integer> receivedSequences = new CopyOnWriteArrayList<>();
    final AtomicInteger receivedEventCount = new AtomicInteger();

    final CountDownLatch pauseLatch = new CountDownLatch(1);

    @Override
    public void onEvent(PolarisEvent event) {
      assertThat(BlockingOperationControl.isBlockingAllowed()).isTrue();
      if (!event.attributes().contains(SEQ)) {
        return;
      }
      try {
        assertThat(pauseLatch.await(30, TimeUnit.SECONDS)).isTrue();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      receivedSequences.add(event.attributes().getRequired(SEQ));
      receivedEventCount.incrementAndGet();
    }
  }

  @Inject PolarisEventDispatcher eventDispatcher;

  @Inject
  @Identifier("ordered-listener1")
  OrderedEventListener orderedEventListener1;

  @Inject
  @Identifier("ordered-listener2")
  OrderedEventListener orderedEventListener2;

  @Inject
  @Identifier("ordered-listener3")
  OrderedEventListener orderedEventListener3;

  @Test
  public void testOrderedEventDelivery() {

    // listener1 is unblocked before event submission begins
    orderedEventListener1.pauseLatch.countDown();

    for (int i = 0; i < EVENTS_COUNT; i++) {
      var attrs = new EventAttributeMap().put(OrderedEventListener.SEQ, i);
      eventDispatcher.dispatch(
          new PolarisEvent(PolarisEventType.AFTER_SEND_NOTIFICATION, null, attrs));
      // listener2 is unblocked midway through event submission
      if (i == EVENTS_COUNT / 2) {
        orderedEventListener2.pauseLatch.countDown();
      }
    }

    // listener3 is unblocked after all events have been submitted
    orderedEventListener3.pauseLatch.countDown();

    await().until(() -> orderedEventListener1.receivedEventCount.get() == EVENTS_COUNT);
    await().until(() -> orderedEventListener2.receivedEventCount.get() == EVENTS_COUNT);
    await().until(() -> orderedEventListener3.receivedEventCount.get() == EVENTS_COUNT);

    assertThat(orderedEventListener1.receivedSequences)
        .containsExactlyElementsOf(IntStream.range(0, EVENTS_COUNT).boxed().toList());
    assertThat(orderedEventListener2.receivedSequences)
        .containsExactlyElementsOf(IntStream.range(0, EVENTS_COUNT).boxed().toList());
    assertThat(orderedEventListener3.receivedSequences)
        .containsExactlyElementsOf(IntStream.range(0, EVENTS_COUNT).boxed().toList());
  }
}
