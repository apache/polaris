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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(PolarisEventListenersTest.PolarisEventListenersTestProfile.class)
public class PolarisEventListenersTest {
  static CountDownLatch latch = new CountDownLatch(4);

  @Singleton
  @Identifier("after-send-event-listener")
  public static class AfterSendEventListener implements PolarisEventListener {
    List<PolarisEvent> expectedEvents = new ArrayList<>();
    List<PolarisEvent> unexpectedEvents = new ArrayList<>();

    @Override
    public void onEvent(PolarisEvent event) {
      if (event.type() == PolarisEventType.AFTER_SEND_NOTIFICATION) {
        expectedEvents.add(event);
      } else {
        unexpectedEvents.add(event);
      }
      latch.countDown();
    }
  }

  @Singleton
  @Identifier("before-send-event-listener")
  public static class BeforeSendEventListener implements PolarisEventListener {
    List<PolarisEvent> expectedEvents = new ArrayList<>();
    List<PolarisEvent> unexpectedEvents = new ArrayList<>();

    @Override
    public void onEvent(PolarisEvent event) {
      if (event.type() == PolarisEventType.BEFORE_SEND_NOTIFICATION) {
        expectedEvents.add(event);
      } else {
        unexpectedEvents.add(event);
      }
      latch.countDown();
    }
  }

  @Singleton
  @Identifier("consume-all-listener")
  public static class ConsumeAllEventListener implements PolarisEventListener {
    List<PolarisEvent> consumedEvents = new ArrayList<>();

    @Override
    public void onEvent(PolarisEvent event) {
      consumedEvents.add(event);
      latch.countDown();
    }
  }

  public static class PolarisEventListenersTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put(
              "polaris.event-listener.types",
              "after-send-event-listener,before-send-event-listener,consume-all-listener")
          .put(
              "polaris.event-listener.after-send-event-listener.enabled-event-types",
              "AFTER_SEND_NOTIFICATION")
          .put(
              "polaris.event-listener.before-send-event-listener.enabled-event-types",
              "BEFORE_SEND_NOTIFICATION")
          .build();
    }
  }

  @Inject PolarisEventDispatcher eventDispatcher;

  @Inject
  @Identifier("after-send-event-listener")
  PolarisEventListener afterSendEventListener;

  @Inject
  @Identifier("before-send-event-listener")
  PolarisEventListener beforeSendEventListener;

  @Inject
  @Identifier("consume-all-listener")
  PolarisEventListener consumeAllEventListener;

  @Test
  public void testEventListenersGetNotified() throws InterruptedException {
    eventDispatcher.dispatch(new PolarisEvent(PolarisEventType.AFTER_SEND_NOTIFICATION, null));
    eventDispatcher.dispatch(new PolarisEvent(PolarisEventType.BEFORE_SEND_NOTIFICATION, null));
    var afterSendEventListener = (AfterSendEventListener) this.afterSendEventListener;
    var beforeSendEventListener = (BeforeSendEventListener) this.beforeSendEventListener;
    var consumeAllEventListener = (ConsumeAllEventListener) this.consumeAllEventListener;
    assertTrue(latch.await(5, TimeUnit.SECONDS));

    assertEquals(1, afterSendEventListener.expectedEvents.size());
    assertEquals(
        PolarisEventType.AFTER_SEND_NOTIFICATION,
        afterSendEventListener.expectedEvents.getFirst().type());
    assertEquals(1, beforeSendEventListener.expectedEvents.size());
    assertEquals(
        PolarisEventType.BEFORE_SEND_NOTIFICATION,
        beforeSendEventListener.expectedEvents.getFirst().type());
    assertEquals(2, consumeAllEventListener.consumedEvents.size());
    assertEquals(
        1,
        consumeAllEventListener.consumedEvents.stream()
            .filter(polarisEvent -> polarisEvent.type() == PolarisEventType.AFTER_SEND_NOTIFICATION)
            .count());
    assertEquals(
        1,
        consumeAllEventListener.consumedEvents.stream()
            .filter(
                polarisEvent -> polarisEvent.type() == PolarisEventType.BEFORE_SEND_NOTIFICATION)
            .count());
    assertEquals(0, afterSendEventListener.unexpectedEvents.size());
    assertEquals(0, beforeSendEventListener.unexpectedEvents.size());
  }
}
