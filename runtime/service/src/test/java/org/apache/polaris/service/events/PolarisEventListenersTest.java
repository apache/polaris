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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(PolarisEventListenersTest.PolarisEventListenersTestProfile.class)
public class PolarisEventListenersTest {
  static final Set<PolarisEventType> CATALOG_EVENTS =
      Arrays.stream(PolarisEventType.values())
          .filter(e -> e.category() == PolarisEventType.Category.CATALOG)
          .collect(Collectors.toSet());

  private abstract static class FilteringEventListener implements PolarisEventListener {
    private final Predicate<PolarisEvent> predicate;

    List<PolarisEvent> expectedEvents = new CopyOnWriteArrayList<>();
    List<PolarisEvent> unexpectedEvents = new CopyOnWriteArrayList<>();
    AtomicInteger receivedEventCount = new AtomicInteger();

    FilteringEventListener(Predicate<PolarisEvent> predicate) {
      this.predicate = predicate;
    }

    @Override
    public void onEvent(PolarisEvent event) {
      if (predicate.test(event)) {
        expectedEvents.add(event);
      } else {
        unexpectedEvents.add(event);
      }
      receivedEventCount.incrementAndGet();
    }
  }

  private abstract static class BaseConsumeAllEventListener implements PolarisEventListener {
    List<PolarisEvent> consumedEvents = new CopyOnWriteArrayList<>();
    AtomicInteger receivedEventCount = new AtomicInteger();

    @Override
    public void onEvent(PolarisEvent event) {
      consumedEvents.add(event);
      receivedEventCount.incrementAndGet();
    }
  }

  @Singleton
  @Identifier("after-send-listener")
  public static class AfterSendEventListener extends FilteringEventListener {
    AfterSendEventListener() {
      super(event -> event.type() == PolarisEventType.AFTER_SEND_NOTIFICATION);
    }
  }

  @Singleton
  @Identifier("before-send-listener")
  public static class BeforeSendEventListener extends FilteringEventListener {
    BeforeSendEventListener() {
      super(event -> event.type() == PolarisEventType.BEFORE_SEND_NOTIFICATION);
    }
  }

  @Singleton
  @Identifier("consume-all-listener")
  public static class ConsumeAllEventListener extends BaseConsumeAllEventListener {}

  @Singleton
  @Identifier("consume-all-listener-2")
  public static class ConsumeAllEventListener2 extends BaseConsumeAllEventListener {}

  @Singleton
  @Identifier("consume-only-catalog-listener")
  public static class ConsumeOnlyCatalogEventsListener extends FilteringEventListener {
    ConsumeOnlyCatalogEventsListener() {
      super(event -> event.type().category() == PolarisEventType.Category.CATALOG);
    }
  }

  @Singleton
  @Identifier("consume-catalog-and-after-notification-listener")
  public static class ConsumeCatalogAndNotificationEventsListener extends FilteringEventListener {
    ConsumeCatalogAndNotificationEventsListener() {
      super(
          event ->
              event.type().category() == PolarisEventType.Category.CATALOG
                  || event.type() == PolarisEventType.AFTER_SEND_NOTIFICATION);
    }
  }

  public static class PolarisEventListenersTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put(
              "polaris.event-listener.types",
              "after-send-listener,before-send-listener,consume-all-listener,consume-all-listener-2,consume-only-catalog-listener,consume-catalog-and-after-notification-listener")
          .put(
              "polaris.event-listener.after-send-listener.enabled-event-types",
              "AFTER_SEND_NOTIFICATION")
          .put(
              "polaris.event-listener.before-send-listener.enabled-event-types",
              "BEFORE_SEND_NOTIFICATION")
          .put(
              "polaris.event-listener.consume-only-catalog-listener.enabled-event-categories",
              "CATALOG")
          .put(
              "polaris.event-listener.consume-catalog-and-after-notification-listener.enabled-event-categories",
              "CATALOG")
          .put(
              "polaris.event-listener.consume-catalog-and-after-notification-listener.enabled-event-types",
              "AFTER_SEND_NOTIFICATION")
          .build();
    }
  }

  @Inject PolarisEventDispatcher eventDispatcher;

  @Inject
  @Identifier("after-send-listener")
  PolarisEventListener afterSendEventListener;

  @Inject
  @Identifier("before-send-listener")
  PolarisEventListener beforeSendEventListener;

  @Inject
  @Identifier("consume-all-listener")
  PolarisEventListener consumeAllEventListener;

  @Inject
  @Identifier("consume-all-listener-2")
  PolarisEventListener consumeAllEventListener2;

  @Inject
  @Identifier("consume-only-catalog-listener")
  PolarisEventListener consumeOnlyCatalogEventListener;

  @Inject
  @Identifier("consume-catalog-and-after-notification-listener")
  PolarisEventListener consumeCatalogAndNotificationEventListener;

  @Test
  public void testEventListenersGetNotified() {
    var afterSendEventListener = (AfterSendEventListener) this.afterSendEventListener;
    var beforeSendEventListener = (BeforeSendEventListener) this.beforeSendEventListener;
    var consumeAllEventListener = (ConsumeAllEventListener) this.consumeAllEventListener;
    var consumeAllEventListener2 = (ConsumeAllEventListener2) this.consumeAllEventListener2;
    var consumeOnlyCatalogEventListener =
        (ConsumeOnlyCatalogEventsListener) this.consumeOnlyCatalogEventListener;
    var consumeCatalogAndNotificationEventListener =
        (ConsumeCatalogAndNotificationEventsListener)
            this.consumeCatalogAndNotificationEventListener;

    eventDispatcher.dispatch(new PolarisEvent(PolarisEventType.AFTER_SEND_NOTIFICATION, null));
    eventDispatcher.dispatch(new PolarisEvent(PolarisEventType.BEFORE_SEND_NOTIFICATION, null));
    for (var eventType : CATALOG_EVENTS) {
      eventDispatcher.dispatch(new PolarisEvent(eventType, null));
    }

    // Only after send notification event
    await().until(() -> afterSendEventListener.receivedEventCount.get() == 1);
    assertEquals(1, afterSendEventListener.expectedEvents.size());
    assertEquals(
        PolarisEventType.AFTER_SEND_NOTIFICATION,
        afterSendEventListener.expectedEvents.getFirst().type());

    // Only before send notification event
    await().until(() -> beforeSendEventListener.receivedEventCount.get() == 1);
    assertEquals(1, beforeSendEventListener.expectedEvents.size());
    assertEquals(
        PolarisEventType.BEFORE_SEND_NOTIFICATION,
        beforeSendEventListener.expectedEvents.getFirst().type());

    // All events received by both consumeAllEventListener and consumeAllEventListener2
    for (var listener :
        new BaseConsumeAllEventListener[] {consumeAllEventListener, consumeAllEventListener2}) {
      await().until(() -> listener.receivedEventCount.get() == 2 + CATALOG_EVENTS.size());
      assertEquals(2 + CATALOG_EVENTS.size(), listener.consumedEvents.size());
      expectEvents(
          listener.consumedEvents,
          Stream.concat(
                  Stream.of(
                      PolarisEventType.AFTER_SEND_NOTIFICATION,
                      PolarisEventType.BEFORE_SEND_NOTIFICATION),
                  CATALOG_EVENTS.stream())
              .collect(Collectors.toSet()));
    }

    // Only catalog events
    await()
        .until(
            () ->
                consumeOnlyCatalogEventListener.receivedEventCount.get() == CATALOG_EVENTS.size());
    assertEquals(CATALOG_EVENTS.size(), consumeOnlyCatalogEventListener.expectedEvents.size());
    expectEvents(consumeOnlyCatalogEventListener.expectedEvents, CATALOG_EVENTS);

    // Catalog and after send notification events
    await()
        .until(
            () ->
                consumeCatalogAndNotificationEventListener.receivedEventCount.get()
                    == CATALOG_EVENTS.size() + 1);
    assertEquals(
        CATALOG_EVENTS.size() + 1,
        consumeCatalogAndNotificationEventListener.expectedEvents.size());
    expectEvents(
        consumeCatalogAndNotificationEventListener.expectedEvents,
        Stream.concat(Stream.of(PolarisEventType.AFTER_SEND_NOTIFICATION), CATALOG_EVENTS.stream())
            .collect(Collectors.toSet()));

    // There are no unexpected events
    assertTrue(afterSendEventListener.unexpectedEvents.isEmpty());
    assertTrue(beforeSendEventListener.unexpectedEvents.isEmpty());
    assertTrue(consumeOnlyCatalogEventListener.unexpectedEvents.isEmpty());
    assertTrue(consumeCatalogAndNotificationEventListener.unexpectedEvents.isEmpty());
  }

  private void expectEvents(List<PolarisEvent> events, Set<PolarisEventType> expectedTypes) {
    for (var event : expectedTypes) {
      assertEquals(1, events.stream().filter(polarisEvent -> polarisEvent.type() == event).count());
    }
  }
}
