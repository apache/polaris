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

package org.apache.polaris.service.events.listeners;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.events.EventListenerConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.threeten.extra.MutableClock;

public class InMemoryBufferPolarisPersistenceEventListenerTest {
  private InMemoryBufferPolarisPersistenceEventListener eventListener;
  private PolarisMetaStoreManager polarisMetaStoreManager;
  private MutableClock clock;
  private CallContext callContext;

  private static final int CONFIG_MAX_BUFFER_SIZE = 5;
  private static final Duration CONFIG_TIME_TO_FLUSH_IN_MS = Duration.ofMillis(500);

  @BeforeEach
  public void setUp() {
    callContext = Mockito.mock(CallContext.class);
    PolarisCallContext polarisCallContext = Mockito.mock(PolarisCallContext.class);
    when(callContext.getPolarisCallContext()).thenReturn(polarisCallContext);
    when(polarisCallContext.copy()).thenReturn(polarisCallContext);

    MetaStoreManagerFactory metaStoreManagerFactory = Mockito.mock(MetaStoreManagerFactory.class);
    polarisMetaStoreManager = Mockito.mock(PolarisMetaStoreManager.class);
    when(metaStoreManagerFactory.getOrCreateMetaStoreManager(Mockito.any()))
        .thenReturn(polarisMetaStoreManager);

    EventListenerConfiguration eventListenerConfiguration =
        Mockito.mock(EventListenerConfiguration.class);
    when(eventListenerConfiguration.maxBufferSize())
        .thenReturn(Optional.of(CONFIG_MAX_BUFFER_SIZE));
    when(eventListenerConfiguration.bufferTime())
        .thenReturn(Optional.of(CONFIG_TIME_TO_FLUSH_IN_MS));

    clock =
        MutableClock.of(
            Instant.ofEpochSecond(0), ZoneOffset.UTC); // Use 0 Epoch Time to make it easier to test

    eventListener =
        new InMemoryBufferPolarisPersistenceEventListener(
            metaStoreManagerFactory, clock, eventListenerConfiguration);
  }

  @Test
  public void testAddToBufferFlushesAfterConfiguredTime() {
    String realmId = "realm1";
    List<PolarisEvent> eventsAddedToBuffer = addEventsWithoutTriggeringFlush(realmId);

    // Push clock forwards to flush the buffer
    clock.add(CONFIG_TIME_TO_FLUSH_IN_MS.multipliedBy(2));
    eventListener.checkAndFlushBufferIfNecessary(realmId);
    verify(polarisMetaStoreManager, times(1))
        .writeEvents(eq(callContext.getPolarisCallContext()), eq(eventsAddedToBuffer));
  }

  @Test
  public void testAddToBufferFlushesAfterMaxEvents() {
    String realm1 = "realm1";
    List<PolarisEvent> eventsAddedToBuffer = addEventsWithoutTriggeringFlush(realm1);
    addEventsWithoutTriggeringFlush("realm2");

    // Add the last event for realm1 and verify that it did trigger the flush
    PolarisEvent triggeringEvent = createSampleEvent();
    RealmContext realmContext = () -> realm1;
    when(callContext.getRealmContext()).thenReturn(realmContext);
    eventListener.addToBuffer(triggeringEvent, callContext);
    eventsAddedToBuffer.add(triggeringEvent);

    // Given the call to checkAndFlushBufferIfNecessary is async, the calling thread should not have
    // blocked and nothing would've been done immediately
    verify(polarisMetaStoreManager, times(0))
        .writeEvents(eq(callContext.getPolarisCallContext()), eq(eventsAddedToBuffer));

    // Calling checkAndFlushBufferIfNecessary manually to replicate the behavior of the executor
    // service
    eventListener.checkAndFlushBufferIfNecessary(realm1);
    verify(polarisMetaStoreManager, times(0))
        .writeEvents(eq(callContext.getPolarisCallContext()), eq(eventsAddedToBuffer));
  }

  @Test
  public void testCheckAndFlushBufferIfNecessaryIsThreadSafe() throws Exception {
    String realmId = "realm1";
    int threadCount = 10;
    List<Thread> threads = new ArrayList<>();
    ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();

    // Pre-populate the buffer with events
    List<PolarisEvent> events = addEventsWithoutTriggeringFlush(realmId);

    // Push clock forwards to flush the buffer
    clock.add(CONFIG_TIME_TO_FLUSH_IN_MS.multipliedBy(2));

    // Each thread will call checkAndFlushBufferIfNecessary concurrently
    for (int i = 0; i < threadCount; i++) {
      Thread t =
          new Thread(
              () -> {
                try {
                  eventListener.checkAndFlushBufferIfNecessary(realmId);
                } catch (Exception e) {
                  exceptions.add(e);
                }
              });
      threads.add(t);
    }
    // Start all threads
    threads.forEach(Thread::start);
    // Wait for all threads to finish
    for (Thread t : threads) {
      t.join();
    }
    // There should be no exceptions
    if (!exceptions.isEmpty()) {
      throw new AssertionError(
          "Exceptions occurred in concurrent checkAndFlushBufferIfNecessary: ", exceptions.peek());
    }
    // Only one flush should occur
    verify(polarisMetaStoreManager, times(1))
        .writeEvents(eq(callContext.getPolarisCallContext()), eq(events));
  }

  private List<PolarisEvent> addEventsWithoutTriggeringFlush(String realmId) {
    List<PolarisEvent> realmEvents = new ArrayList<>();
    for (int i = 0; i < CONFIG_MAX_BUFFER_SIZE - 1; i++) {
      realmEvents.add(createSampleEvent());
    }
    RealmContext realmContext = () -> realmId;
    when(callContext.getRealmContext()).thenReturn(realmContext);
    for (PolarisEvent realmEvent : realmEvents) {
      eventListener.addToBuffer(realmEvent, callContext);
    }
    verify(polarisMetaStoreManager, times(0)).writeEvents(Mockito.any(), Mockito.any());
    return realmEvents;
  }

  private PolarisEvent createSampleEvent() {
    String catalogId = "test-catalog";
    String id = UUID.randomUUID().toString();
    String requestId = "test-request-id";
    String eventType = "TEST_EVENT";
    long timestampMs = 0;
    String principalName = "test-user";
    PolarisEvent.ResourceType resourceType = PolarisEvent.ResourceType.TABLE;
    String resourceIdentifier = "test-table";

    PolarisEvent event =
        new PolarisEvent(
            catalogId,
            id,
            requestId,
            eventType,
            timestampMs,
            principalName,
            resourceType,
            resourceIdentifier);

    Map<String, String> additionalParams = new HashMap<>();
    additionalParams.put("testKey", "testValue");
    event.setAdditionalParameters(additionalParams);

    return event;
  }
}
