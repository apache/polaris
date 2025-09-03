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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.container.ContainerRequestContext;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.ArgumentCaptor;
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

    MetaStoreManagerFactory metaStoreManagerFactory = Mockito.mock(MetaStoreManagerFactory.class);
    polarisMetaStoreManager = Mockito.mock(PolarisMetaStoreManager.class);
    when(metaStoreManagerFactory.getOrCreateMetaStoreManager(any()))
        .thenReturn(polarisMetaStoreManager);

    InMemoryBufferEventListenerConfiguration eventListenerConfiguration =
        Mockito.mock(InMemoryBufferEventListenerConfiguration.class);
    when(eventListenerConfiguration.maxBufferSize()).thenReturn(CONFIG_MAX_BUFFER_SIZE);
    when(eventListenerConfiguration.bufferTime()).thenReturn(CONFIG_TIME_TO_FLUSH_IN_MS);

    clock =
        MutableClock.of(
            Instant.ofEpochSecond(0), ZoneOffset.UTC); // Use 0 Epoch Time to make it easier to test

    eventListener =
        new InMemoryBufferPolarisPersistenceEventListener(
            metaStoreManagerFactory, clock, eventListenerConfiguration);

    eventListener.callContext = callContext;
  }

  @Test
  public void testProcessEventFlushesAfterConfiguredTime() {
    String realmId = "realm1";
    List<PolarisEvent> eventsAddedToBuffer = addEventsWithoutTriggeringFlush(realmId);

    // Push clock forwards to flush the buffer
    clock.add(CONFIG_TIME_TO_FLUSH_IN_MS.multipliedBy(2));
    eventListener.checkAndFlushBufferIfNecessary(realmId, false);
    verify(polarisMetaStoreManager, times(1)).writeEvents(any(), eq(eventsAddedToBuffer));
  }

  @Test
  public void testProcessEventFlushesAfterMaxEvents() {
    String realm1 = "realm1";
    List<PolarisEvent> eventsAddedToBuffer = addEventsWithoutTriggeringFlush(realm1);
    List<PolarisEvent> eventsAddedToBufferRealm2 = addEventsWithoutTriggeringFlush("realm2");

    // Add the last event for realm1 and verify that it did trigger the flush
    PolarisEvent triggeringEvent = createSampleEvent();
    RealmContext realmContext = () -> realm1;
    when(callContext.getRealmContext()).thenReturn(realmContext);
    eventListener.processEvent(triggeringEvent);
    eventsAddedToBuffer.add(triggeringEvent);

    // Calling checkAndFlushBufferIfNecessary manually to replicate the behavior of the executor
    // service
    eventListener.checkAndFlushBufferIfNecessary(realm1, false);
    verify(polarisMetaStoreManager, times(1)).writeEvents(any(), eq(eventsAddedToBuffer));
    verify(polarisMetaStoreManager, times(0)).writeEvents(any(), eq(eventsAddedToBufferRealm2));
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
                  eventListener.checkAndFlushBufferIfNecessary(realmId, false);
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
    verify(polarisMetaStoreManager, times(1)).writeEvents(any(), eq(events));
  }

  @Execution(ExecutionMode.SAME_THREAD)
  @Test
  public void testProcessEventIsThreadSafe() throws Exception {
    String realmId = "realm1";
    when(callContext.getRealmContext()).thenReturn(() -> realmId);
    int threadCount = 10;
    List<Thread> threads = new ArrayList<>();
    ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<PolarisEvent> allEvents = new ConcurrentLinkedQueue<>();
    eventListener.start();

    for (int i = 0; i < threadCount; i++) {
      Thread t =
          new Thread(
              () -> {
                try {
                  for (int j = 0; j < 10; j++) {
                    PolarisEvent event = createSampleEvent();
                    allEvents.add(event);
                    eventListener.processEvent(event);
                  }
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
          "Exceptions occurred in concurrent processEvent: ", exceptions.peek());
    }

    Awaitility.await("expected amount of records should be processed")
        .atMost(Duration.ofSeconds(30))
        .pollDelay(Duration.ofMillis(500))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              clock.add(500, ChronoUnit.MILLIS);
              ArgumentCaptor<List<PolarisEvent>> eventsCaptor = ArgumentCaptor.captor();
              verify(polarisMetaStoreManager, atLeastOnce()).writeEvents(any(), eventsCaptor.capture());
              List<PolarisEvent> eventsProcessed =
                  eventsCaptor.getAllValues().stream().flatMap(List::stream).toList();
              if (eventsProcessed.size() > 100) {
                eventsProcessed = new ArrayList<>();
              }
              assertThat(eventsProcessed.size()).isGreaterThanOrEqualTo(allEvents.size());
            });
    ArgumentCaptor<List<PolarisEvent>> eventsCaptor = ArgumentCaptor.captor();
    verify(polarisMetaStoreManager, atLeastOnce()).writeEvents(any(), eventsCaptor.capture());
    List<PolarisEvent> seenEvents =
        eventsCaptor.getAllValues().stream().flatMap(List::stream).toList();
    assertThat(seenEvents.size()).isEqualTo(allEvents.size());
    assertThat(seenEvents).hasSameElementsAs(allEvents);
  }

  @Test
  public void testRequestIdFunctionalityWithContainerRequestContext() {
    // Test when containerRequestContext has requestId property
    ContainerRequestContext mockContainerRequestContext =
        Mockito.mock(ContainerRequestContext.class);
    String expectedRequestId = "custom-request-id-123";

    when(mockContainerRequestContext.hasProperty("requestId")).thenReturn(true);
    when(mockContainerRequestContext.getProperty("requestId")).thenReturn(expectedRequestId);

    eventListener.containerRequestContext = mockContainerRequestContext;

    String actualRequestId = eventListener.getRequestId();
    assertThat(actualRequestId)
        .as("Expected requestId '" + expectedRequestId + "' but got '" + actualRequestId + "'")
        .isEqualTo(expectedRequestId);
  }

  @Test
  public void testRequestIdFunctionalityWithoutContainerRequestContext() {
    // Test when containerRequestContext is null
    try {
      java.lang.reflect.Field field =
          InMemoryBufferPolarisPersistenceEventListener.class.getDeclaredField(
              "containerRequestContext");
      field.setAccessible(true);
      field.set(eventListener, null);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set containerRequestContext field", e);
    }

    String requestId1 = eventListener.getRequestId();
    String requestId2 = eventListener.getRequestId();

    assertThat(requestId1 == null).isTrue();
    assertThat(requestId2 == null).isTrue();
  }

  @Test
  public void testRequestIdFunctionalityWithContainerRequestContextButNoProperty() {
    // Test when containerRequestContext exists but doesn't have requestId property
    ContainerRequestContext mockContainerRequestContext =
        Mockito.mock(ContainerRequestContext.class);
    when(mockContainerRequestContext.hasProperty("requestId")).thenReturn(false);
    eventListener.containerRequestContext = mockContainerRequestContext;

    String requestId = eventListener.getRequestId();

    assertThat(requestId == null).isTrue();

    // Verify that getProperty was never called since hasProperty returned false
    verify(mockContainerRequestContext, times(0)).getProperty("requestId");
  }

  private List<PolarisEvent> addEventsWithoutTriggeringFlush(String realmId) {
    List<PolarisEvent> realmEvents = new ArrayList<>();
    for (int i = 0; i < CONFIG_MAX_BUFFER_SIZE - 1; i++) {
      realmEvents.add(createSampleEvent());
    }
    RealmContext realmContext = () -> realmId;
    when(callContext.getRealmContext()).thenReturn(realmContext);
    for (PolarisEvent realmEvent : realmEvents) {
      eventListener.processEvent(realmEvent);
    }
    verify(polarisMetaStoreManager, times(0)).writeEvents(any(), any());
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
    event.setAdditionalProperties(additionalParams);

    return event;
  }
}
