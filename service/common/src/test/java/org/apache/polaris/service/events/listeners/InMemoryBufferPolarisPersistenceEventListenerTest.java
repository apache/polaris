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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.container.ContainerRequestContext;
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

    InMemoryBufferPersistenceListenerConfiguration eventListenerConfiguration =
        Mockito.mock(InMemoryBufferPersistenceListenerConfiguration.class);
    when(eventListenerConfiguration.maxBufferSize()).thenReturn(CONFIG_MAX_BUFFER_SIZE);
    when(eventListenerConfiguration.bufferTime()).thenReturn(CONFIG_TIME_TO_FLUSH_IN_MS);


    clock =
        MutableClock.of(
            Instant.ofEpochSecond(0), ZoneOffset.UTC); // Use 0 Epoch Time to make it easier to test

    eventListener =
        new InMemoryBufferPolarisPersistenceEventListener(
            metaStoreManagerFactory, clock, eventListenerConfiguration);

    // Use reflection to set the callContext field
    try {
      java.lang.reflect.Field field = InMemoryBufferPolarisPersistenceEventListener.class.getDeclaredField("callContext");
      field.setAccessible(true);
      field.set(eventListener, callContext);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set callContext field", e);
    }
  }

  @Test
  public void testAddToBufferFlushesAfterConfiguredTime() {
    String realmId = "realm1";
    List<PolarisEvent> eventsAddedToBuffer = addEventsWithoutTriggeringFlush(realmId);

    // Push clock forwards to flush the buffer
    clock.add(CONFIG_TIME_TO_FLUSH_IN_MS.multipliedBy(2));
    eventListener.checkAndFlushBufferIfNecessary(realmId, false);
    verify(polarisMetaStoreManager, times(1))
        .writeEvents(eq(callContext.getPolarisCallContext()), eq(eventsAddedToBuffer));
  }

  @Test
  public void testAddToBufferFlushesAfterMaxEvents() {
    String realm1 = "realm1";
    List<PolarisEvent> eventsAddedToBuffer = addEventsWithoutTriggeringFlush(realm1);
    List<PolarisEvent> eventsAddedToBufferRealm2 = addEventsWithoutTriggeringFlush("realm2");

    // Add the last event for realm1 and verify that it did trigger the flush
    PolarisEvent triggeringEvent = createSampleEvent();
    RealmContext realmContext = () -> realm1;
    when(callContext.getRealmContext()).thenReturn(realmContext);
    eventListener.addToBuffer(triggeringEvent);
    eventsAddedToBuffer.add(triggeringEvent);

    // Calling checkAndFlushBufferIfNecessary manually to replicate the behavior of the executor
    // service
    eventListener.checkAndFlushBufferIfNecessary(realm1, false);
    verify(polarisMetaStoreManager, times(1))
        .writeEvents(eq(callContext.getPolarisCallContext()), eq(eventsAddedToBuffer));
    verify(polarisMetaStoreManager, times(0))
        .writeEvents(eq(callContext.getPolarisCallContext()), eq(eventsAddedToBufferRealm2));
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
    verify(polarisMetaStoreManager, times(1))
        .writeEvents(eq(callContext.getPolarisCallContext()), eq(events));
  }

  @Test
  public void testRequestIdFunctionalityWithContainerRequestContext() {
    // Test when containerRequestContext has requestId property
    ContainerRequestContext mockContainerRequestContext =
        Mockito.mock(ContainerRequestContext.class);
    String expectedRequestId = "custom-request-id-123";

    when(mockContainerRequestContext.hasProperty("requestId")).thenReturn(true);
    when(mockContainerRequestContext.getProperty("requestId")).thenReturn(expectedRequestId);

    // Use reflection to set the containerRequestContext field
    try {
      java.lang.reflect.Field field =
          InMemoryBufferPolarisPersistenceEventListener.class.getDeclaredField(
              "containerRequestContext");
      field.setAccessible(true);
      field.set(eventListener, mockContainerRequestContext);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set containerRequestContext field", e);
    }

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

    // Both should be valid UUIDs
    assertThat(isValidUUID(requestId1))
        .as("Generated requestId should be a valid UUID: " + requestId1)
        .isTrue();
    assertThat(isValidUUID(requestId2))
        .as("Generated requestId should be a valid UUID: " + requestId2)
        .isTrue();

    // Each call should generate a different UUID
    assertThat(requestId1)
        .as("Each call to getRequestId() should generate a different UUID")
        .isNotEqualTo(requestId2);
  }

  @Test
  public void testRequestIdFunctionalityWithContainerRequestContextButNoProperty() {
    // Test when containerRequestContext exists but doesn't have requestId property
    ContainerRequestContext mockContainerRequestContext =
        Mockito.mock(ContainerRequestContext.class);
    when(mockContainerRequestContext.hasProperty("requestId")).thenReturn(false);

    try {
      java.lang.reflect.Field field =
          InMemoryBufferPolarisPersistenceEventListener.class.getDeclaredField(
              "containerRequestContext");
      field.setAccessible(true);
      field.set(eventListener, mockContainerRequestContext);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set containerRequestContext field", e);
    }

    String requestId = eventListener.getRequestId();

    // Should generate a UUID since property is not available
    assertThat(isValidUUID(requestId))
        .as("Generated requestId should be a valid UUID: " + requestId)
        .isTrue();

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
      eventListener.addToBuffer(realmEvent);
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
    event.setAdditionalProperties(additionalParams);

    return event;
  }

  private boolean isValidUUID(String uuid) {
    try {
      UUID.fromString(uuid);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
