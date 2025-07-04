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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.events.EventListenerConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.threeten.extra.MutableClock;

public class InMemoryBufferPolarisPersistenceEventListenerTest {
  private InMemoryBufferPolarisPersistenceEventListener eventListener;
  private PolarisConfigurationStore configurationStore;
  private BasePersistence basePersistence;
  private MutableClock clock;
  private CallContext callContext;

  private static final int CONFIG_MAX_BUFFER_SIZE = 5;
  private static final long CONFIG_TIME_TO_FLUSH_IN_MS = 500;

  @BeforeEach
  public void setUp() {
    callContext = Mockito.mock(CallContext.class);
    basePersistence = mock(BasePersistence.class);
    Supplier basePersistenceSupplier = () -> basePersistence;
    MetaStoreManagerFactory metaStoreManagerFactory = Mockito.mock(MetaStoreManagerFactory.class);
    when(metaStoreManagerFactory.getOrCreateSessionSupplier(Mockito.any()))
        .thenReturn(basePersistenceSupplier);

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
            metaStoreManagerFactory, configurationStore, clock, eventListenerConfiguration);
  }

  @Test
  public void testAddToBufferFlushesAfterConfiguredTime() {
    String realmId = "realm1";
    List<PolarisEvent> eventsAddedToBuffer = addEventsWithoutTriggeringFlush(realmId);

    // Push clock forwards to flush the buffer
    clock.add(CONFIG_TIME_TO_FLUSH_IN_MS * 2, ChronoUnit.MILLIS);
    eventListener.checkAndFlushBufferIfNecessary(realmId);
    verify(basePersistence, times(1)).writeEvents(eq(eventsAddedToBuffer));
  }

  @Test
  public void testAddToBufferFlushesAfterMaxEvents() {
    List<PolarisEvent> eventsAddedToBuffer = addEventsWithoutTriggeringFlush("realm1");
    addEventsWithoutTriggeringFlush("realm2");

    // Add the last event for realm1 and verify that it did trigger the flush
    PolarisEvent triggeringEvent = new PolarisEvent();
    RealmContext realmContext = () -> "realm1";
    when(callContext.getRealmContext()).thenReturn(realmContext);
    eventListener.addToBuffer(triggeringEvent, callContext);
    eventsAddedToBuffer.add(triggeringEvent);
    verify(basePersistence, times(1)).writeEvents(eq(eventsAddedToBuffer));
  }

  private List<PolarisEvent> addEventsWithoutTriggeringFlush(String realmId) {
    List<PolarisEvent> realmEvents = new ArrayList<>();
    for (int i = 0; i < CONFIG_MAX_BUFFER_SIZE - 1; i++) {
      realmEvents.add(new PolarisEvent());
    }
    RealmContext realmContext = () -> realmId;
    when(callContext.getRealmContext()).thenReturn(realmContext);
    for (int i = 0; i < realmEvents.size(); i++) {
      eventListener.addToBuffer(realmEvents.get(i), callContext);
    }
    verify(basePersistence, times(0)).writeEvents(Mockito.any());
    return realmEvents;
  }
}
