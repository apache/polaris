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
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.service.events.listeners;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threeten.extra.MutableClock;

public class FileBufferPolarisPersistenceEventListenerTest {
  private final MetaStoreManagerFactory metaStoreManagerFactory =
      mock(MetaStoreManagerFactory.class);
  private final PolarisConfigurationStore polarisConfigurationStore =
      mock(PolarisConfigurationStore.class);
  private FileBufferPolarisPersistenceEventListener listener;
  private static final int numShards = 2; // Assuming 2 shards for testing

  @BeforeEach
  void setUp() {
    when(polarisConfigurationStore.getConfiguration(
            null, FeatureConfiguration.EVENT_BUFFER_MAX_SIZE))
        .thenReturn(5);
    when(polarisConfigurationStore.getConfiguration(
            null, FeatureConfiguration.EVENT_BUFFER_NUM_SHARDS))
        .thenReturn(numShards);
    when(polarisConfigurationStore.getConfiguration(
            null, FeatureConfiguration.EVENT_BUFFER_TIME_TO_FLUSH_IN_MS))
        .thenReturn(60000);
    when(polarisConfigurationStore.getConfiguration(
            null, FeatureConfiguration.EVENT_BUFFER_NUM_THREADS))
        .thenReturn(2);
    MutableClock clock = mock(MutableClock.class);
    listener =
        spy(
            new FileBufferPolarisPersistenceEventListener(
                metaStoreManagerFactory, polarisConfigurationStore, clock));
  }

  @Test
  void testAddToBufferRotatesShardWhenMaxBufferSizeReached() {
    CallContext mockCallContext = mock(CallContext.class);
    RealmContext mockRealmContext = mock(RealmContext.class);
    PolarisEvent mockEvent = mock(PolarisEvent.class);

    when(mockCallContext.getRealmContext()).thenReturn(mockRealmContext);
    when(mockRealmContext.getRealmIdentifier()).thenReturn("realm1");

    // Simulate adding events, does not exceed max size
    for (int i = 0; i < 4; i++) {
      listener.addToBuffer(mockEvent, mockCallContext);
    }

    int shardNum = mockEvent.hashCode() % numShards;

    verify(listener, times(0)).rotateShard("realm1", shardNum);

    listener.addToBuffer(mockEvent, mockCallContext);
    verify(listener, times(1)).rotateShard("realm1", shardNum);
  }
}
