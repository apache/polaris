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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Event listener that buffers in memory and then dumps to persistence. */
@ApplicationScoped
@Identifier("persistence-in-memory-buffer")
public class InMemoryBufferPolarisPersistenceEventListener extends PolarisPersistenceEventListener {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(InMemoryBufferPolarisPersistenceEventListener.class);
  MetaStoreManagerFactory metaStoreManagerFactory;
  PolarisConfigurationStore polarisConfigurationStore;

  private HashMap<String, List<PolarisEvent>> buffer = new HashMap<>();
  private final ScheduledExecutorService thread = Executors.newSingleThreadScheduledExecutor();
  private final int timeToFlush;
  private final int maxBufferSize;

  @Inject
  public InMemoryBufferPolarisPersistenceEventListener(
      MetaStoreManagerFactory metaStoreManagerFactory,
      PolarisConfigurationStore polarisConfigurationStore) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.polarisConfigurationStore = polarisConfigurationStore;
    this.timeToFlush =
        polarisConfigurationStore.getConfiguration(
            null, FeatureConfiguration.EVENT_BUFFER_TIME_TO_FLUSH_IN_MS);
    this.maxBufferSize =
        polarisConfigurationStore.getConfiguration(
            null, FeatureConfiguration.EVENT_BUFFER_MAX_SIZE);

    Runnable bufferCheckTask =
        () -> {
          try {
            for (String realmId : buffer.keySet()) {
              checkAndFlushBufferIfNecessary(realmId);
            }
          } catch (Exception e) {
            LOGGER.debug("Buffer checking task failed: ", e);
          }
        };

    var future = thread.scheduleAtFixedRate(bufferCheckTask, 0, timeToFlush, TimeUnit.MILLISECONDS);
  }

  @Override
  void addToBuffer(PolarisEvent polarisEvent, CallContext callCtx) {
    String realmId = callCtx.getRealmContext().getRealmIdentifier();
    buffer.computeIfAbsent(realmId, k -> new ArrayList<>()).add(polarisEvent);
    checkAndFlushBufferIfNecessary(realmId);
  }

  private void checkAndFlushBufferIfNecessary(String realmId) {
    if (!buffer.containsKey(realmId) || buffer.get(realmId).isEmpty()) {
      return;
    }
    if (System.currentTimeMillis() - buffer.get(realmId).getFirst().getTimestampMs()
            > this.timeToFlush
        || buffer.size() >= this.maxBufferSize) {
      List<PolarisEvent> bufferToFlush = buffer.get(realmId);
      buffer.put(realmId, new ArrayList<>());
      metaStoreManagerFactory
          .getOrCreateSessionSupplier(() -> realmId)
          .get()
          .writeEvents(bufferToFlush);
    }
  }
}
