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

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import org.apache.polaris.core.PolarisCallContext;
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
  private static final String REQUEST_ID_KEY = "requestId";
  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final Clock clock;

  private final ConcurrentHashMap<String, ConcurrentLinkedQueue<EventAndContext>> buffer =
      new ConcurrentHashMap<>();
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  private final ConcurrentHashMap<Future<?>, Integer> futures = new ConcurrentHashMap<>();
  private final Duration timeToFlush;
  private final int maxBufferSize;

  @Context
  ContainerRequestContext containerRequestContext;

  private record EventAndContext(PolarisEvent polarisEvent, PolarisCallContext callContext) {}

  @Inject
  public InMemoryBufferPolarisPersistenceEventListener(
      MetaStoreManagerFactory metaStoreManagerFactory,
      Clock clock,
      InMemoryBufferPersistenceListenerConfiguration eventListenerConfiguration) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.clock = clock;
    this.timeToFlush =
        eventListenerConfiguration.bufferTime().orElse(Duration.of(30, ChronoUnit.SECONDS));
    this.maxBufferSize = eventListenerConfiguration.maxBufferSize().orElse(5); // 5 events default
  }

  @PostConstruct
  void start() {
    futures.put(
        executor.scheduleAtFixedRate(
            this::runCleanup, 0, timeToFlush.toMillis(), TimeUnit.MILLISECONDS),
        1);
  }

  void runCleanup() {
    for (String realmId : buffer.keySet()) {
      try {
        checkAndFlushBufferIfNecessary(realmId);
      } catch (Exception e) {
        LOGGER.debug("Buffer checking task failed for realm ({}): {}", realmId, e);
      }
    }
    // Clean up futures
    try {
      futures.keySet().removeIf(future -> future.isCancelled() || future.isDone());
    } catch (Exception e) {
      LOGGER.debug("Futures reaper task failed.");
    }
  }

  @PreDestroy
  void shutdown() {
    futures.keySet().forEach(future -> future.cancel(false));
    executor.shutdownNow();
  }

  @Override
  String getRequestId() {
    if (containerRequestContext != null && containerRequestContext.hasProperty(REQUEST_ID_KEY)) {
      return (String) containerRequestContext.getProperty(REQUEST_ID_KEY);
    }
    return UUID.randomUUID().toString();
  }

  @Override
  void addToBuffer(PolarisEvent polarisEvent, CallContext callCtx) {
    String realmId = callCtx.getRealmContext().getRealmIdentifier();

    buffer
        .computeIfAbsent(realmId, k -> new ConcurrentLinkedQueue<>())
        .add(new EventAndContext(polarisEvent, callCtx.getPolarisCallContext().copy()));
    futures.put(executor.submit(() -> checkAndFlushBufferIfNecessary(realmId)), 1);
  }

  @VisibleForTesting
  public void checkAndFlushBufferIfNecessary(String realmId) {
    ConcurrentLinkedQueue<EventAndContext> queue = buffer.get(realmId);
    if (queue == null || queue.isEmpty()) {
      return;
    }

    // Given that we are using a ConcurrentLinkedQueue, this should not lock any calls to `add` on
    // the queue.
    synchronized (queue) {
      // Double-check inside synchronized block
      if (queue.isEmpty()) {
        return;
      }

      EventAndContext head = queue.peek();
      if (head == null) {
        return;
      }

      Duration elapsed = Duration.ofMillis(clock.millis() - head.polarisEvent.getTimestampMs());

      if (elapsed.compareTo(timeToFlush) > 0 || queue.size() >= maxBufferSize) {
        // Atomically replace old queue with new queue
        boolean replaced = buffer.replace(realmId, queue, new ConcurrentLinkedQueue<>());
        if (!replaced) {
          // Another thread concurrently modified the buffer, so do not continue
          return;
        }

        metaStoreManagerFactory
            .getOrCreateMetaStoreManager(() -> realmId)
            .writeEvents(
                head.callContext(),
                new ArrayList<>(queue.stream().map(EventAndContext::polarisEvent).toList()));
      }
    }
  }
}
