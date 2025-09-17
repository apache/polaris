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
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.SecurityContext;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
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

  private final ConcurrentHashMap<String, ConcurrentLinkedQueueWithApproximateSize<PolarisEvent>>
      buffer = new ConcurrentHashMap<>();
  private final ScheduledExecutorService executor;
  private final ConcurrentHashMap<String, Future<?>> futures = new ConcurrentHashMap<>();
  private ScheduledFuture<?> scheduledFuture;
  private final Duration timeToFlush;
  private final int maxBufferSize;

  @Inject CallContext callContext;
  @Inject Clock clock;
  @Context SecurityContext securityContext;
  @Context ContainerRequestContext containerRequestContext;

  @Inject
  public InMemoryBufferPolarisPersistenceEventListener(
      MetaStoreManagerFactory metaStoreManagerFactory,
      Clock clock,
      InMemoryBufferEventListenerConfiguration eventListenerConfiguration) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.clock = clock;
    this.timeToFlush = eventListenerConfiguration.bufferTime();
    this.maxBufferSize = eventListenerConfiguration.maxBufferSize();

    executor = Executors.newSingleThreadScheduledExecutor();
  }

  @PostConstruct
  void start() {
    scheduledFuture =
        executor.scheduleAtFixedRate(
            this::runCleanup, 0, timeToFlush.toMillis(), TimeUnit.MILLISECONDS);
  }

  void runCleanup() {
    for (String realmId : buffer.keySet()) {
      try {
        checkAndFlushBufferIfNecessary(realmId, false);
      } catch (Exception e) {
        LOGGER.debug("Buffer checking task failed for realm ({}): {}", realmId, e);
      }
    }
  }

  @PreDestroy
  void shutdown() {
    scheduledFuture.cancel(false);
    futures.forEach((key, future) -> future.cancel(false));
    executor.shutdown();

    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          LOGGER.warn("Executor did not shut down cleanly");
        }
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    } finally {
      for (String realmId : buffer.keySet()) {
        try {
          checkAndFlushBufferIfNecessary(realmId, true);
        } catch (Exception e) {
          LOGGER.debug("Buffer flushing task failed for realm ({}): ", realmId, e);
        }
      }
    }
  }

  @Nullable
  @Override
  String getRequestId() {
    if (containerRequestContext != null && containerRequestContext.hasProperty(REQUEST_ID_KEY)) {
      return (String) containerRequestContext.getProperty(REQUEST_ID_KEY);
    }
    return null;
  }

  @Override
  void processEvent(PolarisEvent polarisEvent) {
    String realmId = callContext.getRealmContext().getRealmIdentifier();

    ConcurrentLinkedQueueWithApproximateSize<PolarisEvent> realmQueue =
        buffer.computeIfAbsent(realmId, k -> new ConcurrentLinkedQueueWithApproximateSize<>());
    realmQueue.add(polarisEvent);
    if (realmQueue.size() >= maxBufferSize) {
      futures.compute(
          realmId,
          (k, v) -> {
            if (v == null || v.isDone()) {
              return executor.submit(() -> checkAndFlushBufferIfNecessary(realmId, true));
            }
            return v;
          });
    }
  }

  @VisibleForTesting
  void checkAndFlushBufferIfNecessary(String realmId, boolean forceFlush) {
    ConcurrentLinkedQueueWithApproximateSize<PolarisEvent> queue = buffer.get(realmId);
    if (queue == null || queue.isEmpty()) {
      return;
    }

    PolarisEvent head = queue.peek();
    if (head == null) {
      return;
    }

    Duration elapsed = Duration.ofMillis(clock.millis() - head.getTimestampMs());

    if (elapsed.compareTo(timeToFlush) > 0 || queue.size() >= maxBufferSize || forceFlush) {
      // Atomically replace old queue with new queue
      boolean replaced =
          buffer.replace(realmId, queue, new ConcurrentLinkedQueueWithApproximateSize<>());
      if (!replaced) {
        // Another thread concurrently modified the buffer, so do not continue
        return;
      }

      RealmContext realmContext = () -> realmId;
      PolarisMetaStoreManager metaStoreManager =
          metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
      BasePersistence basePersistence = metaStoreManagerFactory.getOrCreateSession(realmContext);
      metaStoreManager.writeEvents(
          new PolarisCallContext(realmContext, basePersistence), queue.stream().toList());

      if (buffer.get(realmId).size() >= maxBufferSize) {
        // Ensure that all events will be flushed, even if the race condition is triggered where
        // new events are added between replacing the buffer above and the finishing of this method.
        futures.put(realmId, executor.submit(() -> checkAndFlushBufferIfNecessary(realmId, true)));
      }
    }
  }

  @Override
  ContextSpecificInformation getContextSpecificInformation() {
    return new ContextSpecificInformation(
        clock.millis(),
        securityContext.getUserPrincipal() == null
            ? null
            : securityContext.getUserPrincipal().getName());
  }
}
