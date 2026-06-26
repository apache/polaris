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

package org.apache.polaris.service.events.listeners.inmemory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.events.listeners.PolarisPersistenceEventListener;
import org.eclipse.microprofile.faulttolerance.Fallback;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Identifier("persistence-in-memory-buffer")
public class InMemoryBufferEventListener extends PolarisPersistenceEventListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryBufferEventListener.class);

  @Inject MetaStoreManagerFactory metaStoreManagerFactory;
  @Inject InMemoryBufferEventListenerConfiguration configuration;

  /**
   * Thrown by {@link EventProcessor#onNext} when the wrapped processor has already completed (for
   * example, it was evicted between the cache lookup and the {@code onNext} call). {@link
   * #processEvent} catches it and retries with a freshly loaded processor.
   */
  private static final class CompletedException extends IllegalStateException {}

  /**
   * Wraps a {@link UnicastProcessor} together with its own lock, so that mutual exclusion between
   * {@code onNext} (from {@link #processEvent}) and {@code onComplete} (from eviction or shutdown)
   * does not depend on smallrye-mutiny's internal {@code synchronized} on {@code
   * UnicastProcessor.onNext}.
   */
  protected final class EventProcessor {

    @VisibleForTesting final UnicastProcessor<EventEntity> processor;
    private final ReentrantLock lock = new ReentrantLock();
    private boolean completed = false; // guarded by lock

    EventProcessor(String realmId) {
      processor = UnicastProcessor.create();
      processor
          .emitOn(Infrastructure.getDefaultWorkerPool())
          .group()
          .intoLists()
          .of(configuration.maxBufferSize(), configuration.bufferTime())
          .subscribe()
          .with(events -> flush(realmId, events), error -> onProcessorError(realmId, error));
    }

    void onNext(EventEntity event) {
      lock.lock();
      try {
        if (completed) {
          throw new CompletedException();
        }
        processor.onNext(event);
      } finally {
        lock.unlock();
      }
    }

    void onComplete() {
      lock.lock();
      try {
        if (!completed) {
          completed = true;
          processor.onComplete();
        }
      } finally {
        lock.unlock();
      }
    }
  }

  private final LoadingCache<String, EventProcessor> processors =
      Caffeine.newBuilder()
          .expireAfterAccess(Duration.ofHours(1))
          .removalListener(
              (String realmId, EventProcessor processor, RemovalCause cause) -> {
                if (processor != null) {
                  processor.onComplete();
                }
              })
          .build(this::createProcessor);

  // Construct via a method (not EventProcessor::new) so that when the cache lives on a CDI client
  // proxy, the call is delegated to the contextual bean instance, whose injected configuration is
  // non-null. A direct inner-class instantiation would capture the proxy as the enclosing instance
  // and read its uninjected (null) configuration.
  protected EventProcessor createProcessor(String realmId) {
    return new EventProcessor(realmId);
  }

  private final ReentrantReadWriteLock shutdownLock = new ReentrantReadWriteLock();
  private boolean shutdown = false; // guarded by shutdownLock

  @Override
  protected void processEvent(String realmId, EventEntity event) {
    shutdownLock.readLock().lock();
    try {
      if (shutdown) {
        return;
      }
      while (true) {
        var processor = Objects.requireNonNull(processors.get(realmId));
        try {
          processor.onNext(event);
          return;
        } catch (CompletedException ignored) {
          // processor was evicted between the cache lookup and onNext; retry with a fresh one
        }
      }
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  @PreDestroy
  public void shutdown() {
    shutdownLock.writeLock().lock();
    try {
      shutdown = true;
      processors.invalidateAll();
    } finally {
      shutdownLock.writeLock().unlock();
    }
  }

  @Retry(maxRetries = 5, delay = 1000, jitter = 100)
  @Fallback(fallbackMethod = "onFlushError")
  protected void flush(String realmId, List<EventEntity> events) {
    RealmContext realmContext = () -> realmId;
    var metaStoreManager = metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
    var basePersistence = metaStoreManagerFactory.getOrCreateSession(realmContext);
    var callContext = new PolarisCallContext(realmContext, basePersistence);
    metaStoreManager.writeEvents(callContext, events);
  }

  @SuppressWarnings("unused")
  protected void onFlushError(String realmId, List<EventEntity> events, Throwable error) {
    LOGGER.error("Failed to persist {} events for realm '{}'", events.size(), realmId, error);
  }

  protected void onProcessorError(String realmId, Throwable error) {
    LOGGER.error(
        "Unexpected error while processing events for realm '{}'; some events may have been dropped",
        realmId,
        error);
    processors.invalidate(realmId);
  }

  @VisibleForTesting
  EventProcessor processor(String realmId) {
    return processors.get(realmId);
  }

  @VisibleForTesting
  boolean hasProcessor(String realmId) {
    return processors.getIfPresent(realmId) != null;
  }
}
