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
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEvent;
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

  @VisibleForTesting
  final LoadingCache<String, UnicastProcessor<PolarisEvent>> processors =
      Caffeine.newBuilder()
          .expireAfterAccess(Duration.ofHours(1))
          .evictionListener(
              (String realmId, UnicastProcessor<?> processor, RemovalCause cause) ->
                  completeSynchronized(processor))
          .build(this::createProcessor);

  @Override
  protected void processEvent(String realmId, PolarisEvent event) {
    var processor = Objects.requireNonNull(processors.get(realmId));
    // UnicastProcessor.onNext() is internally synchronized (smallrye-mutiny
    // UnicastProcessor declares onNext as `public synchronized void`), so concurrent
    // processEvent() calls for the same realm serialize on the processor's intrinsic
    // lock without an external guard.
    processor.onNext(event);
  }

  @PreDestroy
  public void shutdown() {
    processors.asMap().values().forEach(InMemoryBufferEventListener::completeSynchronized);
    processors.invalidateAll(); // doesn't call the eviction listener
  }

  /**
   * Calls {@link UnicastProcessor#onComplete()} while holding the processor's intrinsic monitor.
   *
   * <p>smallrye-mutiny's {@code UnicastProcessor.onNext} is method-{@code synchronized} on the
   * processor instance; {@code onComplete} is not. Acquiring the same intrinsic monitor here
   * restores symmetric mutual exclusion between concurrent {@code onNext} (from {@code
   * processEvent}) and {@code onComplete} (from eviction or shutdown). Wrapping the pattern as an
   * invariant keeps the synchronization requirement structurally visible to future maintainers.
   */
  private static void completeSynchronized(UnicastProcessor<?> processor) {
    synchronized (processor) {
      processor.onComplete();
    }
  }

  protected UnicastProcessor<PolarisEvent> createProcessor(String realmId) {
    UnicastProcessor<PolarisEvent> processor = UnicastProcessor.create();
    processor
        .emitOn(Infrastructure.getDefaultWorkerPool())
        .group()
        .intoLists()
        .of(configuration.maxBufferSize(), configuration.bufferTime())
        .subscribe()
        .with(events -> flush(realmId, events), error -> onProcessorError(realmId, error));
    return processor;
  }

  @Retry(maxRetries = 5, delay = 1000, jitter = 100)
  @Fallback(fallbackMethod = "onFlushError")
  protected void flush(String realmId, List<PolarisEvent> events) {
    RealmContext realmContext = () -> realmId;
    var metaStoreManager = metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
    var basePersistence = metaStoreManagerFactory.getOrCreateSession(realmContext);
    var metricsPersistence = metaStoreManagerFactory.getOrCreateMetricsPersistence(realmContext);
    var callContext = new PolarisCallContext(realmContext, basePersistence, metricsPersistence);
    metaStoreManager.writeEvents(callContext, events);
  }

  @SuppressWarnings("unused")
  protected void onFlushError(String realmId, List<PolarisEvent> events, Throwable error) {
    LOGGER.error("Failed to persist {} events for realm '{}'", events.size(), realmId, error);
  }

  protected void onProcessorError(String realmId, Throwable error) {
    LOGGER.error(
        "Unexpected error while processing events for realm '{}'; some events may have been dropped",
        realmId,
        error);
    processors.invalidate(realmId);
  }
}
