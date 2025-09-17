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

import static org.apache.polaris.service.logging.LoggingMDCFilter.REQUEST_ID_KEY;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.SecurityContext;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.events.listeners.InMemoryBufferEventListenerConfiguration;
import org.apache.polaris.service.events.listeners.PolarisPersistenceEventListener;
import org.eclipse.microprofile.faulttolerance.Fallback;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Identifier("persistence-in-memory")
public class InMemoryEventListener extends PolarisPersistenceEventListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryEventListener.class);

  @Inject CallContext callContext;
  @Inject Clock clock;
  @Inject MetaStoreManagerFactory metaStoreManagerFactory;
  @Inject InMemoryBufferEventListenerConfiguration configuration;

  @Context SecurityContext securityContext;
  @Context ContainerRequestContext requestContext;

  @VisibleForTesting
  final LoadingCache<String, UnicastProcessor<PolarisEvent>> processors =
      Caffeine.newBuilder()
          .expireAfterAccess(Duration.ofHours(1))
          .evictionListener(
              (String realmId, UnicastProcessor<?> processor, RemovalCause cause) ->
                  processor.onComplete())
          .build(this::createProcessor);

  @Override
  protected void processEvent(PolarisEvent event) {
    var realmId = callContext.getRealmContext().getRealmIdentifier();
    processEvent(realmId, event);
  }

  protected void processEvent(String realmId, PolarisEvent event) {
    var processor = Objects.requireNonNull(processors.get(realmId));
    processor.onNext(event);
  }

  @Override
  protected ContextSpecificInformation getContextSpecificInformation() {
    var principal = securityContext.getUserPrincipal();
    var principalName = principal == null ? null : principal.getName();
    return new ContextSpecificInformation(clock.millis(), principalName);
  }

  @Nullable
  @Override
  protected String getRequestId() {
    return (String) requestContext.getProperty(REQUEST_ID_KEY);
  }

  @PreDestroy
  public void shutdown() {
    processors.asMap().values().forEach(UnicastProcessor::onComplete);
    processors.invalidateAll(); // doesn't call the eviction listener
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
    var callContext = new PolarisCallContext(realmContext, basePersistence);
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
