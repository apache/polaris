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
package org.apache.polaris.persistence.nosql.weld;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.polaris.ids.api.IdGeneratorSpec;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.nosql.async.AsyncConfiguration;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.backend.BackendConfiguration;
import org.apache.polaris.persistence.nosql.api.cache.CacheConfig;
import org.apache.polaris.persistence.nosql.api.cache.CacheSizing;
import org.apache.polaris.persistence.nosql.inmemory.InMemoryConfiguration;
import org.apache.polaris.persistence.nosql.nodeids.api.NodeManagementConfig;

@ApplicationScoped
public class CdiTestingProviders {

  @Produces
  @ApplicationScoped
  AsyncConfiguration asyncConfiguration() {
    return AsyncConfiguration.builder().build();
  }

  @Produces
  @ApplicationScoped
  BackendConfiguration backendConfiguration() {
    return BackendConfiguration.BuildableBackendConfiguration.builder().type("InMemory").build();
  }

  @Produces
  @ApplicationScoped
  InMemoryConfiguration inMemoryConfiguration() {
    return new InMemoryConfiguration() {};
  }

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Produces
  @ApplicationScoped
  CacheConfig cacheConfig(MonotonicClock monotonicClock) {
    return CacheConfig.BuildableCacheConfig.builder()
        .sizing(CacheSizing.builder().fixedSize(MemorySize.ofMega(32)).build())
        .clockNanos(monotonicClock::nanoTime)
        .build();
  }

  @Produces
  @ApplicationScoped
  NodeManagementConfig nodeManagementConfig() {
    return NodeManagementConfig.BuildableNodeManagementConfig.builder()
        .idGeneratorSpec(IdGeneratorSpec.BuildableIdGeneratorSpec.builder().build())
        .build();
  }

  @Produces
  @ApplicationScoped
  PersistenceParams persistenceBaseConfig() {
    return PersistenceParams.BuildablePersistenceParams.builder().build();
  }

  private ScheduledExecutorService executorService;

  @PostConstruct
  void initScheduler() {
    executorService = Executors.newScheduledThreadPool(2);
  }

  @PreDestroy
  void stopScheduler() {
    // "Forget" tasks scheduled in the future
    executorService.shutdownNow();
    // Properly close
    executorService.close();
  }
}
