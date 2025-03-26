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
package org.apache.polaris.persistence.nosql.impl.cache;

import static org.apache.polaris.persistence.nosql.api.cache.CacheConfig.DEFAULT_ENABLE;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceDecorator;
import org.apache.polaris.persistence.nosql.api.cache.CacheBackend;
import org.apache.polaris.persistence.nosql.api.cache.CacheConfig;
import org.apache.polaris.persistence.nosql.api.cache.DistributedCacheInvalidation;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

/**
 * Decorator adding the application global cache to a {@link Persistence}, exposes priority {@value
 * #PRIORITY}.
 */
@ApplicationScoped
class PersistenceCacheDecorator implements PersistenceDecorator {
  static int PRIORITY = 1000;

  private final CacheConfig cacheConfig;

  private final CacheBackend local;
  private final CacheBackend cacheBackend;

  @Inject
  PersistenceCacheDecorator(
      CacheConfig cacheConfig,
      @Any Instance<MeterRegistry> meterRegistry,
      @Any Instance<DistributedCacheInvalidation.Sender> invalidationSender) {
    this.cacheConfig = cacheConfig;

    if (!cacheConfig.enable().orElse(DEFAULT_ENABLE)) {
      local = cacheBackend = NoopCacheBackend.INSTANCE;
    } else {
      local = PersistenceCaches.newBackend(cacheConfig, meterRegistry.stream().findAny());
      cacheBackend =
          invalidationSender.isResolvable()
              ? new DistributedInvalidationsCacheBackend(local, invalidationSender.get())
              : local;
    }
  }

  @Produces
  CacheBackend cacheBackend() {
    return cacheBackend;
  }

  @Produces
  DistributedCacheInvalidation.Receiver distributedCacheInvalidationHandler() {
    return new DistributedCacheInvalidation.Receiver() {
      @Override
      public void evictObj(@Nonnull String realmId, @Nonnull ObjRef objRef) {
        local.remove(realmId, objRef);
      }

      @Override
      public void evictReference(@Nonnull String realmId, @Nonnull String refName) {
        local.removeReference(realmId, refName);
      }
    };
  }

  @Override
  public boolean active() {
    return cacheConfig.enable().orElse(DEFAULT_ENABLE);
  }

  @Override
  public int priority() {
    return PRIORITY;
  }

  @Override
  public Persistence decorate(Persistence persistence) {
    return cacheBackend.wrap(persistence);
  }
}
