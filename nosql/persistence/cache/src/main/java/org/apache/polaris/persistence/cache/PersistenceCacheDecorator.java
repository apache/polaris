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
package org.apache.polaris.persistence.cache;

import static org.apache.polaris.persistence.cache.CacheConfig.DEFAULT_ENABLE;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.polaris.persistence.api.Persistence;
import org.apache.polaris.persistence.api.PersistenceDecorator;

/**
 * Decorator adding the application global cache to a {@link Persistence}, exposes priority {@value
 * #PRIORITY}.
 */
@ApplicationScoped
class PersistenceCacheDecorator implements PersistenceDecorator, CacheSupport {

  static int PRIORITY = 1000;

  private final CacheConfig cacheConfig;
  private final Instance<MeterRegistry> meterRegistry;

  private CacheBackend cacheBackend;

  @Inject
  PersistenceCacheDecorator(CacheConfig cacheConfig, Instance<MeterRegistry> meterRegistry) {
    this.cacheConfig = cacheConfig;
    this.meterRegistry = meterRegistry;
  }

  @PostConstruct
  void init() {
    this.cacheBackend =
        cacheConfig.enable().orElse(DEFAULT_ENABLE)
            ? PersistenceCaches.newBackend(cacheConfig, meterRegistry.stream().findAny())
            : null;
  }

  @Override
  public boolean active() {
    return cacheBackend != null;
  }

  @Override
  public int priority() {
    return PRIORITY;
  }

  @Override
  public Persistence decorate(Persistence persistence) {
    return cacheBackend.wrap(persistence);
  }

  @Override
  public void purgeWholeCache() {
    if (cacheBackend != null) {
      cacheBackend.purge();
    }
  }

  @Override
  public void purgeRealm(String realmId) {
    if (cacheBackend != null) {
      cacheBackend.clear(realmId);
    }
  }

  @Override
  public long estimatedSize() {
    return cacheBackend != null ? cacheBackend.estimatedSize() : 0L;
  }
}
