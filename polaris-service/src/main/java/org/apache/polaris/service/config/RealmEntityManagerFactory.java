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
package org.apache.polaris.service.config;

import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;

import jakarta.inject.Provider;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.context.RealmScope;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Gets or creates PolarisEntityManager instances based on config values and RealmContext. */
@RealmScope
public class RealmEntityManagerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealmEntityManagerFactory.class);
  private final MetaStoreManagerFactory metaStoreManagerFactory;

  // Key: realmIdentifier
  private final Map<String, PolarisEntityManager> cachedEntityManagers = new HashMap<>();
  private final Provider<EntityCache> entityCache;

  // Subclasses for test injection.
  protected RealmEntityManagerFactory() {
    this.metaStoreManagerFactory = null;
    this.entityCache = null;
  }

  @Inject
  public RealmEntityManagerFactory(
      MetaStoreManagerFactory metaStoreManagerFactory, Provider<EntityCache> entityCache) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.entityCache = entityCache;
  }

  public PolarisEntityManager getOrCreateEntityManager(RealmContext context) {
    String realm = context.getRealmIdentifier();

    LOGGER.debug("Looking up PolarisEntityManager for realm {}", realm);

    return cachedEntityManagers.computeIfAbsent(
        realm,
        r -> {
          LOGGER.info("Initializing new PolarisEntityManager for realm {}", r);
          return new PolarisEntityManager(
              metaStoreManagerFactory.getOrCreateMetaStoreManager(context),
              metaStoreManagerFactory.getOrCreateStorageCredentialCache(context),
              entityCache.get());
        });
  }
}
