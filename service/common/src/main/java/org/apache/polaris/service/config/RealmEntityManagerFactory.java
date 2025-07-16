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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Gets or creates PolarisEntityManager instances based on config values and RealmContext. */
@ApplicationScoped
public class RealmEntityManagerFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealmEntityManagerFactory.class);

  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final PolarisConfigurationStore configurationStore;
  private final StorageCredentialCache storageCredentialCache;

  // Key: realmIdentifier
  private final Map<String, PolarisEntityManager> cachedEntityManagers = new ConcurrentHashMap<>();

  @Inject
  public RealmEntityManagerFactory(
      MetaStoreManagerFactory metaStoreManagerFactory,
      PolarisConfigurationStore configurationStore,
      StorageCredentialCache storageCredentialCache) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.configurationStore = configurationStore;
    this.storageCredentialCache = storageCredentialCache;
  }

  public PolarisEntityManager getOrCreateEntityManager(RealmContext context) {
    String realm = context.getRealmIdentifier();

    LOGGER.debug("Looking up PolarisEntityManager for realm {}", realm);

    return cachedEntityManagers.computeIfAbsent(
        realm,
        r -> {
          LOGGER.info("Initializing new PolarisEntityManager for realm {}", r);
          RealmConfig realmConfig = new RealmConfigImpl(configurationStore, context);
          return new PolarisEntityManager(
              metaStoreManagerFactory.getOrCreateMetaStoreManager(context),
              storageCredentialCache,
              metaStoreManagerFactory.getOrCreateEntityCache(context, realmConfig));
        });
  }
}
