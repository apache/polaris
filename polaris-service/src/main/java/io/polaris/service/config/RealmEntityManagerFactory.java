/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.service.config;

import io.polaris.core.context.RealmContext;
import io.polaris.core.persistence.MetaStoreManagerFactory;
import io.polaris.core.persistence.PolarisEntityManager;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Gets or creates PolarisEntityManager instances based on config values and RealmContext. */
public class RealmEntityManagerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealmEntityManagerFactory.class);
  private final MetaStoreManagerFactory metaStoreManagerFactory;

  // Key: realmIdentifier
  private final Map<String, PolarisEntityManager> cachedEntityManagers = new HashMap<>();

  // Subclasses for test injection.
  protected RealmEntityManagerFactory() {
    this.metaStoreManagerFactory = null;
  }

  public RealmEntityManagerFactory(MetaStoreManagerFactory metaStoreManagerFactory) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
  }

  public PolarisEntityManager getOrCreateEntityManager(RealmContext context) {
    String realm = context.getRealmIdentifier();

    LOGGER.debug("Looking up PolarisEntityManager for realm {}", realm);
    PolarisEntityManager entityManagerInstance = cachedEntityManagers.get(realm);
    if (entityManagerInstance == null) {
      LOGGER.info("Initializing new PolarisEntityManager for realm {}", realm);

      entityManagerInstance =
          new PolarisEntityManager(
              metaStoreManagerFactory.getOrCreateMetaStoreManager(context),
              metaStoreManagerFactory.getOrCreateSessionSupplier(context),
              metaStoreManagerFactory.getOrCreateStorageCredentialCache(context));

      cachedEntityManagers.put(realm, entityManagerInstance);
    }
    return entityManagerInstance;
  }
}
