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

package org.apache.polaris.service.catalog.io;

import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides temporary, scoped credentials for accessing table data in object storage (S3, GCS, Azure
 * Blob Storage).
 *
 * <p>This provider decouples credential vending from catalog implementations, and should be the
 * primary entrypoint to get sub-scoped credentials for accessing table data.
 */
@ApplicationScoped
public class StorageAccessConfigProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageAccessConfigProvider.class);

  private final StorageCredentialCache storageCredentialCache;
  private final MetaStoreManagerFactory metaStoreManagerFactory;

  @Inject
  public StorageAccessConfigProvider(
      StorageCredentialCache storageCredentialCache,
      MetaStoreManagerFactory metaStoreManagerFactory) {
    this.storageCredentialCache = storageCredentialCache;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
  }

  /**
   * Vends credentials for accessing table storage at explicit locations.
   *
   * @param callContext the call context containing realm, principal, and security context
   * @param tableIdentifier the table identifier, used for logging and refresh endpoint construction
   * @param tableLocations set of storage location URIs to scope credentials to
   * @param storageActions the storage operations (READ, WRITE, LIST, DELETE) to scope credentials
   *     to
   * @param refreshCredentialsEndpoint optional endpoint URL for clients to refresh credentials
   * @param resolvedPath the entity hierarchy to search for storage configuration
   * @return {@link StorageAccessConfig} with scoped credentials and metadata; empty if no storage
   *     config found
   */
  public StorageAccessConfig getStorageAccessConfig(
      @Nonnull CallContext callContext,
      @Nonnull TableIdentifier tableIdentifier,
      @Nonnull Set<String> tableLocations,
      @Nonnull Set<PolarisStorageActions> storageActions,
      @Nonnull Optional<String> refreshCredentialsEndpoint,
      @Nonnull PolarisResolvedPathWrapper resolvedPath) {
    LOGGER
        .atDebug()
        .addKeyValue("tableIdentifier", tableIdentifier)
        .addKeyValue("tableLocation", tableLocations)
        .log("Fetching client credentials for table");
    Optional<PolarisEntity> storageInfo = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);
    if (storageInfo.isEmpty()) {
      LOGGER
          .atWarn()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .log("Table entity has no storage configuration in its hierarchy");
      return StorageAccessConfig.builder().supportsCredentialVending(false).build();
    }
    return FileIOUtil.refreshAccessConfig(
        callContext,
        storageCredentialCache,
        metaStoreManagerFactory.getOrCreateMetaStoreManager(callContext.getRealmContext()),
        tableIdentifier,
        tableLocations,
        storageActions,
        storageInfo.get(),
        refreshCredentialsEndpoint);
  }
}
