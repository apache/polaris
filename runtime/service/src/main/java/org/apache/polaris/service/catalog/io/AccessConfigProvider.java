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
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.AccessConfig;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageUtil;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory to create {@link AccessConfig} for accessing table storage based on param */
@ApplicationScoped
public class AccessConfigProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(AccessConfigProvider.class);
  private final StorageCredentialCache storageCredentialCache;
  private final PolarisMetaStoreManager metaStoreManager;

  @Inject
  public AccessConfigProvider(
      StorageCredentialCache storageCredentialCache,
      PolarisMetaStoreManager polarisMetaStoreManager) {
    this.storageCredentialCache = storageCredentialCache;
    this.metaStoreManager = polarisMetaStoreManager;
  }

  /**
   * Get access config for table storage
   *
   * @param callContext the call context
   * @param tableIdentifier the table identifier
   * @param tableMetadata the table metadata
   * @param storageActions the storage actions
   * @param refreshCredentialsEndpoint the refresh credentials endpoint
   * @param resolvedPath the resolved path wrapper containing the hierarchical path to search for
   *     storage info
   * @return the access config
   */
  public AccessConfig getAccessConfig(
      @Nonnull CallContext callContext,
      @Nonnull TableIdentifier tableIdentifier,
      @Nonnull TableMetadata tableMetadata,
      @Nonnull Set<PolarisStorageActions> storageActions,
      @Nonnull Optional<String> refreshCredentialsEndpoint,
      @Nonnull PolarisResolvedPathWrapper resolvedPath) {
    return getAccessConfig(
        callContext,
        tableIdentifier,
        StorageUtil.getLocationsUsedByTable(tableMetadata),
        storageActions,
        refreshCredentialsEndpoint,
        resolvedPath);
  }

  /**
   * Get access config for table storage
   *
   * @param callContext the call context
   * @param tableIdentifier the table identifier
   * @param tableLocations the set of table locations
   * @param storageActions the storage actions
   * @param refreshCredentialsEndpoint the refresh credentials endpoint
   * @param resolvedPath the resolved path wrapper containing the hierarchical path to search for
   *     storage info
   * @return the access config
   */
  public AccessConfig getAccessConfig(
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
      return AccessConfig.builder().build();
    }
    return FileIOUtil.refreshAccessConfig(
        callContext,
        storageCredentialCache,
        metaStoreManager,
        tableIdentifier,
        tableLocations,
        storageActions,
        storageInfo.get(),
        refreshCredentialsEndpoint);
  }
}
