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

package org.apache.polaris.service.catalog.credentials;

import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.storage.AccessConfig;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageUtil;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.catalog.iceberg.IcebergStorageUtils;
import org.apache.polaris.service.catalog.io.FileIOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultCredentialVendor implements SupportsCredentialDelegation {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCredentialVendor.class);

  private final CallContext callContext;
  private final PolarisResolutionManifestCatalogView resolvedEntityView;
  private final StorageCredentialCache storageCredentialCache;
  private final PolarisMetaStoreManager metaStoreManager;

  public DefaultCredentialVendor(
      CallContext callContext,
      PolarisResolutionManifestCatalogView resolvedEntityView,
      StorageCredentialCache storageCredentialCache,
      PolarisMetaStoreManager metaStoreManager) {
    this.callContext = callContext;
    this.resolvedEntityView = resolvedEntityView;
    this.storageCredentialCache = storageCredentialCache;
    this.metaStoreManager = metaStoreManager;
  }

  @Override
  public AccessConfig getAccessConfig(
      TableIdentifier tableIdentifier,
      TableMetadata tableMetadata,
      Set<PolarisStorageActions> storageActions,
      Optional<String> refreshCredentialsEndpoint) {
    LOGGER
        .atDebug()
        .addKeyValue("tableIdentifier", tableIdentifier)
        .addKeyValue("tableLocation", tableMetadata.location())
        .log("Fetching client credentials for table");
    Optional<PolarisEntity> storageInfo =
        IcebergStorageUtils.findStorageInfo(resolvedEntityView, tableIdentifier);
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
        StorageUtil.getLocationsUsedByTable(tableMetadata),
        storageActions,
        storageInfo.get(),
        refreshCredentialsEndpoint);
  }
}
