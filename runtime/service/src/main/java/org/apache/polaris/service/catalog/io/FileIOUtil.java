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

import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.PolarisCredentialVendor;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileIOUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileIOUtil.class);

  private FileIOUtil() {}

  /**
   * Finds storage configuration information in the hierarchy of the resolved storage entity.
   *
   * <p>This method starts at the "leaf" level (e.g., table) and walks "upwards" through namespaces
   * in the hierarchy to the "root." It searches for the first entity containing storage config
   * properties, identified using a key from {@link
   * PolarisEntityConstants#getStorageConfigInfoPropertyName()}.
   *
   * @param resolvedStorageEntity the resolved entity wrapper containing the hierarchical path
   * @return an {@link Optional} containing the entity with storage config, or empty if not found
   */
  public static Optional<PolarisEntity> findStorageInfoFromHierarchy(
      PolarisResolvedPathWrapper resolvedStorageEntity) {
    Optional<PolarisEntity> storageInfoEntity =
        resolvedStorageEntity.getRawFullPath().reversed().stream()
            .filter(
                e ->
                    e.getInternalPropertiesAsMap()
                        .containsKey(PolarisEntityConstants.getStorageConfigInfoPropertyName()))
            .findFirst();
    return storageInfoEntity;
  }

  /**
   * Refreshes or generates subscoped creds for accessing table storage based on the params.
   *
   * <p>Use cases:
   *
   * <ul>
   *   <li>In {@link IcebergCatalog}, subscoped credentials are generated or refreshed when the
   *       client sends a loadTable request to vend credentials.
   *   <li>In {@link DefaultFileIOFactory}, subscoped credentials are obtained to access the storage
   *       and read/write metadata JSON files.
   * </ul>
   */
  public static StorageAccessConfig refreshAccessConfig(
      CallContext callContext,
      StorageCredentialCache storageCredentialCache,
      PolarisCredentialVendor credentialVendor,
      TableIdentifier tableIdentifier,
      Set<String> tableLocations,
      Set<PolarisStorageActions> storageActions,
      PolarisEntity entity,
      Optional<String> refreshCredentialsEndpoint) {

    boolean skipCredentialSubscopingIndirection =
        callContext
            .getRealmConfig()
            .getConfig(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION);
    if (skipCredentialSubscopingIndirection) {
      LOGGER
          .atDebug()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .log("Skipping generation of subscoped creds for table");
      return StorageAccessConfig.builder().build();
    }

    boolean allowList =
        storageActions.contains(PolarisStorageActions.LIST)
            || storageActions.contains(PolarisStorageActions.ALL);
    Set<String> writeLocations =
        storageActions.contains(PolarisStorageActions.WRITE)
                || storageActions.contains(PolarisStorageActions.DELETE)
                || storageActions.contains(PolarisStorageActions.ALL)
            ? tableLocations
            : Set.of();
    StorageAccessConfig storageAccessConfig =
        storageCredentialCache.getOrGenerateSubScopeCreds(
            credentialVendor,
            callContext.getPolarisCallContext(),
            entity,
            allowList,
            tableLocations,
            writeLocations,
            refreshCredentialsEndpoint);
    LOGGER
        .atDebug()
        .addKeyValue("tableIdentifier", tableIdentifier)
        .addKeyValue("credentialKeys", storageAccessConfig.credentials().keySet())
        .addKeyValue("extraProperties", storageAccessConfig.extraProperties())
        .log("Loaded scoped credentials for table");
    if (storageAccessConfig.credentials().isEmpty()) {
      LOGGER.debug("No credentials found for table");
    }
    return storageAccessConfig;
  }
}
