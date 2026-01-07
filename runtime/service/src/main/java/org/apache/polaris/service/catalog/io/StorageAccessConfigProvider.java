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
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.rest.PolarisResourcePaths;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageCredentialsVendor;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides temporary, scoped credentials for accessing table data in object storage (S3, GCS, Azure
 * Blob Storage).
 *
 * <p>This provider decouples credential vending from catalog implementations, and should be the
 * primary entrypoint to get sub-scoped credentials for accessing table data.
 */
@RequestScoped
public class StorageAccessConfigProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageAccessConfigProvider.class);

  private final StorageCredentialCache storageCredentialCache;
  private final StorageCredentialsVendor storageCredentialsVendor;
  private final PolarisPrincipal polarisPrincipal;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;
  private final CatalogPrefixParser prefixParser;
  private final UriInfo uriInfo;

  @Inject
  public StorageAccessConfigProvider(
      StorageCredentialCache storageCredentialCache,
      StorageCredentialsVendor storageCredentialsVendor,
      PolarisPrincipal polarisPrincipal,
      PolarisStorageIntegrationProvider storageIntegrationProvider,
      CatalogPrefixParser prefixParser,
      UriInfo uriInfo) {
    this.storageCredentialCache = storageCredentialCache;
    this.storageCredentialsVendor = storageCredentialsVendor;
    this.polarisPrincipal = polarisPrincipal;
    this.storageIntegrationProvider = storageIntegrationProvider;
    this.prefixParser = prefixParser;
    this.uriInfo = uriInfo;
  }

  /**
   * Vends credentials for accessing table storage at explicit locations.
   *
   * @param tableIdentifier the table identifier, used for logging and refresh endpoint construction
   * @param tableLocations set of storage location URIs to scope credentials to
   * @param storageActions the storage operations (READ, WRITE, LIST, DELETE) to scope credentials
   *     to
   * @param refreshCredentialsEndpoint optional endpoint URL for clients to refresh credentials
   * @param resolvedPath the entity hierarchy to search for storage configuration
   * @return {@link StorageAccessConfig} with scoped credentials and metadata; empty if no storage
   *     config found
   */
  public StorageAccessConfig getStorageAccessConfigForCredentialsVending(
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
      return StorageAccessConfig.EMPTY;
    }
    PolarisEntity storageInfoEntity = storageInfo.get();

    boolean skipCredentialSubscopingIndirection =
        storageCredentialsVendor
            .getRealmConfig()
            .getConfig(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION);
    if (skipCredentialSubscopingIndirection) {
      LOGGER
          .atDebug()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .log("Skipping generation of subscoped creds for table");
      return StorageAccessConfig.EMPTY;
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
    StorageAccessConfig accessConfig =
        storageCredentialCache.getOrGenerateSubScopeCreds(
            storageCredentialsVendor,
            storageInfoEntity,
            allowList,
            tableLocations,
            writeLocations,
            polarisPrincipal,
            refreshCredentialsEndpoint);

    LOGGER
        .atDebug()
        .addKeyValue("tableIdentifier", tableIdentifier)
        .addKeyValue("credentialKeys", accessConfig.credentials().keySet())
        .addKeyValue("extraProperties", accessConfig.extraProperties())
        .log("Loaded scoped credentials for table");
    if (accessConfig.credentials().isEmpty()) {
      LOGGER.debug("No credentials found for table");
    }
    return accessConfig;
  }

  /**
   * Generates a remote signing configuration for accessing table storage at explicit locations.
   *
   * @param catalogName the name of the catalog
   * @param tableIdentifier the table identifier, used for logging and refresh endpoint construction
   * @param resolvedPath the entity hierarchy to search for storage configuration
   * @return {@link StorageAccessConfig} with scoped credentials and metadata; empty if no storage
   *     config found
   */
  public StorageAccessConfig getStorageAccessConfigForRemoteSigning(
      @Nonnull String catalogName,
      @Nonnull TableIdentifier tableIdentifier,
      @Nonnull PolarisResolvedPathWrapper resolvedPath) {
    LOGGER
        .atDebug()
        .addKeyValue("tableIdentifier", tableIdentifier)
        .log("Fetching remote signing config for table");

    Optional<PolarisEntity> storageInfo = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);
    Optional<PolarisStorageConfigurationInfo> configurationInfo =
        storageInfo
            .map(PolarisEntity::getInternalPropertiesAsMap)
            .map(info -> info.get(PolarisEntityConstants.getStorageConfigInfoPropertyName()))
            .map(PolarisStorageConfigurationInfo::deserialize);

    if (configurationInfo.isEmpty()) {
      LOGGER
          .atWarn()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .log("Table entity has no storage configuration in its hierarchy");
      return StorageAccessConfig.EMPTY;
    }

    PolarisStorageIntegration<AwsStorageConfigurationInfo> storageIntegration =
        storageIntegrationProvider.getStorageIntegrationForConfig(configurationInfo.get());

    if (!(storageIntegration
        instanceof AwsCredentialsStorageIntegration awsCredentialsStorageIntegration)) {
      LOGGER
          .atWarn()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .log("Table entity storage integration is not an AWS credentials storage integration");
      return StorageAccessConfig.EMPTY;
    }

    String prefix = prefixParser.catalogNameToPrefix(catalogName);
    // TODO M2 handle cases where the catalog server is behind a proxy
    URI signerUri = uriInfo.getBaseUriBuilder().path(PolarisResourcePaths.API_PATH_SEGMENT).build();
    String signerEndpoint = new PolarisResourcePaths(prefix).s3RemoteSigning(tableIdentifier);

    return awsCredentialsStorageIntegration.getRemoteSigningAccessConfig(signerUri, signerEndpoint);
  }
}
