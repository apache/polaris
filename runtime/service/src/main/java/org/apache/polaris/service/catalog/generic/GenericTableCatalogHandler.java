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
package org.apache.polaris.service.catalog.generic;

import static org.apache.polaris.service.catalog.AccessDelegationMode.VENDED_CREDENTIALS;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.inject.Instance;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.catalog.FederatedCatalogFactory;
import org.apache.polaris.core.catalog.GenericTableCatalog;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.service.catalog.AccessDelegationMode;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.catalog.common.CatalogUtils;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.types.GenericTable;
import org.apache.polaris.service.types.ListGenericTablesResponse;
import org.apache.polaris.service.types.LoadGenericTableResponse;
import org.apache.polaris.service.types.StorageAccessConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PolarisImmutable
@SuppressWarnings("immutables:incompat")
public abstract class GenericTableCatalogHandler extends CatalogHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenericTableCatalogHandler.class);

  protected abstract PolarisCredentialManager credentialManager();

  protected abstract Instance<FederatedCatalogFactory> federatedCatalogFactories();

  protected abstract StorageAccessConfigProvider storageAccessConfigProvider();

  private GenericTableCatalog genericTableCatalog;

  @Override
  protected void initializeCatalog() {
    CatalogEntity resolvedCatalogEntity = resolutionManifest.getResolvedCatalogEntity();
    ConnectionConfigInfoDpo connectionConfigInfoDpo =
        resolvedCatalogEntity.getConnectionConfigInfoDpo();
    if (connectionConfigInfoDpo != null) {
      LOGGER
          .atInfo()
          .addKeyValue("remoteUrl", connectionConfigInfoDpo.getUri())
          .log("Initializing federated catalog");
      FeatureConfiguration.enforceFeatureEnabledOrThrow(
          realmConfig(), FeatureConfiguration.ENABLE_CATALOG_FEDERATION);

      GenericTableCatalog federatedCatalog;
      ConnectionType connectionType =
          ConnectionType.fromCode(connectionConfigInfoDpo.getConnectionTypeCode());

      // Use the unified factory pattern for all federated catalog types
      Instance<FederatedCatalogFactory> federatedCatalogFactory =
          federatedCatalogFactories()
              .select(Identifier.Literal.of(connectionType.getFactoryIdentifier()));
      if (federatedCatalogFactory.isResolvable()) {
        // Pass through catalog properties (e.g., rest.client.proxy.*, timeout settings)
        Map<String, String> catalogProperties = resolvedCatalogEntity.getPropertiesAsMap();
        federatedCatalog =
            federatedCatalogFactory
                .get()
                .createGenericCatalog(
                    connectionConfigInfoDpo, credentialManager(), catalogProperties);
      } else {
        throw new UnsupportedOperationException(
            "External catalog factory for type '" + connectionType + "' is unavailable.");
      }
      this.genericTableCatalog = federatedCatalog;
    } else {
      LOGGER.atInfo().log("Initializing non-federated catalog");
      this.genericTableCatalog =
          new PolarisGenericTableCatalog(
              metaStoreManager(), callContext(), this.resolutionManifest);
      this.genericTableCatalog.initialize(catalogName(), Map.of());
    }
  }

  public ListGenericTablesResponse listGenericTables(Namespace parent) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_TABLES;
    authorizeBasicNamespaceOperationOrThrow(op, parent);

    return ListGenericTablesResponse.builder()
        .setIdentifiers(new LinkedHashSet<>(genericTableCatalog.listGenericTables(parent)))
        .build();
  }

  public LoadGenericTableResponse createGenericTable(
      TableIdentifier identifier,
      String format,
      String baseLocation,
      String doc,
      Map<String, String> properties,
      EnumSet<AccessDelegationMode> delegationModes) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_TABLE_DIRECT;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, identifier);

    GenericTableEntity createdEntity =
        this.genericTableCatalog.createGenericTable(
            identifier, format, baseLocation, doc, properties);
    GenericTable createdTable =
        GenericTable.builder()
            .setName(createdEntity.getName())
            .setFormat(createdEntity.getFormat())
            .setBaseLocation(createdEntity.getBaseLocation())
            .setDoc(createdEntity.getDoc())
            .setProperties(createdEntity.getPropertiesAsMap())
            .build();

    List<StorageAccessConfig> storageAccessConfigs =
        shouldVendCredentials(delegationModes)
            ? vendCredentials(
                identifier,
                createdEntity,
                Set.of(
                    PolarisStorageActions.READ,
                    PolarisStorageActions.WRITE,
                    PolarisStorageActions.LIST))
            : List.of();

    return LoadGenericTableResponse.builder()
        .setTable(createdTable)
        .setStorageAccessConfigs(storageAccessConfigs)
        .build();
  }

  public boolean dropGenericTable(TableIdentifier identifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITHOUT_PURGE;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, identifier);

    return this.genericTableCatalog.dropGenericTable(identifier);
  }

  public LoadGenericTableResponse loadGenericTable(
      TableIdentifier identifier, EnumSet<AccessDelegationMode> delegationModes) {
    ensureResolutionManifestForTable(identifier);
    boolean credentialVendingRequested = shouldVendCredentials(delegationModes);

    Set<PolarisStorageActions> actionsRequested =
        authorizeLoadTableLike(
            identifier, PolarisEntitySubType.GENERIC_TABLE, credentialVendingRequested);

    GenericTableEntity loadedEntity = this.genericTableCatalog.loadGenericTable(identifier);
    GenericTable loadedTable =
        GenericTable.builder()
            .setName(loadedEntity.getName())
            .setFormat(loadedEntity.getFormat())
            .setBaseLocation(loadedEntity.getBaseLocation())
            .setDoc(loadedEntity.getDoc())
            .setProperties(loadedEntity.getPropertiesAsMap())
            .build();

    List<StorageAccessConfig> storageAccessConfigs =
        credentialVendingRequested
            ? vendCredentials(identifier, loadedEntity, actionsRequested)
            : List.of();

    return LoadGenericTableResponse.builder()
        .setTable(loadedTable)
        .setStorageAccessConfigs(storageAccessConfigs)
        .build();
  }

  private boolean shouldVendCredentials(EnumSet<AccessDelegationMode> delegationModes) {
    return delegationModes.contains(VENDED_CREDENTIALS)
        && realmConfig()
            .getConfig(
                FeatureConfiguration.ENABLE_GENERIC_TABLES_CREDENTIAL_VENDING,
                resolutionManifest.getResolvedCatalogEntity());
  }

  private List<StorageAccessConfig> vendCredentials(
      TableIdentifier tableIdentifier,
      GenericTableEntity entity,
      Set<PolarisStorageActions> actions) {
    String baseLocation = entity.getBaseLocation();
    if (baseLocation == null || baseLocation.isEmpty()) {
      LOGGER.debug(
          "No base location set for generic table {}, skipping credential vending",
          tableIdentifier);
      return List.of();
    }

    PolarisResolvedPathWrapper resolvedStoragePath =
        CatalogUtils.findResolvedStorageEntity(
            resolutionManifest, tableIdentifier, PolarisEntitySubType.GENERIC_TABLE);
    if (resolvedStoragePath == null) {
      LOGGER.debug(
          "Unable to find storage configuration information for generic table {}", tableIdentifier);
      return List.of();
    }

    Set<String> tableLocations = Set.of(baseLocation);
    org.apache.polaris.core.storage.StorageAccessConfig storageAccessConfig =
        storageAccessConfigProvider()
            .getStorageAccessConfig(
                tableIdentifier, tableLocations, actions, Optional.empty(), resolvedStoragePath);

    Map<String, String> credentials = storageAccessConfig.credentials();
    if (credentials.isEmpty()) {
      LOGGER.debug("No credentials vended for generic table {}", tableIdentifier);
      return List.of();
    }

    Map<String, String> allConfig = new java.util.HashMap<>(credentials);
    allConfig.putAll(storageAccessConfig.extraProperties());

    return List.of(
        StorageAccessConfig.builder().setPrefix(baseLocation).setConfig(allConfig).build());
  }
}
