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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.core.SecurityContext;
import java.util.LinkedHashSet;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.catalog.GenericTableCatalog;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.types.GenericTable;
import org.apache.polaris.service.types.ListGenericTablesResponse;
import org.apache.polaris.service.types.LoadGenericTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericTableCatalogHandler extends CatalogHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenericTableCatalogHandler.class);

  private PolarisMetaStoreManager metaStoreManager;

  private GenericTableCatalog genericTableCatalog;

  public GenericTableCatalogHandler(
      PolarisDiagnostics diagnostics,
      CallContext callContext,
      ResolutionManifestFactory resolutionManifestFactory,
      PolarisMetaStoreManager metaStoreManager,
      SecurityContext securityContext,
      String catalogName,
      PolarisAuthorizer authorizer,
      UserSecretsManager userSecretsManager,
      Instance<ExternalCatalogFactory> externalCatalogFactories) {
    super(
        diagnostics,
        callContext,
        resolutionManifestFactory,
        securityContext,
        catalogName,
        authorizer,
        userSecretsManager,
        externalCatalogFactories);
    this.metaStoreManager = metaStoreManager;
  }

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
          callContext.getRealmConfig(), FeatureConfiguration.ENABLE_CATALOG_FEDERATION);

      GenericTableCatalog federatedCatalog;
      ConnectionType connectionType =
          ConnectionType.fromCode(connectionConfigInfoDpo.getConnectionTypeCode());

      // Use the unified factory pattern for all external catalog types
      Instance<ExternalCatalogFactory> externalCatalogFactory =
          externalCatalogFactories.select(
              Identifier.Literal.of(connectionType.getFactoryIdentifier()));
      if (externalCatalogFactory.isResolvable()) {
        federatedCatalog =
            externalCatalogFactory
                .get()
                .createGenericCatalog(connectionConfigInfoDpo, getUserSecretsManager());
      } else {
        throw new UnsupportedOperationException(
            "External catalog factory for type '" + connectionType + "' is unavailable.");
      }
      this.genericTableCatalog = federatedCatalog;
    } else {
      LOGGER.atInfo().log("Initializing non-federated catalog");
      this.genericTableCatalog =
          new PolarisGenericTableCatalog(metaStoreManager, callContext, this.resolutionManifest);
      this.genericTableCatalog.initialize(catalogName, Map.of());
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
      Map<String, String> properties) {
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

    return LoadGenericTableResponse.builder().setTable(createdTable).build();
  }

  public boolean dropGenericTable(TableIdentifier identifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITHOUT_PURGE;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, identifier);

    return this.genericTableCatalog.dropGenericTable(identifier);
  }

  public LoadGenericTableResponse loadGenericTable(TableIdentifier identifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_TABLE;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.GENERIC_TABLE, identifier);

    GenericTableEntity loadedEntity = this.genericTableCatalog.loadGenericTable(identifier);
    GenericTable loadedTable =
        GenericTable.builder()
            .setName(loadedEntity.getName())
            .setFormat(loadedEntity.getFormat())
            .setBaseLocation(loadedEntity.getBaseLocation())
            .setDoc(loadedEntity.getDoc())
            .setProperties(loadedEntity.getPropertiesAsMap())
            .build();

    return LoadGenericTableResponse.builder().setTable(loadedTable).build();
  }
}
