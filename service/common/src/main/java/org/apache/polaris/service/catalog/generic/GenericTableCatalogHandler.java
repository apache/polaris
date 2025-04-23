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

import static org.apache.polaris.service.catalog.conversion.xtable.XTableConvertorConfigurations.TARGET_FORMAT_METADATA_PATH_KEY;

import jakarta.ws.rs.core.SecurityContext;
import java.util.LinkedHashSet;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.catalog.conversion.xtable.RunSyncResponse;
import org.apache.polaris.service.catalog.conversion.xtable.XTableConversionUtils;
import org.apache.polaris.service.catalog.conversion.xtable.XTableConverter;
import org.apache.polaris.service.types.GenericTable;
import org.apache.polaris.service.types.ListGenericTablesResponse;
import org.apache.polaris.service.types.LoadGenericTableResponse;

public class GenericTableCatalogHandler extends CatalogHandler {

  private PolarisMetaStoreManager metaStoreManager;

  private GenericTableCatalog genericTableCatalog;

  public GenericTableCatalogHandler(
      CallContext callContext,
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      SecurityContext securityContext,
      String catalogName,
      PolarisAuthorizer authorizer) {
    super(callContext, entityManager, securityContext, catalogName, authorizer);
    this.metaStoreManager = metaStoreManager;
  }

  public void enforceGenericTablesEnabledOrThrow() {
    boolean enabled =
        callContext
            .getPolarisCallContext()
            .getConfigurationStore()
            .getConfiguration(
                callContext.getPolarisCallContext(), FeatureConfiguration.ENABLE_GENERIC_TABLES);
    if (!enabled) {
      throw new UnsupportedOperationException("Generic table support is not enabled");
    }
  }

  @Override
  protected void initializeCatalog() {
    enforceGenericTablesEnabledOrThrow();
    this.genericTableCatalog =
        new GenericTableCatalog(metaStoreManager, callContext, this.resolutionManifest);
    initializeConversionServiceIfEnabled();
  }

  public ListGenericTablesResponse listGenericTables(Namespace parent) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_TABLES;
    authorizeBasicNamespaceOperationOrThrow(op, parent);

    return ListGenericTablesResponse.builder()
        .setIdentifiers(new LinkedHashSet<>(genericTableCatalog.listGenericTables(parent)))
        .build();
  }

  public LoadGenericTableResponse createGenericTable(
      TableIdentifier identifier, String format, String doc, Map<String, String> properties) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_TABLE_DIRECT;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, identifier);

    GenericTableEntity createdEntity =
        this.genericTableCatalog.createGenericTable(identifier, format, doc, properties);
    GenericTable createdTable =
        new GenericTable(
            createdEntity.getName(),
            createdEntity.getFormat(),
            createdEntity.getDoc(),
            createdEntity.getPropertiesAsMap());
    convertIfRequired(createdEntity, createdTable);
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
        new GenericTable(
            loadedEntity.getName(),
            loadedEntity.getFormat(),
            loadedEntity.getDoc(),
            loadedEntity.getPropertiesAsMap());

    convertIfRequired(loadedEntity, loadedTable);
    return LoadGenericTableResponse.builder().setTable(loadedTable).build();
  }

  private void convertIfRequired(GenericTableEntity entity, GenericTable table) {
    if (XTableConversionUtils.requiresConversion(callContext, table.getProperties())) {
      RunSyncResponse response = XTableConverter.getInstance().execute(entity);
      table.getProperties().put(TARGET_FORMAT_METADATA_PATH_KEY, response.getTargetMetadataPath());
    }
  }
}
