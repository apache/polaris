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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolarisGenericTableCatalog implements GenericTableCatalog {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisGenericTableCatalog.class);

  private String name;

  private final CallContext callContext;
  private final PolarisResolutionManifestCatalogView resolvedEntityView;
  private final CatalogEntity catalogEntity;
  private long catalogId = -1;
  private PolarisMetaStoreManager metaStoreManager;

  public PolarisGenericTableCatalog(
      PolarisMetaStoreManager metaStoreManager,
      CallContext callContext,
      PolarisResolutionManifestCatalogView resolvedEntityView) {
    this.callContext = callContext;
    this.resolvedEntityView = resolvedEntityView;
    this.catalogEntity =
        CatalogEntity.of(resolvedEntityView.getResolvedReferenceCatalogEntity().getRawLeafEntity());
    this.catalogId = catalogEntity.getId();
    this.metaStoreManager = metaStoreManager;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.name = name;
    if (!properties.isEmpty()) {
      throw new IllegalStateException("PolarisGenericTableCatalog does not support properties");
    }
  }

  @Override
  public GenericTableEntity createGenericTable(
      TableIdentifier tableIdentifier,
      String format,
      String baseLocation,
      String doc,
      Map<String, String> properties) {
    PolarisResolvedPathWrapper resolvedParent =
        resolvedEntityView.getResolvedPath(tableIdentifier.namespace());
    if (resolvedParent == null) {
      // Illegal state because the namespace should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format(
              "Failed to fetch resolved parent for TableIdentifier '%s'", tableIdentifier));
    }

    List<PolarisEntity> catalogPath = resolvedParent.getRawFullPath();

    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            tableIdentifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ANY_SUBTYPE);
    GenericTableEntity entity =
        GenericTableEntity.of(
            resolvedEntities == null ? null : resolvedEntities.getRawLeafEntity());
    if (null == entity) {
      entity =
          new GenericTableEntity.Builder(tableIdentifier, format)
              .setCatalogId(this.catalogId)
              .setParentNamespace(tableIdentifier.namespace())
              .setParentId(resolvedParent.getRawLeafEntity().getId())
              .setId(
                  this.metaStoreManager
                      .generateNewEntityId(this.callContext.getPolarisCallContext())
                      .getId())
              .setProperties(properties)
              .setDoc(doc)
              .setBaseLocation(baseLocation)
              .setCreateTimestamp(System.currentTimeMillis())
              .build();
    } else {
      throw new AlreadyExistsException(
          "Iceberg table, view, or generic table already exists: %s", tableIdentifier);
    }

    EntityResult res =
        this.metaStoreManager.createEntityIfNotExists(
            this.callContext.getPolarisCallContext(),
            PolarisEntity.toCoreList(catalogPath),
            entity);
    if (!res.isSuccess()) {
      switch (res.getReturnStatus()) {
        case BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS:
          throw new AlreadyExistsException(
              "Iceberg table, view, or generic table already exists: %s", tableIdentifier);

        default:
          throw new IllegalStateException(
              String.format(
                  "Unknown error status for identifier %s: %s with extraInfo: %s",
                  tableIdentifier, res.getReturnStatus(), res.getExtraInformation()));
      }
    }
    GenericTableEntity resultEntity = GenericTableEntity.of(res.getEntity());
    LOGGER.debug(
        "Created GenericTable entity {} with TableIdentifier {}", resultEntity, tableIdentifier);
    return resultEntity;
  }

  @Override
  public GenericTableEntity loadGenericTable(TableIdentifier tableIdentifier) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            tableIdentifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.GENERIC_TABLE);
    GenericTableEntity entity =
        GenericTableEntity.of(
            resolvedEntities == null ? null : resolvedEntities.getRawLeafEntity());
    if (null == entity) {
      throw new NoSuchTableException("Generic table does not exist: %s", tableIdentifier);
    } else {
      return entity;
    }
  }

  @Override
  public boolean dropGenericTable(TableIdentifier tableIdentifier) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            tableIdentifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.GENERIC_TABLE);

    if (resolvedEntities == null) {
      throw new NoSuchTableException("Generic table does not exist: %s", tableIdentifier);
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawParentPath();
    PolarisEntity leafEntity = resolvedEntities.getRawLeafEntity();

    DropEntityResult dropEntityResult =
        this.metaStoreManager.dropEntityIfExists(
            this.callContext.getPolarisCallContext(),
            PolarisEntity.toCoreList(catalogPath),
            leafEntity,
            Map.of(),
            false);

    return dropEntityResult.isSuccess();
  }

  @Override
  public List<TableIdentifier> listGenericTables(Namespace namespace) {
    PolarisResolvedPathWrapper resolvedEntities = resolvedEntityView.getResolvedPath(namespace);
    if (resolvedEntities == null) {
      throw new NoSuchNamespaceException("Namespace '%s' does not exist", namespace);
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawFullPath();
    List<PolarisEntity.NameAndId> entities =
        PolarisEntity.toNameAndIdList(
            this.metaStoreManager
                .listEntities(
                    this.callContext.getPolarisCallContext(),
                    PolarisEntity.toCoreList(catalogPath),
                    PolarisEntityType.TABLE_LIKE,
                    PolarisEntitySubType.GENERIC_TABLE,
                    PageToken.readEverything())
                .getEntities());
    return PolarisCatalogHelpers.nameAndIdToTableIdentifiers(catalogPath, entities);
  }
}
