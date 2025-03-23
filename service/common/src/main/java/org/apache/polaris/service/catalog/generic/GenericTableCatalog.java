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

import jakarta.ws.rs.core.SecurityContext;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.GenericTableEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.task.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericTableCatalog {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenericTableCatalog.class);

  private final PolarisEntityManager entityManager;
  private final CallContext callContext;
  private final PolarisResolutionManifestCatalogView resolvedEntityView;
  private final CatalogEntity catalogEntity;
  private final TaskExecutor taskExecutor;
  private final SecurityContext securityContext;
  private final String catalogName;
  private long catalogId = -1;
  private FileIOFactory fileIOFactory;
  private PolarisMetaStoreManager metaStoreManager;

  /**
   * @param entityManager provides handle to underlying PolarisMetaStoreManager with which to
   *     perform mutations on entities.
   * @param callContext the current CallContext
   * @param resolvedEntityView accessor to resolved entity paths that have been pre-vetted to ensure
   *     this catalog instance only interacts with authorized resolved paths.
   * @param taskExecutor Executor we use to register cleanup task handlers
   */
  public GenericTableCatalog(
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      CallContext callContext,
      PolarisResolutionManifestCatalogView resolvedEntityView,
      SecurityContext securityContext,
      TaskExecutor taskExecutor,
      FileIOFactory fileIOFactory) {
    this.entityManager = entityManager;
    this.callContext = callContext;
    this.resolvedEntityView = resolvedEntityView;
    this.catalogEntity =
        CatalogEntity.of(resolvedEntityView.getResolvedReferenceCatalogEntity().getRawLeafEntity());
    this.securityContext = securityContext;
    this.taskExecutor = taskExecutor;
    this.catalogId = catalogEntity.getId();
    this.catalogName = catalogEntity.getName();
    this.fileIOFactory = fileIOFactory;
    this.metaStoreManager = metaStoreManager;
  }

  public void createGenericTable(
      TableIdentifier tableIdentifier, String format, Map<String, String> properties) {
    PolarisResolvedPathWrapper resolvedParent =
        resolvedEntityView.getResolvedPath(tableIdentifier.namespace());
    if (resolvedParent == null) {
      // Illegal state because the namespace should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format(
              "Failed to fetch resolved parent for TableIdentifier '%s'", tableIdentifier));
    }

    List<PolarisEntity> catalogPath = resolvedParent.getRawFullPath();

    // TODO we need to filter by type here?
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            tableIdentifier, PolarisEntityType.GENERIC_TABLE, PolarisEntitySubType.ANY_SUBTYPE);
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
    PolarisEntity resultEntity = PolarisEntity.of(res);
    LOGGER.debug(
        "Created GenericTable entity {} with TableIdentifier {}", resultEntity, tableIdentifier);
  }

  public GenericTableEntity loadGenericTable(TableIdentifier tableIdentifier) {
    // TODO we need to filter by type here?
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            tableIdentifier, PolarisEntityType.GENERIC_TABLE, PolarisEntitySubType.ANY_SUBTYPE);
    GenericTableEntity entity =
        GenericTableEntity.of(
            resolvedEntities == null ? null : resolvedEntities.getRawLeafEntity());
    if (null == entity) {
      throw new NoSuchTableException("Table does not exist: %s", tableIdentifier);
    } else {
      return entity;
    }
  }
}
