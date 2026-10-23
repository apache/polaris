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
package org.apache.polaris.service.catalog.directory;

import static java.util.Objects.requireNonNull;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.alreadyExistsExceptionForTableLikeEntity;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.noSuchNamespaceException;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.notFoundExceptionForTableLikeEntity;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.catalog.DirectoryCatalog;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.table.DirectoryEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolarisDirectoryCatalog implements DirectoryCatalog {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisDirectoryCatalog.class);

  private final CallContext callContext;
  private final PolarisResolutionManifestCatalogView resolvedEntityView;
  private final long catalogId;
  private final PolarisMetaStoreManager metaStoreManager;

  public PolarisDirectoryCatalog(
      PolarisMetaStoreManager metaStoreManager,
      CallContext callContext,
      PolarisResolutionManifestCatalogView resolvedEntityView) {
    this.callContext = callContext;
    this.resolvedEntityView = requireNonNull(resolvedEntityView, "No resolved entity view");
    this.catalogId =
        requireNonNull(resolvedEntityView.getResolvedCatalogEntity(), "No resolved catalog entity")
            .getId();
    this.metaStoreManager = metaStoreManager;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    if (!properties.isEmpty()) {
      throw new IllegalStateException("PolarisDirectoryCatalog does not support properties");
    }
  }

  @Override
  public DirectoryEntity createDirectory(
      TableIdentifier tableIdentifier,
      String baseLocation,
      String filterInclude,
      String filterExclude,
      String scanSchedule,
      Map<String, String> properties) {
    PolarisResolvedPathWrapper resolvedParent =
        resolvedEntityView.getResolvedPath(
            ResolvedPathKey.ofNamespace(tableIdentifier.namespace()));
    if (resolvedParent == null) {
      throw new IllegalStateException(
          String.format(
              "Failed to fetch resolved parent for TableIdentifier '%s'", tableIdentifier));
    }

    List<PolarisEntity> catalogPath = resolvedParent.getRawFullPath();

    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            ResolvedPathKey.ofTableLike(tableIdentifier), PolarisEntitySubType.ANY_SUBTYPE);
    PolarisEntity entity = resolvedEntities == null ? null : resolvedEntities.getRawLeafEntity();
    if (null == entity) {
      entity =
          new DirectoryEntity.Builder(tableIdentifier, baseLocation)
              .setCatalogId(this.catalogId)
              .setParentNamespace(tableIdentifier.namespace())
              .setParentId(resolvedParent.getRawLeafEntity().getId())
              .setId(
                  this.metaStoreManager
                      .generateNewEntityId(this.callContext.getPolarisCallContext())
                      .getId())
              .setProperties(properties)
              .setFilterInclude(filterInclude)
              .setFilterExclude(filterExclude)
              .setScanSchedule(scanSchedule)
              .setCreateTimestamp(System.currentTimeMillis())
              .build();
    } else {
      throw alreadyExistsExceptionForTableLikeEntity(tableIdentifier, entity.getSubType());
    }

    EntityResult res =
        this.metaStoreManager.createEntityIfNotExists(
            this.callContext.getPolarisCallContext(),
            PolarisEntity.toCoreList(catalogPath),
            entity);
    if (!res.isSuccess()) {
      if (requireNonNull(res.getReturnStatus()) == BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS) {
        throw alreadyExistsExceptionForTableLikeEntity(
            tableIdentifier, res.getAlreadyExistsEntitySubType());
      }
      throw new IllegalStateException(
          String.format(
              "Unknown error status for identifier %s: %s with extraInfo: %s",
              tableIdentifier, res.getReturnStatus(), res.getExtraInformation()));
    }
    DirectoryEntity resultEntity = DirectoryEntity.of(res.getEntity());
    LOGGER.debug(
        "Created Directory entity {} with TableIdentifier {}", resultEntity, tableIdentifier);
    return resultEntity;
  }

  @Override
  public DirectoryEntity loadDirectory(TableIdentifier tableIdentifier) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            ResolvedPathKey.ofTableLike(tableIdentifier), PolarisEntitySubType.DIRECTORY);
    DirectoryEntity entity =
        DirectoryEntity.of(resolvedEntities == null ? null : resolvedEntities.getRawLeafEntity());
    if (null == entity) {
      throw notFoundExceptionForTableLikeEntity(tableIdentifier, PolarisEntitySubType.DIRECTORY);
    } else {
      return entity;
    }
  }

  @Override
  public boolean dropDirectory(TableIdentifier tableIdentifier) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            ResolvedPathKey.ofTableLike(tableIdentifier), PolarisEntitySubType.DIRECTORY);

    if (resolvedEntities == null) {
      throw notFoundExceptionForTableLikeEntity(tableIdentifier, PolarisEntitySubType.DIRECTORY);
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
  public List<TableIdentifier> listDirectories(Namespace namespace) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(namespace));
    if (resolvedEntities == null) {
      throw noSuchNamespaceException(namespace);
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawFullPath();
    List<PolarisEntity.NameAndId> entities =
        PolarisEntity.toNameAndIdList(
            this.metaStoreManager
                .listEntities(
                    this.callContext.getPolarisCallContext(),
                    PolarisEntity.toCoreList(catalogPath),
                    PolarisEntityType.TABLE_LIKE,
                    PolarisEntitySubType.DIRECTORY,
                    PageToken.readEverything())
                .getEntities());
    return PolarisCatalogHelpers.nameAndIdToTableIdentifiers(catalogPath, entities);
  }
}
