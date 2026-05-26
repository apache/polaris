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

import static java.util.Objects.requireNonNull;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.alreadyExistsExceptionForTableLikeEntity;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.noSuchNamespaceException;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.notFoundExceptionForTableLikeEntity;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.catalog.GenericTableCatalog;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisEntityUtils;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.service.catalog.common.CatalogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolarisGenericTableCatalog implements GenericTableCatalog {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisGenericTableCatalog.class);

  private final CallContext callContext;
  private final PolarisResolutionManifestCatalogView resolvedEntityView;
  private final long catalogId;
  private final PolarisMetaStoreManager metaStoreManager;

  public PolarisGenericTableCatalog(
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
        resolvedEntityView.getResolvedPath(
            ResolvedPathKey.ofNamespace(tableIdentifier.namespace()));
    if (resolvedParent == null) {
      // Illegal state because the namespace should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format(
              "Failed to fetch resolved parent for TableIdentifier '%s'", tableIdentifier));
    }

    if (baseLocation != null && !baseLocation.isEmpty()) {
      CatalogUtils.validateLocationForTableLike(
          resolvedEntityView, callContext.getRealmConfig(), tableIdentifier, baseLocation);
      validateNoLocationOverlap(tableIdentifier, baseLocation, resolvedParent.getRawFullPath());
    }

    List<PolarisEntity> catalogPath = resolvedParent.getRawFullPath();

    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            ResolvedPathKey.ofTableLike(tableIdentifier), PolarisEntitySubType.ANY_SUBTYPE);
    PolarisEntity entity = resolvedEntities == null ? null : resolvedEntities.getRawLeafEntity();
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
    GenericTableEntity resultEntity = GenericTableEntity.of(res.getEntity());
    LOGGER.debug(
        "Created GenericTable entity {} with TableIdentifier {}", resultEntity, tableIdentifier);
    return resultEntity;
  }

  @Override
  public GenericTableEntity loadGenericTable(TableIdentifier tableIdentifier) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            ResolvedPathKey.ofTableLike(tableIdentifier), PolarisEntitySubType.GENERIC_TABLE);
    GenericTableEntity entity =
        GenericTableEntity.of(
            resolvedEntities == null ? null : resolvedEntities.getRawLeafEntity());
    if (null == entity) {
      throw notFoundExceptionForTableLikeEntity(
          tableIdentifier, PolarisEntitySubType.GENERIC_TABLE);
    } else {
      return entity;
    }
  }

  @Override
  public boolean dropGenericTable(TableIdentifier tableIdentifier) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            ResolvedPathKey.ofTableLike(tableIdentifier), PolarisEntitySubType.GENERIC_TABLE);

    if (resolvedEntities == null) {
      throw notFoundExceptionForTableLikeEntity(
          tableIdentifier, PolarisEntitySubType.GENERIC_TABLE);
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
                    PolarisEntitySubType.GENERIC_TABLE,
                    PageToken.readEverything())
                .getEntities());
    return PolarisCatalogHelpers.nameAndIdToTableIdentifiers(catalogPath, entities);
  }

  private void validateNoLocationOverlap(
      TableIdentifier identifier, String location, List<PolarisEntity> parentPath) {
    RealmConfig realmConfig = callContext.getRealmConfig();
    CatalogEntity catalogEntity = resolvedEntityView.getResolvedCatalogEntity();
    if (catalogEntity != null
        && realmConfig.getConfig(
            FeatureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP, catalogEntity)) {
      LOGGER.debug("Skipping location overlap validation for identifier '{}'", identifier);
      return;
    }

    boolean useOptimizedSiblingCheck =
        realmConfig.getConfig(FeatureConfiguration.OPTIMIZED_SIBLING_CHECK);
    if (useOptimizedSiblingCheck) {
      GenericTableEntity virtualEntity =
          new GenericTableEntity.Builder(identifier, "")
              .setCatalogId(catalogId)
              .setParentId(parentPath.getLast().getId())
              .setBaseLocation(location)
              .build();
      Optional<Optional<String>> result =
          metaStoreManager.hasOverlappingSiblings(
              callContext.getPolarisCallContext(), virtualEntity);
      if (result.isPresent()) {
        if (result.get().isPresent()) {
          throw new ForbiddenException(
              "Unable to create table at location '%s' because it conflicts with "
                  + "existing table or namespace at %s",
              location, result.get().get());
        }
        return;
      }
    }

    StorageLocation targetLocation = StorageLocation.of(location);
    List<PolarisEntityCore> coreParentPath = PolarisEntity.toCoreList(parentPath);

    ListEntitiesResult siblingTablesResult =
        metaStoreManager.listEntities(
            callContext.getPolarisCallContext(),
            coreParentPath,
            PolarisEntityType.TABLE_LIKE,
            PolarisEntitySubType.ANY_SUBTYPE,
            PageToken.readEverything());
    if (siblingTablesResult.isSuccess() && siblingTablesResult.getEntities() != null) {
      for (EntityNameLookupRecord sibling : siblingTablesResult.getEntities()) {
        if (sibling.getName().equals(identifier.name())) {
          continue;
        }
        checkEntityLocationOverlap(sibling, targetLocation, location);
      }
    }

    ListEntitiesResult siblingNamespacesResult =
        metaStoreManager.listEntities(
            callContext.getPolarisCallContext(),
            coreParentPath,
            PolarisEntityType.NAMESPACE,
            PolarisEntitySubType.ANY_SUBTYPE,
            PageToken.readEverything());
    if (siblingNamespacesResult.isSuccess() && siblingNamespacesResult.getEntities() != null) {
      for (EntityNameLookupRecord sibling : siblingNamespacesResult.getEntities()) {
        checkEntityLocationOverlap(sibling, targetLocation, location);
      }
    }
  }

  private void checkEntityLocationOverlap(
      EntityNameLookupRecord sibling, StorageLocation targetLocation, String location) {
    EntityResult loadResult =
        metaStoreManager.loadEntity(
            callContext.getPolarisCallContext(),
            sibling.getCatalogId(),
            sibling.getId(),
            PolarisEntityType.fromCode(sibling.getTypeCode()));
    if (!loadResult.isSuccess() || loadResult.getEntity() == null) {
      return;
    }
    PolarisEntity siblingEntity = new PolarisEntity(loadResult.getEntity());
    PolarisEntityUtils.asLocationBasedEntity(siblingEntity)
        .map(LocationBasedEntity::getBaseLocation)
        .map(StorageLocation::of)
        .ifPresent(
            siblingLocation -> {
              if (targetLocation.isChildOf(siblingLocation)
                  || siblingLocation.isChildOf(targetLocation)) {
                throw new ForbiddenException(
                    "Unable to create table at location '%s' because it conflicts with "
                        + "existing table or namespace at location '%s'",
                    location, siblingLocation);
              }
            });
  }
}
