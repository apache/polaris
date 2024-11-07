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
package org.apache.polaris.service.persistence;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.TableLikeEntity;
import org.apache.polaris.core.entity.TableMetadataEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataCacheManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataCacheManager.class);

  /** Load the cached {@link Table} or fall back to `fallback` if one doesn't exist */
  public static TableMetadata loadTableMetadata(
      TableIdentifier tableIdentifier,
      PolarisCallContext callContext,
      PolarisEntityManager entityManager,
      PolarisResolutionManifestCatalogView resolvedEntityView,
      Supplier<TableMetadata> fallback) {
    LOGGER.debug(String.format("Loading cached metadata for %s", tableIdentifier));
    Optional<TableMetadata> cachedMetadata =
        loadCachedTableMetadata(tableIdentifier, callContext, entityManager, resolvedEntityView);
    if (cachedMetadata.isPresent()) {
      LOGGER.debug(String.format("Using cached metadata for %s", tableIdentifier));
      return cachedMetadata.get();
    } else {
      TableMetadata metadata = fallback.get();
      PolarisMetaStoreManager.EntityResult cacheResult =
          cacheTableMetadata(
              tableIdentifier, metadata, callContext, entityManager, resolvedEntityView);
      if (!cacheResult.isSuccess()) {
        LOGGER.debug(String.format("Failed to cache metadata for %s", tableIdentifier));
      }
      return metadata;
    }
  }

  /**
   * Attempt to add table metadata to the cache
   *
   * @return The result of trying to cache the metadata
   */
  private static PolarisMetaStoreManager.EntityResult cacheTableMetadata(
      TableIdentifier tableIdentifier,
      TableMetadata metadata,
      PolarisCallContext callContext,
      PolarisEntityManager entityManager,
      PolarisResolutionManifestCatalogView resolvedEntityView) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(tableIdentifier, PolarisEntitySubType.TABLE);
    if (resolvedEntities == null) {
      return new PolarisMetaStoreManager.EntityResult(
          PolarisMetaStoreManager.ReturnStatus.ENTITY_NOT_FOUND, null);
    } else {
      TableLikeEntity tableEntity = TableLikeEntity.of(resolvedEntities.getRawLeafEntity());
      TableMetadataEntity tableMetadataEntity =
          new TableMetadataEntity.Builder()
              .setCatalogId(tableEntity.getCatalogId())
              .setParentId(tableEntity.getId())
              .setId(entityManager.getMetaStoreManager().generateNewEntityId(callContext).getId())
              .setCreateTimestamp(System.currentTimeMillis())
              .setMetadataLocation(metadata.metadataFileLocation())
              .setContent(TableMetadataParser.toJson(metadata))
              .build();
      try {
        return entityManager
            .getMetaStoreManager()
            .createEntityIfNotExists(
                callContext,
                PolarisEntity.toCoreList(resolvedEntities.getRawFullPath()),
                tableMetadataEntity);
      } catch (RuntimeException e) {
        // PersistenceException (& other extension-specific exceptions) may not be in scope,
        // but we can make a best-effort attempt to swallow it and just forego caching
        if (e.toString().contains("PersistenceException")) {
          return new PolarisMetaStoreManager.EntityResult(
              PolarisMetaStoreManager.ReturnStatus.UNEXPECTED_ERROR_SIGNALED, e.getMessage());
        } else {
          throw e;
        }
      }
    }
  }

  /** Return the cached {@link Table} entity, if one exists */
  private static @NotNull Optional<TableMetadata> loadCachedTableMetadata(
      TableIdentifier tableIdentifier,
      PolarisCallContext callContext,
      PolarisEntityManager entityManager,
      PolarisResolutionManifestCatalogView resolvedEntityView) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(tableIdentifier, PolarisEntitySubType.TABLE);
    if (resolvedEntities == null) {
      return Optional.empty();
    } else {
      TableLikeEntity entity = TableLikeEntity.of(resolvedEntities.getRawLeafEntity());
      String metadataLocation = entity.getMetadataLocation();
      PolarisMetaStoreManager.ListEntitiesResult metadataResult =
          entityManager
              .getMetaStoreManager()
              .listEntities(
                  callContext,
                  PolarisEntity.toCoreList(resolvedEntities.getRawFullPath()),
                  PolarisEntityType.TABLE_METADATA,
                  PolarisEntitySubType.ANY_SUBTYPE);
      return Optional.ofNullable(metadataResult.getEntities()).stream()
          .flatMap(Collection::stream)
          .flatMap(
              result -> {
                PolarisMetaStoreManager.EntityResult metadataEntityResult =
                    entityManager
                        .getMetaStoreManager()
                        .loadEntity(callContext, result.getCatalogId(), result.getId());
                return Optional.ofNullable(metadataEntityResult.getEntity())
                    .map(TableMetadataEntity::of)
                    .stream();
              })
          .filter(
              metadata -> {
                if (metadata.getMetadataLocation().equals(metadataLocation)) {
                  return true;
                } else {
                  LOGGER.debug(
                      String.format("Deleting old entry for %s", metadata.getMetadataLocation()));
                  entityManager
                      .getMetaStoreManager()
                      .dropEntityIfExists(
                          callContext,
                          PolarisEntity.toCoreList(resolvedEntities.getRawFullPath()),
                          metadata,
                          null,
                          /* purge= */ false);
                  return false;
                }
              })
          .findFirst()
          .map(metadataEntity -> TableMetadataParser.fromJson(metadataEntity.getContent()));
    }
  }
}
