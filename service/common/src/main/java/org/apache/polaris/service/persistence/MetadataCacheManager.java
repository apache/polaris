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

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.entity.table.TableLikeEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains utility methods related to storing TableMetadata in the metastore and retrieving it from
 * the metastore
 */
public class MetadataCacheManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataCacheManager.class);

  /**
   * Load the cached metadata.json content and location or fall back to `fallback` if one doesn't
   * exist. If the metadata is not currently cached, it may be added to the cache.
   */
  public static MetadataJson loadTableMetadataJson(
      TableIdentifier tableIdentifier,
      int maxBytesToCache,
      PolarisCallContext callContext,
      PolarisMetaStoreManager metastoreManager,
      PolarisResolutionManifestCatalogView resolvedEntityView,
      Supplier<TableMetadata> fallback) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(
            tableIdentifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE);
    // If the table doesn't exist, just fall back fast
    if (resolvedEntities == null) {
      return MetadataJson.fromMetadata(fallback.get());
    }
    LOGGER.debug(String.format("Loading cached metadata for %s", tableIdentifier));
    IcebergTableLikeEntity tableLikeEntity =
        IcebergTableLikeEntity.of(resolvedEntities.getRawLeafEntity());
    String cacheContent = tableLikeEntity.getMetadataCacheContent();
    if (cacheContent != null) {
      LOGGER.debug(String.format("Using cached metadata for %s", tableIdentifier));
      Map<String, String> entityProperties = tableLikeEntity.getPropertiesAsMap();
      return new MetadataJson(
          tableLikeEntity.getMetadataLocation(),
          tableLikeEntity.getMetadataCacheContent(),
          Stream.of(
                  entityProperties.get(PolarisEntityConstants.ENTITY_BASE_LOCATION),
                  entityProperties.get(
                      IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY),
                  entityProperties.get(
                      IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY))
              .filter(Objects::nonNull)
              .collect(Collectors.toSet()));
    } else {
      MetadataJson fallbackJson = MetadataJson.fromMetadata(fallback.get());
      var cacheResult =
          cacheTableMetadataJson(
              tableLikeEntity,
              fallbackJson.content(),
              maxBytesToCache,
              callContext,
              metastoreManager,
              resolvedEntityView);
      if (!cacheResult.isSuccess()) {
        LOGGER.debug(String.format("Failed to cache metadata for %s", tableIdentifier));
      }
      return fallbackJson;
    }
  }

  /**
   * Attempt to add table metadata to the cache
   *
   * @return The result of trying to cache the metadata
   */
  private static EntityResult cacheTableMetadataJson(
      IcebergTableLikeEntity tableLikeEntity,
      String metadataJson,
      int maxBytesToCache,
      PolarisCallContext callContext,
      PolarisMetaStoreManager metaStoreManager,
      PolarisResolutionManifestCatalogView resolvedEntityView) {
    if (maxBytesToCache != FeatureConfiguration.METADATA_CACHE_MAX_BYTES_INFINITE_CACHING) {
      if (metadataJson.length() > maxBytesToCache) {
        LOGGER.debug(
            String.format(
                "Will not cache metadata for %s; metadata above the limit of %d bytes",
                tableLikeEntity.getTableIdentifier(), maxBytesToCache));
        return new EntityResult(EntityResult.ReturnStatus.SUCCESS, null);
      }
    }

    LOGGER.debug(String.format("Caching metadata for %s", tableLikeEntity.getTableIdentifier()));
    TableLikeEntity newTableLikeEntity =
        new IcebergTableLikeEntity.Builder(tableLikeEntity)
            .setMetadataContent(tableLikeEntity.getMetadataLocation(), metadataJson)
            .build();
    PolarisResolvedPathWrapper resolvedPath =
        resolvedEntityView.getResolvedPath(
            tableLikeEntity.getTableIdentifier(),
            PolarisEntityType.TABLE_LIKE,
            PolarisEntitySubType.ICEBERG_TABLE);
    try {
      return metaStoreManager.updateEntityPropertiesIfNotChanged(
          callContext,
          PolarisEntity.toCoreList(resolvedPath.getRawParentPath()),
          newTableLikeEntity);
    } catch (RuntimeException e) {
      // PersistenceException (& other extension-specific exceptions) may not be in scope,
      // but we can make a best-effort attempt to swallow it and just forego caching
      if (e.toString().contains("PersistenceException")) {
        LOGGER.warn(
            String.format(
                "Encountered an error while caching %s: %s",
                tableLikeEntity.getTableIdentifier(), e));
        return new EntityResult(
            EntityResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED, e.getMessage());
      } else {
        throw e;
      }
    }
  }
}
